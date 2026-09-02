//! The Postgres data plane, isolated per run.
//!
//! # Q2 — resolved: schema-per-run
//!
//! Each [`RunId`] gets a Postgres schema named `run_<uuid>`. Parallel test runs
//! share a database and see none of each other's rows (invariant 6).
//!
//! # Trap T5
//!
//! `search_path` is a **per-connection** setting. Pooled connections are handed
//! out fresh and will not carry a `search_path` that was set once at startup on
//! some other connection — so a query that worked in development quietly reads
//! the wrong schema under load, which is the worst possible way for isolation to
//! fail. It is therefore set in `after_connect`, and since the path differs per
//! run, each run gets its own pool. The pool, not the query, is what carries
//! the isolation.
//!
//! # This is the data plane
//!
//! Everything here is wiped by dropping a schema, and none of it is
//! control-plane state (invariant 3). `reset` does not touch it.

use std::collections::HashMap;
use std::sync::Arc;

use sqlx::postgres::{PgPool, PgPoolOptions};
use testbed_core::RunId;
use tokio::sync::RwLock;

/// Applied into each run's schema at creation.
const MIGRATION: &str = include_str!("../../../migrations/001_items.sql");

/// sqlx 0.9 requires query strings to be `'static` — its executors may spawn,
/// so a borrowed statement cannot be proven to outlive the work. DDL here is
/// per-run and therefore not `'static` by construction, so the handful of
/// statements each run needs are interned once and reused.
///
/// Bounded by the number of runs the process creates, at a few dozen bytes
/// each, and a run's statements stay valid for as long as the run might be
/// recreated. `format!`-ing them at each call site instead does not compile.
fn interned(sql: String) -> &'static str {
    use std::collections::HashSet;
    use std::sync::{Mutex, OnceLock};

    static POOL: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();
    let pool = POOL.get_or_init(|| Mutex::new(HashSet::new()));

    let mut pool = pool.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(existing) = pool.get(sql.as_str()) {
        return existing;
    }
    let leaked: &'static str = Box::leak(sql.into_boxed_str());
    pool.insert(leaked);
    leaked
}

/// Pools keyed by run. Each pool pins `search_path` to its run's schema.
pub struct DataPlane {
    url: String,
    pools: RwLock<HashMap<RunId, PgPool>>,
}

#[derive(Debug, thiserror::Error)]
pub enum DataError {
    #[error("data plane is not configured; set DATABASE_URL and start Postgres")]
    Unconfigured,
    #[error("run {0} does not exist; create it with POST /_admin/runs")]
    UnknownRun(RunId),
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
}

impl DataPlane {
    /// Connects and verifies the server is reachable.
    ///
    /// Callers treat failure as "no data plane", not as fatal — the testbed is
    /// routinely run without Postgres for the HTTP and telemetry surfaces, and
    /// refusing to boot would make those unusable.
    pub async fn connect(url: &str) -> Result<Self, DataError> {
        // A throwaway connection, purely to fail fast with a clear message
        // rather than at the first request.
        let probe = PgPoolOptions::new().max_connections(1).connect(url).await?;
        probe.close().await;

        Ok(Self {
            url: url.to_string(),
            pools: RwLock::new(HashMap::new()),
        })
    }

    /// Creates the run's schema and applies the data-plane migration.
    ///
    /// Idempotent: creating an existing run is a no-op, so a test harness can
    /// call it without tracking what it has already made.
    pub async fn create_run(&self, run: RunId) -> Result<(), DataError> {
        let schema = run.schema();

        // Schema names come from `RunId::schema()` — `run_` plus 32 hex
        // characters — so they cannot contain anything needing quoting. This is
        // the one place a name is interpolated rather than bound, because
        // Postgres does not accept a bind parameter for an identifier.
        debug_assert!(schema
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_'));

        // Declared before the connection is acquired: `Executor` requires the
        // query to outlive the executor borrow, and `&Pool` as an executor
        // demands `'static` outright.
        let create = interned(format!("CREATE SCHEMA IF NOT EXISTS {schema}"));
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.url)
            .await?;
        let mut conn = admin.acquire().await?;
        sqlx::raw_sql(create).execute(&mut *conn).await?;
        drop(conn);
        admin.close().await;

        // Applied through the run's own pool, so `search_path` is already set
        // and the migration needs no schema qualification.
        let pool = self.pool(run).await?;
        let mut conn = pool.acquire().await?;
        sqlx::raw_sql(MIGRATION).execute(&mut *conn).await?;

        tracing::info!(%run, %schema, "run created");
        Ok(())
    }

    /// Drops the run's schema and everything in it.
    pub async fn drop_run(&self, run: RunId) -> Result<(), DataError> {
        let schema = run.schema();

        if let Some(pool) = self.pools.write().await.remove(&run) {
            pool.close().await;
        }

        let drop_sql = interned(format!("DROP SCHEMA IF EXISTS {schema} CASCADE"));
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.url)
            .await?;
        let mut conn = admin.acquire().await?;
        sqlx::raw_sql(drop_sql).execute(&mut *conn).await?;
        drop(conn);
        admin.close().await;

        tracing::info!(%run, %schema, "run dropped");
        Ok(())
    }

    /// The pool for `run`, creating it on first use.
    ///
    /// Every connection this pool hands out has already had `search_path` set
    /// by `after_connect` — that is the whole isolation mechanism (T5).
    pub async fn pool(&self, run: RunId) -> Result<PgPool, DataError> {
        if let Some(pool) = self.pools.read().await.get(&run) {
            return Ok(pool.clone());
        }

        // Built once per run, not per connection.
        let set_search_path = interned(format!("SET search_path TO {}", run.schema()));

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .after_connect(move |conn, _meta| {
                Box::pin(async move {
                    // T5: every connection, every time. A `search_path` set
                    // once at startup does not travel to pooled connections.
                    sqlx::raw_sql(set_search_path).execute(&mut *conn).await?;
                    Ok(())
                })
            })
            .connect(&self.url)
            .await?;

        let mut pools = self.pools.write().await;
        // Another task may have raced us here; keep whichever landed first so
        // a run never ends up with two pools.
        Ok(pools.entry(run).or_insert(pool).clone())
    }

    /// Runs known to this process.
    pub async fn runs(&self) -> Vec<RunId> {
        self.pools.read().await.keys().copied().collect()
    }
}

/// Optional data plane. `None` means Postgres was not configured or not
/// reachable at boot; the HTTP and telemetry surfaces work regardless, and
/// `/api/items` reports that clearly rather than panicking.
pub type MaybeData = Option<Arc<DataPlane>>;

/// Resolves the data plane or explains its absence.
pub fn require(data: &MaybeData) -> Result<&Arc<DataPlane>, DataError> {
    data.as_ref().ok_or(DataError::Unconfigured)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_names_need_no_quoting() {
        // The invariant that makes the one interpolated identifier safe.
        for _ in 0..100 {
            let schema = RunId::new().schema();
            assert!(
                schema
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_'),
                "unsafe schema identifier: {schema}"
            );
            assert!(schema.starts_with("run_"));
        }
    }

    #[test]
    fn the_migration_creates_no_schema_of_its_own() {
        // It is applied with `search_path` already set, once per run. A
        // `CREATE SCHEMA` in here would put every run's tables in one place.
        //
        // Comments are stripped first: the file explains *why* it contains no
        // `CREATE SCHEMA`, and matching that prose would be a false positive.
        let statements: String = MIGRATION
            .lines()
            .map(|line| line.split("--").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");

        assert!(
            !statements.to_uppercase().contains("CREATE SCHEMA"),
            "the migration must be schema-agnostic"
        );
        assert!(statements.contains("CREATE TABLE IF NOT EXISTS items"));
    }

    #[test]
    fn an_absent_data_plane_reports_itself_clearly() {
        let Err(err) = require(&None) else {
            panic!("an absent data plane resolved to something");
        };
        assert!(matches!(err, DataError::Unconfigured));
        assert!(err.to_string().contains("DATABASE_URL"));
    }
}
