use crate::schema::queue::Job;
use crate::smtp::SmtpService;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct AppState {
    pub mailer: Arc<SmtpService>,
    pub job_tx: mpsc::Sender<Job>,
    pub db: Arc<Database>,
}

pub struct Database {
    pub url: String,
}

pub type SharedState = Arc<AppState>;
