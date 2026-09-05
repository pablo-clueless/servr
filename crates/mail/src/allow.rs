//! Who this testbed is allowed to send real mail to.
//!
//! # Why this exists
//!
//! `POST /_admin/mail/send` takes an arbitrary `to` address, and `/_admin` has
//! no authentication (HANDOFF §10). Point that at Mailpit and the worst case is
//! a cluttered local inbox. Point it at an authenticated relay and the worst
//! case is an **open mail relay on the public internet**: anyone who finds the
//! URL can send mail from your account, to anyone, until the provider suspends
//! you and your sending domain is blocklisted.
//!
//! So the relay transport is only usable with an allowlist, and the check lives
//! here — one function, called from one place in [`crate::Mailer::send`], for
//! the same reason the `X-Testbed-Run` header is set in exactly one place: a
//! guard with two call sites is a guard with one bypass.
//!
//! # It fails closed
//!
//! A relay configured with no allowlist refuses **every** send. That is
//! deliberate. The alternative — treating "no list" as "no restriction" — makes
//! the dangerous configuration the one you get by forgetting something, and
//! forgetting is the normal case.

use std::fmt;

/// Recipients a relay send is permitted to reach.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Allowlist {
    entries: Vec<Entry>,
}

#[derive(Debug, Clone, PartialEq)]
enum Entry {
    /// `@example.com` — any address at exactly this domain.
    Domain(String),
    /// `someone@example.com` — this address and no other.
    Address(String),
    /// `*` — everything. Explicit, and logged loudly at boot.
    Any,
}

impl Allowlist {
    /// Parses `MAIL_ALLOWED_RECIPIENTS`: comma-separated, entries are either
    /// `@domain`, a full address, or `*`.
    pub fn parse(raw: &str) -> Self {
        let entries = raw
            .split(',')
            .map(str::trim)
            .filter(|e| !e.is_empty())
            .map(|e| {
                let lower = e.to_ascii_lowercase();
                if lower == "*" {
                    Entry::Any
                } else if let Some(domain) = lower.strip_prefix('@') {
                    Entry::Domain(domain.to_string())
                } else {
                    Entry::Address(lower)
                }
            })
            .collect();

        Self { entries }
    }

    /// True when nothing is permitted — an unconfigured relay.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// True when this list permits sending anywhere.
    pub fn is_unrestricted(&self) -> bool {
        self.entries.contains(&Entry::Any)
    }

    /// Whether `address` may be sent to.
    ///
    /// The domain is taken from the **last** `@`, which is what an SMTP server
    /// does. Splitting on the first would let `victim@evil.com@allowed.com`
    /// through by reading `evil.com` as the domain, and comparing by suffix
    /// instead of equality would let `allowed.com.evil.com` through. Both are
    /// covered by tests below because both are the obvious way to write this.
    pub fn permits(&self, address: &str) -> bool {
        let address = address.trim().to_ascii_lowercase();
        let Some((_, domain)) = address.rsplit_once('@') else {
            // No domain at all is not a deliverable address; refuse rather than
            // hand it to the relay to reject.
            return false;
        };

        self.entries.iter().any(|entry| match entry {
            Entry::Any => true,
            Entry::Domain(allowed) => domain == allowed,
            Entry::Address(allowed) => address == *allowed,
        })
    }
}

impl fmt::Display for Allowlist {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.entries.is_empty() {
            return write!(f, "(none — every relay send is refused)");
        }
        let rendered: Vec<String> = self
            .entries
            .iter()
            .map(|e| match e {
                Entry::Any => "*".to_string(),
                Entry::Domain(d) => format!("@{d}"),
                Entry::Address(a) => a.clone(),
            })
            .collect();
        write!(f, "{}", rendered.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unconfigured_list_permits_nothing() {
        let list = Allowlist::parse("");
        assert!(list.is_empty());
        assert!(!list.permits("anyone@anywhere.com"));
    }

    #[test]
    fn a_domain_entry_permits_that_domain_only() {
        let list = Allowlist::parse("@example.com");
        assert!(list.permits("someone@example.com"));
        assert!(
            list.permits("SOMEONE@EXAMPLE.COM"),
            "matching is case-insensitive"
        );
        assert!(!list.permits("someone@other.com"));
    }

    #[test]
    fn an_address_entry_permits_that_address_only() {
        let list = Allowlist::parse("ops@example.com");
        assert!(list.permits("ops@example.com"));
        assert!(!list.permits("someone-else@example.com"));
    }

    /// A suffix comparison would let this through. It is the obvious way to
    /// write domain matching and it is wrong.
    #[test]
    fn a_lookalike_subdomain_is_refused() {
        let list = Allowlist::parse("@example.com");
        assert!(!list.permits("someone@example.com.evil.test"));
        assert!(!list.permits("someone@notexample.com"));
        assert!(!list.permits("someone@evil-example.com"));
    }

    /// Splitting on the *first* `@` would read the domain as `evil.test` here
    /// and, worse, a naive "contains allowed domain" check would pass it.
    #[test]
    fn an_address_with_an_embedded_at_is_judged_on_its_real_domain() {
        let list = Allowlist::parse("@example.com");
        assert!(
            list.permits("victim@evil.test@example.com"),
            "the real domain is the last one, and it is allowed"
        );
        assert!(
            !list.permits("victim@example.com@evil.test"),
            "the real domain is evil.test, which is not allowed"
        );
    }

    #[test]
    fn an_address_with_no_domain_is_refused() {
        assert!(!Allowlist::parse("@example.com").permits("not-an-address"));
        assert!(!Allowlist::parse("*").permits(""));
    }

    #[test]
    fn several_entries_compose() {
        let list = Allowlist::parse("@example.com, ops@other.test ,@third.test");
        assert!(list.permits("a@example.com"));
        assert!(list.permits("ops@other.test"));
        assert!(list.permits("b@third.test"));
        assert!(!list.permits("someone@other.test"));
        assert!(!list.permits("a@nope.test"));
    }

    #[test]
    fn the_wildcard_permits_anything_but_is_visible_as_such() {
        let list = Allowlist::parse("*");
        assert!(list.is_unrestricted());
        assert!(list.permits("anyone@anywhere.test"));
        assert_eq!(list.to_string(), "*");
    }

    #[test]
    fn a_normal_list_does_not_read_as_unrestricted() {
        let list = Allowlist::parse("@example.com");
        assert!(!list.is_unrestricted());
        assert!(!list.is_empty());
    }

    #[test]
    fn it_renders_for_the_boot_log() {
        assert_eq!(
            Allowlist::parse("@a.test,b@c.test").to_string(),
            "@a.test, b@c.test"
        );
        assert!(Allowlist::parse("").to_string().contains("refused"));
    }
}
