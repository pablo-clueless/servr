//! Webhook signing, both schemes (Q4).
//!
//! Operator decision was **both**, selected per endpoint in scenario config, so
//! a client library can be tested against whichever one it actually implements
//! without standing up two testbeds.
//!
//! # The Stripe timestamp is virtual
//!
//! `t=` comes from the testbed clock, not wall time. That follows decision 7 —
//! the virtual clock is authoritative — and it is also the more useful
//! behaviour: a receiver that rejects signatures outside a freshness window is
//! *supposed* to reject after a `clock/advance`, and being able to provoke that
//! on demand is the point of having a clock at all. A receiver comparing `t`
//! against its own wall clock will disagree with the testbed the moment time is
//! advanced, which is the bug being simulated, not one in here.

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use testbed_core::SigningScheme;

type HmacSha256 = Hmac<Sha256>;

/// Header name for [`SigningScheme::Stripe`].
pub const STRIPE_HEADER: &str = "stripe-signature";
/// Header name for [`SigningScheme::Github`].
pub const GITHUB_HEADER: &str = "x-hub-signature-256";

/// Used when an endpoint asks to be signed but carries no secret.
///
/// A fixed, published value rather than a random one: the gate has to verify
/// the delivered signature, and a secret it cannot learn makes that impossible.
/// This is a testbed — there is nothing here worth keeping secret.
pub const DEFAULT_SECRET: &str = "testbed-webhook-secret";

/// The header a signed delivery carries, or `None` for [`SigningScheme::None`].
pub fn header(
    scheme: SigningScheme,
    secret: &str,
    body: &[u8],
    at: DateTime<Utc>,
) -> Option<(&'static str, String)> {
    match scheme {
        SigningScheme::None => None,
        SigningScheme::Stripe => {
            let t = at.timestamp();
            let mac = hex_hmac(secret, &stripe_payload(t, body));
            Some((STRIPE_HEADER, format!("t={t},v1={mac}")))
        }
        SigningScheme::Github => {
            Some((GITHUB_HEADER, format!("sha256={}", hex_hmac(secret, body))))
        }
    }
}

/// Whether `value` is a signature this secret would have produced for `body`.
///
/// Exposed because the Phase 7 gate has to verify what was delivered, and a
/// testbed that can sign but not check its own signatures cannot tell a broken
/// signer from a broken verifier.
pub fn verify(scheme: SigningScheme, secret: &str, body: &[u8], value: &str) -> bool {
    match scheme {
        // Nothing was signed, so nothing verifies. Returning `true` here would
        // make an unsigned delivery indistinguishable from a correctly signed
        // one, which is the one answer that must never be given.
        SigningScheme::None => false,
        SigningScheme::Stripe => {
            let Some((t, mac)) = parse_stripe(value) else {
                return false;
            };
            constant_time_eq(secret, &stripe_payload(t, body), &mac)
        }
        SigningScheme::Github => match value.strip_prefix("sha256=") {
            Some(mac) => constant_time_eq(secret, body, mac),
            None => false,
        },
    }
}

/// Stripe signs `"<t>.<body>"`, not the body alone — a signature that omitted
/// the timestamp would be replayable forever.
fn stripe_payload(t: i64, body: &[u8]) -> Vec<u8> {
    let mut payload = format!("{t}.").into_bytes();
    payload.extend_from_slice(body);
    payload
}

fn parse_stripe(value: &str) -> Option<(i64, String)> {
    let mut t = None;
    let mut v1 = None;
    for part in value.split(',') {
        match part.trim().split_once('=') {
            Some(("t", raw)) => t = raw.parse().ok(),
            Some(("v1", raw)) => v1 = Some(raw.to_string()),
            _ => {}
        }
    }
    Some((t?, v1?))
}

fn hex_hmac(secret: &str, message: &[u8]) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("hmac accepts any key length");
    mac.update(message);
    hex::encode(mac.finalize().into_bytes())
}

/// Compares through `hmac`'s own verifier rather than `==` on the hex strings.
///
/// A byte-by-byte string compare leaks where the first difference is. That is a
/// real attack on a real verifier, and the testbed is a reference other people
/// will read and copy — demonstrating the sloppy version here would be worse
/// than the microseconds it saves.
fn constant_time_eq(secret: &str, message: &[u8], expected_hex: &str) -> bool {
    let Ok(expected) = hex::decode(expected_hex) else {
        return false;
    };
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("hmac accepts any key length");
    mac.update(message);
    mac.verify_slice(&expected).is_ok()
}

/// SHA-256 of a body, hex encoded. `EventKind::WebhookIn` carries this so a
/// capture can be matched to what was sent without putting the body on the bus.
pub fn body_sha256(body: &[u8]) -> String {
    use sha2::Digest;
    hex::encode(Sha256::digest(body))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "whsec_test";
    const BODY: &[u8] = br#"{"x":1}"#;

    fn at() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    #[test]
    fn a_stripe_signature_verifies_against_its_secret() {
        let (name, value) = header(SigningScheme::Stripe, SECRET, BODY, at()).unwrap();
        assert_eq!(name, STRIPE_HEADER);
        assert!(value.starts_with("t=1700000000,v1="));
        assert!(verify(SigningScheme::Stripe, SECRET, BODY, &value));
    }

    #[test]
    fn a_github_signature_verifies_against_its_secret() {
        let (name, value) = header(SigningScheme::Github, SECRET, BODY, at()).unwrap();
        assert_eq!(name, GITHUB_HEADER);
        assert!(value.starts_with("sha256="));
        assert!(verify(SigningScheme::Github, SECRET, BODY, &value));
    }

    #[test]
    fn the_wrong_secret_does_not_verify() {
        for scheme in [SigningScheme::Stripe, SigningScheme::Github] {
            let (_, value) = header(scheme, SECRET, BODY, at()).unwrap();
            assert!(
                !verify(scheme, "whsec_other", BODY, &value),
                "{scheme:?} verified under the wrong secret"
            );
        }
    }

    #[test]
    fn a_tampered_body_does_not_verify() {
        for scheme in [SigningScheme::Stripe, SigningScheme::Github] {
            let (_, value) = header(scheme, SECRET, BODY, at()).unwrap();
            assert!(
                !verify(scheme, SECRET, br#"{"x":2}"#, &value),
                "{scheme:?} verified a body it did not sign"
            );
        }
    }

    /// The timestamp is inside the signed payload, so moving it invalidates the
    /// signature — otherwise a captured delivery replays forever.
    #[test]
    fn a_stripe_signature_is_bound_to_its_timestamp() {
        let (_, value) = header(SigningScheme::Stripe, SECRET, BODY, at()).unwrap();
        let moved = value.replace("t=1700000000", "t=1700009999");
        assert!(!verify(SigningScheme::Stripe, SECRET, BODY, &moved));
    }

    #[test]
    fn the_two_schemes_do_not_verify_each_others_signatures() {
        let (_, stripe) = header(SigningScheme::Stripe, SECRET, BODY, at()).unwrap();
        let (_, github) = header(SigningScheme::Github, SECRET, BODY, at()).unwrap();

        assert!(!verify(SigningScheme::Github, SECRET, BODY, &stripe));
        assert!(!verify(SigningScheme::Stripe, SECRET, BODY, &github));
    }

    #[test]
    fn scheme_none_signs_nothing_and_verifies_nothing() {
        assert!(header(SigningScheme::None, SECRET, BODY, at()).is_none());
        assert!(
            !verify(SigningScheme::None, SECRET, BODY, "anything"),
            "an unsigned delivery must not verify, or unsigned and signed \
             become indistinguishable"
        );
    }

    #[test]
    fn malformed_signature_headers_are_rejected_rather_than_panicking() {
        for junk in ["", "t=", "v1=abc", "t=notanumber,v1=abc", "sha256=zz", "="] {
            assert!(!verify(SigningScheme::Stripe, SECRET, BODY, junk));
            assert!(!verify(SigningScheme::Github, SECRET, BODY, junk));
        }
    }

    #[test]
    fn body_hashes_are_stable_and_distinguish_bodies() {
        assert_eq!(body_sha256(BODY), body_sha256(BODY));
        assert_ne!(body_sha256(BODY), body_sha256(br#"{"x":2}"#));
        assert_eq!(body_sha256(BODY).len(), 64);
    }
}
