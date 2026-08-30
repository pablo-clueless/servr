use crate::schema::email::SendEmail;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Job {
    SendEmail(SendEmail),
    ProcessWebhook {
        id: String,
        payload: serde_json::Value,
    },
    Ping,
}
