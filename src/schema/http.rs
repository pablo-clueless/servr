use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    pub headers: HashMap<String, String>,
    pub ip: String,
    pub user_agent: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response<T> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub message: String,
    pub meta: Meta,
    pub method: String,
    pub path: String,
    pub request_id: String,
    pub status: u16,
    pub timestamp: i64,
}

impl<T> Response<T> {
    pub fn new(
        data: Option<T>,
        message: &str,
        status: u16,
        method: &str,
        path: &str,
        ip: &str,
        ua: &str,
    ) -> Self {
        Self {
            data,
            error: None,
            message: message.to_string(),
            meta: Meta {
                headers: HashMap::new(),
                ip: ip.to_string(),
                user_agent: ua.to_string(),
            },
            method: method.to_string(),
            path: path.to_string(),
            request_id: uuid::Uuid::new_v4().to_string(),
            status,
            timestamp: chrono::Utc::now().timestamp(),
        }
    }

    #[allow(dead_code)]
    pub fn error(message: &str, status: u16, method: &str, path: &str, ip: &str, ua: &str) -> Self {
        Self {
            data: None,
            error: Some(message.to_string()),
            message: "An error occurred".to_string(),
            meta: Meta {
                headers: HashMap::new(),
                ip: ip.to_string(),
                user_agent: ua.to_string(),
            },
            method: method.to_string(),
            path: path.to_string(),
            request_id: uuid::Uuid::new_v4().to_string(),
            status,
            timestamp: chrono::Utc::now().timestamp(),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pagination {
    pub has_next: bool,
    pub has_prev: bool,
    pub page: u32,
    pub page_size: u32,
    pub total: u64,
    pub total_pages: u64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Paginated<T> {
    pub data: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub message: String,
    pub pagination: Pagination,
    pub status: u16,
    pub timestamp: i64,
}
