use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub name: String,
    pub email: String,
    pub active: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Post {
    pub id: String,
    pub title: String,
    pub content: String,
    pub author_id: String,
    pub published: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Album {
    pub id: String,
    pub title: String,
    pub artist_id: String,
    pub image_url: String,
    pub published: bool,
}
