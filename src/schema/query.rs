use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct User {
    pub id: String,
    pub name: String,
    pub email: String,
    pub active: bool,
    pub address: Option<Address>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Address {
    pub street: String,
    pub city: String,
    pub zipcode: String,
    pub country: String,
    pub geo: Geo,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Geo {
    pub lat: String,
    pub lng: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Post {
    pub id: String,
    pub title: String,
    pub content: String,
    pub author_id: String,
    pub published: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Album {
    pub id: String,
    pub title: String,
    pub artist: Artist,
    pub image_url: String,
    pub published: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Artist {
    pub id: String,
    pub name: String,
    pub genre: String,
    pub active: bool,
}
