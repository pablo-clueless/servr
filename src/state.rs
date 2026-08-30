use crate::schema::query::{Address, Album, Artist, Geo, Post, User};
use crate::schema::queue::Job;
use crate::smtp::SmtpService;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;

pub struct AppState {
    pub mailer: Arc<SmtpService>,
    pub job_tx: mpsc::Sender<Job>,
    pub db: Arc<Database>,
    pub users: RwLock<HashMap<String, User>>,
    pub posts: RwLock<HashMap<String, Post>>,
    pub albums: RwLock<HashMap<String, Album>>,
}

#[allow(dead_code)]
pub struct Database {
    pub url: String,
}

pub type SharedState = Arc<AppState>;

pub fn seed_data(state: &AppState) {
    let mut users = state.users.write().unwrap();
    let mut posts = state.posts.write().unwrap();
    let mut albums = state.albums.write().unwrap();

    for i in 1..=50 {
        let user_id = i.to_string();
        users.insert(
            user_id.clone(),
            User {
                id: user_id.clone(),
                name: format!("User {}", i),
                email: format!("user{}@example.com", i),
                active: true,
                address: Some(Address {
                    street: format!("{} Main St", i),
                    city: "Rust City".to_string(),
                    zipcode: format!("{:05}", i),
                    country: "Rustland".to_string(),
                    geo: Geo {
                        lat: format!("{}.123", i),
                        lng: format!("{}.456", i),
                    },
                }),
            },
        );

        posts.insert(
            i.to_string(),
            Post {
                id: i.to_string(),
                title: format!("Post #{}", i),
                content: format!("Content of post {}. This is some sample text.", i),
                author_id: user_id.clone(),
                published: true,
            },
        );

        albums.insert(
            i.to_string(),
            Album {
                id: i.to_string(),
                title: format!("Album {}", i),
                artist: Artist {
                    id: user_id.clone(),
                    name: format!("User {}", i),
                    genre: "Generic".to_string(),
                    active: true,
                },
                image_url: format!("http://example.com/album{}.jpg", i),
                published: true,
            },
        );
    }
}
