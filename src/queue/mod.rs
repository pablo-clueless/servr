use crate::schema::queue::Job;
use crate::smtp::SmtpService;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info};

pub async fn start_worker(mut rx: mpsc::Receiver<Job>, mailer: Arc<SmtpService>) {
    info!("Job queue worker started");

    while let Some(job) = rx.recv().await {
        info!("Processing job: {:?}", job);
        match job {
            Job::SendEmail(email) => {
                if let Err(e) = mailer.send_email(email).await {
                    error!("Failed to send email via worker: {}", e);
                } else {
                    info!("Worker successfully sent email");
                }
            }
            Job::ProcessWebhook { id, payload } => {
                info!("Processing webhook {} with payload: {:?}", id, payload);
                // Implement webhook processing logic here
            }
            Job::Ping => {
                info!("Worker received ping job");
            }
        }
    }

    info!("Job queue worker stopped");
}
