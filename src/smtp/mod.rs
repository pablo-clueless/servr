use lettre::{
    transport::smtp::AsyncSmtpTransport,
    AsyncTransport,
    Message,
};
use crate::schema::email::SendEmail;
use crate::error::AppError;
use tracing::info;

pub struct SmtpService {
    transport: AsyncSmtpTransport<lettre::Tokio1Executor>,
}

impl SmtpService {
    pub fn new(host: &str, port: u16, username: Option<&str>, password: Option<&str>) -> Result<Self, AppError> {
        let mut builder = AsyncSmtpTransport::<lettre::Tokio1Executor>::relay(host)
            .map_err(|e| AppError::Internal(e.to_string()))?
            .port(port);

        if let (Some(user), Some(pass)) = (username, password) {
            builder = builder.credentials(lettre::transport::smtp::authentication::Credentials::new(user.to_string(), pass.to_string()));
        }

        let transport = builder.build();

        Ok(Self {
            transport,
        })
    }

    pub async fn send_email(&self, email: SendEmail) -> Result<(), AppError> {
        let mut builder = Message::builder()
            .from(email.from.parse::<lettre::address::Address>().map_err(|e| AppError::BadRequest(e.to_string()))?.into())
            .subject(&email.subject);

        for to in &email.to {
            builder = builder.to(to.parse::<lettre::address::Address>().map_err(|e| AppError::BadRequest(e.to_string()))?.into());
        }

        let body = if let Some(html) = email.html {
            html
        } else if let Some(text) = email.text {
            text
        } else {
            return Err(AppError::BadRequest("Email must have either text or html body".to_string()));
        };

        let message = builder.body(body).map_err(|e| AppError::Internal(e.to_string()))?;

        self.transport.send(message).await.map_err(|e| AppError::SmtpError(e.to_string()))?;

        info!("Email sent successfully to {}", email.to.join(", "));
        Ok(())
    }
}
