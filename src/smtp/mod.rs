use lettre::{
    transport::smtp::AsyncSmtpTransport,
    transport::smtp::SmtpTransportBuilder,
    AsyncTransport,
    Message,
};
use crate::schema::email::SendEmail;
use crate::error::AppError;
use tracing::{info, error};

pub struct SmtpService {
    transport: AsyncSmtpTransport,
}

impl SmtpService {
    pub fn new(host: &str, port: u16, username: Option<&str>, password: Option<&str>) -> Result<Self, AppError> {
        let mut builder = SmtpTransportBuilder::new(host)
            .port(lettre::transport::smtp::Port::new(port));

        if let (Some(user), Some(pass)) = (username, password) {
            builder = builder.credentials(lettre::transport::smtp::Credentials::new(user.to_string(), pass.to_string()));
        }

        let transport = builder.build().map_err(|e| AppError::Internal(e.to_string()))?;

        Ok(Self {
            transport: AsyncSmtpTransport::from(transport),
        })
    }

    pub async fn send_email(&self, email: SendEmail) -> Result<(), AppError> {
        let mut message = Message::builder()
            .from(email.from.parse().map_err(|e| AppError::BadRequest(e.to_string()))?)
            .subject(&email.subject)
            .expect("Email must have a body");

        if let Some(html) = email.html {
            message = message.html_body(html).map_err(|e| AppError::Internal(e.to_string()))?;
        } else if let Some(text) = email.text {
            message = message.text_body(text).map_err(|e| AppError::Internal(e.to_string()))?;
        } else {
            return Err(AppError::BadRequest("Email must have either text or html body".to_string()));
        }

        for to in email.to {
            let msg = message.to(to.parse().map_err(|e| AppError::BadRequest(e.to_string()))?);
            self.transport.send(msg).await.map_err(|e| AppError::SmtpError(e.to_string()))?;
        }

        info!("Email sent successfully to {}", email.to.join(", "));
        Ok(())
    }
}
