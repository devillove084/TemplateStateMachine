use crate::Command;

#[async_trait::async_trait]
pub trait StateMachine: Send + Sync + 'static {
    async fn apply(&self, command: &Command) -> crate::Result<()>;
}

// #[async_trait::async_trait]
// pub trait Storage {
//     async fn put(&self, key: Bytes, value: Option<Bytes>) -> crate::Result<()>;
// }
