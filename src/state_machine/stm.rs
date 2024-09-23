use bytes::Bytes;

#[async_trait::async_trait]
pub trait StateMachine {
    async fn apply(&self, bytes: Bytes) -> crate::Result<()>;
}
