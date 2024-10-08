use bytes::Bytes;

#[async_trait::async_trait]
pub trait Wal: Send + Sync + 'static {
    async fn write(&self, bytes: Bytes) -> crate::Result<()>;
}
