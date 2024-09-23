use bytes::Bytes;

#[async_trait::async_trait]
pub trait Wal {
    async fn write(&self, bytes: Bytes) -> crate::Result<()>;
}
