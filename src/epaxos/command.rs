use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum CommandType {
    Add,
    Delete,
    Update,
    Read,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct Command {
    command_type: CommandType,
    key: Bytes,
    value: Option<Bytes>,
}

impl Command {
    pub async fn execute(&self) -> crate::Result<()> {
        todo!()
    }
}
