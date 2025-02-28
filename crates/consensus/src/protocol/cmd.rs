use std::hash::Hash;

use asc::Asc;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::debug;

use crate::stepper::Stepper;

pub trait CommandLike
where
    Self: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    type Key: Clone + Eq + Ord + Hash + Send + Sync + 'static;

    type Keys: Keys<Key = Self::Key> + Send + Sync + 'static;

    fn keys(&self) -> Self::Keys;

    fn is_nop(&self) -> bool;

    fn create_nop() -> Self;

    fn create_fence() -> Self;

    fn notify_committed(&self);

    fn notify_executed(&self);
}

pub trait Keys {
    type Key;
    fn is_unbounded(&self) -> bool;
    fn for_each(&self, f: impl FnMut(&Self::Key));
}

impl Keys for String {
    type Key = String;

    fn is_unbounded(&self) -> bool {
        false
    }

    fn for_each(&self, mut _f: impl FnMut(&Self::Key)) {
        _f(self);
    }
}

impl CommandLike for String {
    type Key = String;

    type Keys = String;

    fn keys(&self) -> Self::Keys {
        self.clone()
    }

    fn is_nop(&self) -> bool {
        self == "NOP"
    }

    fn create_nop() -> Self {
        "NOP".to_string()
    }

    fn create_fence() -> Self {
        "FENCE".to_string()
    }

    fn notify_committed(&self) {
        debug!("Command committed: {}", self);
    }

    fn notify_executed(&self) {
        debug!("Command executed: {}", self);
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum CommandKind {
    Get(Get),
    Set(Set),
    Del(Del),
    Nop(Nop),
    Fence(Fence),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Get {
    pub key: Bytes,
    #[serde(skip)]
    pub tx: Option<mpsc::Sender<Option<Bytes>>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Set {
    pub key: Bytes,
    pub value: Bytes,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Del {
    pub key: Bytes,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Nop {}

#[derive(Clone, Serialize, Deserialize)]
pub struct Fence {}

#[derive(Clone, Serialize, Deserialize)]
pub struct Command(Asc<MutableCommand>);

impl Command {
    fn as_inner(&self) -> &MutableCommand {
        &self.0
    }

    pub fn from_mutable(cmd: MutableCommand) -> Self {
        Self(Asc::new(cmd))
    }

    #[must_use]
    pub fn into_mutable(self) -> MutableCommand {
        match Asc::try_unwrap(self.0) {
            Ok(cmd) => cmd,
            Err(this) => MutableCommand::clone(&*this),
        }
    }
}

impl AsRef<MutableCommand> for Command {
    fn as_ref(&self) -> &MutableCommand {
        self.as_inner()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MutableCommand {
    pub kind: CommandKind,
    #[serde(skip)]
    pub notify: Option<Asc<CommandNotify>>,
}

impl MutableCommand {
    pub fn is_fence(&self) -> bool {
        matches!(self.kind, CommandKind::Fence(_))
    }

    pub fn is_nop(&self) -> bool {
        matches!(self.kind, CommandKind::Nop(_))
    }

    #[must_use]
    pub fn create_nop() -> Self {
        Self {
            kind: CommandKind::Nop(Nop {}),
            notify: None,
        }
    }

    #[must_use]
    pub fn create_fence() -> Self {
        Self {
            kind: CommandKind::Fence(Fence {}),
            notify: None,
        }
    }

    pub fn fill_keys(&self, keys: &mut Vec<Bytes>) {
        match self.kind {
            CommandKind::Get(ref c) => keys.push(c.key.clone()),
            CommandKind::Set(ref c) => keys.push(c.key.clone()),
            CommandKind::Del(ref c) => keys.push(c.key.clone()),
            CommandKind::Nop(_) => {}
            CommandKind::Fence(_) => {}
        }
    }

    pub fn notify_committed(&self) {
        if let Some(ref n) = self.notify {
            n.notify_committed();
        }
    }

    pub fn notify_executed(&self) {
        if let Some(ref n) = self.notify {
            n.notify_executed();
        }
    }
}

pub struct CommandNotify(Stepper);

impl CommandNotify {
    const COMMITTED: u8 = 1;
    const EXECUTED: u8 = 2;

    #[must_use]
    pub fn new() -> Self {
        Self(Stepper::new())
    }

    pub fn notify_committed(&self) {
        self.0.set_state(Self::COMMITTED);
    }
    pub fn notify_executed(&self) {
        self.0.set_state(Self::EXECUTED);
    }

    pub async fn wait_committed(&self) {
        self.0.wait_state(Self::COMMITTED).await;
    }
    pub async fn wait_executed(&self) {
        self.0.wait_state(Self::EXECUTED).await;
    }
}

impl Default for CommandNotify {
    fn default() -> Self {
        Self::new()
    }
}
