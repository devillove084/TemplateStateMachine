use std::hash::Hash;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::debug;

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

pub trait BatchedCommand {}

pub trait MutableCommand {}
