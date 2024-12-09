use std::hash::Hash;

use serde::de::DeserializeOwned;
use serde::Serialize;

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
        todo!()
    }

    fn for_each(&self, _f: impl FnMut(&Self::Key)) {
        todo!()
    }
}

impl CommandLike for String {
    type Key = String;

    type Keys = String;

    fn keys(&self) -> Self::Keys {
        todo!()
    }

    fn is_nop(&self) -> bool {
        todo!()
    }

    fn create_nop() -> Self {
        todo!()
    }

    fn create_fence() -> Self {
        todo!()
    }

    fn notify_committed(&self) {
        todo!()
    }

    fn notify_executed(&self) {
        todo!()
    }
}