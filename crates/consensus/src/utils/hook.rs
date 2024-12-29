#[cfg(test)]
use std::sync::{Arc, atomic::AtomicBool};

#[cfg(test)]
use tokio::sync::Notify;

#[cfg(test)]
#[derive(Clone)]
pub struct TestHooks {
    pub phase_preaccept_barrier: Arc<Notify>,
    pub skip: Arc<AtomicBool>,
}
