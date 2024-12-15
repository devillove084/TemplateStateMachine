use std::fmt;
use std::hash::Hash;

use asc::Asc;
use once_cell::sync::Lazy;
use ordered_vecmap::VecSet;
use serde::{Deserialize, Serialize};

use super::ReplicaId;

/// Use vec set struct offer an ordered and memory performance implementation
/// than b+ tree, and support set union and iterator. And "Acc" is short for
/// "Accumulator".
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MutableAcc(VecSet<ReplicaId>);

impl MutableAcc {
    #[must_use]
    pub fn with_capacity(cap: usize) -> Self {
        Self(VecSet::with_capacity(cap))
    }

    pub fn insert(&mut self, id: ReplicaId) {
        let _ = self.0.insert(id);
    }

    pub fn union(&mut self, other: &Self) {
        self.0.union_copied_inplace(&other.0)
    }
}

/// Use `Asc` crate manager `MutableAcc` mutable and immutable capability.
/// Also offer "COW" capability for multi process read and write.
#[derive(Clone, Serialize, Deserialize)]
pub struct Acc(Asc<MutableAcc>);

impl Acc {
    #[inline]
    fn as_inner(&self) -> &MutableAcc {
        &self.0
    }

    #[must_use]
    pub fn from_mutable(acc: MutableAcc) -> Self {
        Self(Asc::new(acc))
    }

    #[must_use]
    pub fn into_mutable(self) -> MutableAcc {
        match Asc::try_unwrap(self.0) {
            Ok(a) => a,
            Err(a) => MutableAcc::clone(&*a),
        }
    }

    pub fn cow_insert(&mut self, id: ReplicaId) {
        let acc = Asc::make_mut(&mut self.0);
        acc.insert(id);
    }
}

impl PartialEq for Acc {
    fn eq(&self, other: &Self) -> bool {
        Asc::ptr_eq(&self.0, &other.0) || self.as_inner() == other.as_inner()
    }
}

impl Eq for Acc {}

impl Hash for Acc {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_inner().hash(state);
    }
}

impl AsRef<MutableAcc> for Acc {
    fn as_ref(&self) -> &MutableAcc {
        self.as_inner()
    }
}

static EMPTY: Lazy<Acc> = Lazy::new(|| Acc(Asc::new(MutableAcc(VecSet::new()))));

impl Default for Acc {
    fn default() -> Self {
        Acc::clone(&*EMPTY)
    }
}

impl AsRef<VecSet<ReplicaId>> for MutableAcc {
    fn as_ref(&self) -> &VecSet<ReplicaId> {
        &self.0
    }
}

impl AsRef<VecSet<ReplicaId>> for Acc {
    fn as_ref(&self) -> &VecSet<ReplicaId> {
        &self.as_inner().0
    }
}

impl FromIterator<ReplicaId> for MutableAcc {
    fn from_iter<T: IntoIterator<Item = ReplicaId>>(iter: T) -> Self {
        MutableAcc(VecSet::from_iter(iter))
    }
}

impl FromIterator<ReplicaId> for Acc {
    fn from_iter<T: IntoIterator<Item = ReplicaId>>(iter: T) -> Self {
        Acc::from_mutable(MutableAcc(VecSet::from_iter(iter)))
    }
}

impl fmt::Debug for Acc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set()
            .entries(self.as_inner().0.as_slice().iter())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_mutable_acc_basic_operations() {
        let mut acc = MutableAcc::default();
        assert_eq!(acc.as_ref().len(), 0);

        acc.insert(ReplicaId::from(1));
        acc.insert(ReplicaId::from(2));
        acc.insert(ReplicaId::from(3));
        assert_eq!(acc.as_ref().len(), 3);
        assert!(acc.as_ref().contains(&ReplicaId::from(1)));
        assert!(acc.as_ref().contains(&ReplicaId::from(2)));
        assert!(acc.as_ref().contains(&ReplicaId::from(3)));

        acc.insert(ReplicaId::from(2));
        assert_eq!(acc.as_ref().len(), 3);

        let mut acc2 = MutableAcc::default();
        acc2.insert(ReplicaId::from(3));
        acc2.insert(ReplicaId::from(4));
        acc.union(&acc2);
        assert_eq!(acc.as_ref().len(), 4);
        assert!(acc.as_ref().contains(&ReplicaId::from(4)));
    }

    #[test]
    fn test_acc_conversion() {
        let mut mutable = MutableAcc::default();
        mutable.insert(ReplicaId::from(1));
        mutable.insert(ReplicaId::from(2));

        let acc = Acc::from_mutable(mutable.clone());
        assert_eq!(acc.as_inner(), &mutable);

        let converted = acc.clone().into_mutable();
        assert_eq!(converted, mutable);
    }

    #[test]
    fn test_acc_cow_behavior() {
        let mut mutable = MutableAcc::default();
        mutable.insert(ReplicaId::from(1));
        mutable.insert(ReplicaId::from(2));

        let mut acc1 = Acc::from_mutable(mutable.clone());
        let acc2 = acc1.clone();

        acc1.cow_insert(ReplicaId::from(3));

        assert!(acc1.as_inner().as_ref().contains(&ReplicaId::from(3)));
        assert!(!acc2.as_inner().as_ref().contains(&ReplicaId::from(3)));

        assert_eq!(acc1.as_inner().as_ref().len(), 3);
        assert_eq!(acc2.as_inner().as_ref().len(), 2);
    }

    #[test]
    fn test_serialization_deserialization() {
        let mut acc = Acc::default();
        acc.cow_insert(ReplicaId::from(1));
        acc.cow_insert(ReplicaId::from(2));
        acc.cow_insert(ReplicaId::from(3));

        let serialized = serde_json::to_string(&acc).expect("Serialization failed");
        println!("Serialized Acc: {}", serialized);

        let deserialized: Acc = serde_json::from_str(&serialized).expect("Deserialization failed");
        assert_eq!(acc, deserialized);
    }
}
