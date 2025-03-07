use consensus::InstanceId;
use consensus::bits::{OneU8, TwoU8};

use std::fmt;

use bytemuck::{AnyBitPattern, CheckedBitPattern, NoUninit};

#[derive(Clone, Copy, NoUninit, AnyBitPattern)]
#[repr(transparent)]
struct Be64([u8; 8]);

impl Be64 {
    fn new(val: u64) -> Self {
        Self(val.to_be_bytes())
    }

    fn to_u64(self) -> u64 {
        u64::from_be_bytes(self.0)
    }
}

impl fmt::Debug for Be64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Be64").field(&self.to_u64()).finish()
    }
}

#[derive(Clone, Copy, NoUninit, CheckedBitPattern)]
#[repr(C)]
pub struct InstanceFieldKey {
    prefix: OneU8,
    replica_id: Be64,
    local_instance_id: Be64,
    field: u8,
}

#[derive(Clone, Copy, NoUninit, CheckedBitPattern)]
#[repr(C)]
pub struct GlobalFieldKey {
    prefix: TwoU8,
    field: u8,
}

impl InstanceFieldKey {
    pub const FIELD_STATUS: u8 = 1;
    pub const FIELD_SEQ: u8 = 2;
    pub const FIELD_propose_ballot: u8 = 3;
    pub const FIELD_CMD: u8 = 4;
    pub const FIELD_OTHERS: u8 = 5;

    #[must_use]
    pub fn new(id: InstanceId, field: u8) -> Self {
        Self {
            prefix: OneU8::VALUE,
            replica_id: Be64::new(id.0.raw_value()),
            local_instance_id: Be64::new(id.1.raw_value()),
            field,
        }
    }

    pub fn set_field(&mut self, field: u8) {
        self.field = field;
    }

    #[must_use]
    pub fn field(&self) -> u8 {
        self.field
    }

    #[must_use]
    pub fn id(&self) -> InstanceId {
        let replica_id = self.replica_id.to_u64().into();
        let local_instance_id = self.local_instance_id.to_u64().into();
        InstanceId(replica_id, local_instance_id)
    }

    pub fn set_id(&mut self, id: InstanceId) {
        self.replica_id = Be64::new(id.0.raw_value());
        self.local_instance_id = Be64::new(id.1.raw_value());
    }
}

impl GlobalFieldKey {
    pub const FIELD_ATTR_BOUNDS: u8 = 1;
    pub const FIELD_STATUS_BOUNDS: u8 = 2;

    #[must_use]
    pub fn new(field: u8) -> Self {
        Self {
            prefix: TwoU8::VALUE,
            field,
        }
    }

    pub fn set_field(&mut self, field: u8) {
        self.field = field;
    }
}
