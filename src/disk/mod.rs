mod disk;
mod page;

pub use crate::disk::disk::DiskManager;
pub use crate::disk::page::{PageId, PAGE_SIZE};
