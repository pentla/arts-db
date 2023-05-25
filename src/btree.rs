use anyhow::Error;

use crate::buffer_pool_manager::BufferPoolManager;
use crate::disk::PageId;

pub struct Btree {
    pub meta_page_id: PageId,
}

pub trait BtreeTrait {
    fn create(&self, bufmgr: &mut BufferPoolManager) -> Result<Btree, Error>;
    fn new(&self, page_id: PageId) -> Result<Btree, Error>;
}
