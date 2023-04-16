use crate::buffer_pool_manager::BufferPoolManager;
use crate::disk::PageId;

pub trait Btree {
    fn create(&self, bufmgr: &mut BufferPoolManager) -> Result<()>;
    fn new(&self, page_id: PageId) -> Result<()>;
}
