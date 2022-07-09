use anyhow::Result;

use artsdb::btree::{BTree, SearchMode};
use artsdb::buffer::BufferPool;
use artsdb::buffer_pool_manager::BufferPoolManager;
use artsdb::disk::{DiskManager, PageId};
use artsdb::tuple;

fn main() -> Result<()> {
    let disk = DiskManager::open("simple.rly")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = BufferPoolManager::new(disk, pool);

    let btree = BTree::new(PageId(0));
    let mut iter = btree.search(&mut bufmgr, SearchMode::Start)?;

    while let Some((key, value)) = iter.next(&mut bufmgr)? {
        let mut record = vec![];
        tuple::decode(&key, &mut record);
        tuple::decode(&value, &mut record);
        println!("{:?}", tuple::Pretty(&record));
    }
    Ok(())
}
