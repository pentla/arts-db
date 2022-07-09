use crate::{
    buffer::BufferPool,
    buffer_pool_manager::BufferPoolManager,
    disk::{DiskManager, PageId},
    table::SimpleTable,
};
use anyhow::Result;

mod bsearch;
mod btree;
mod buffer;
mod buffer_pool_manager;
mod disk;
mod memcmpable;
mod slotted;
mod table;
mod tuple;

fn main() -> Result<()> {
    println!("Hello, world!");
    // ファイルを開き、そのpathを返す
    let disk = DiskManager::open("simple.rly")?;

    //
    let pool = BufferPool::new(10);
    let mut bufmgr = BufferPoolManager::new(disk, pool);
    let mut table = SimpleTable {
        meta_page_id: PageId(0),
        num_key_elems: 1,
    };
    table.create(&mut bufmgr)?;
    dbg!(&table);
    table.insert(&mut bufmgr, &[b"z", b"Alice", b"Smith"])?;
    table.insert(&mut bufmgr, &[b"x", b"Bob", b"Johnson"])?;
    table.insert(&mut bufmgr, &[b"y", b"Charlie", b"Williams"])?;
    table.insert(&mut bufmgr, &[b"w", b"Dave", b"Miller"])?;
    table.insert(&mut bufmgr, &[b"v", b"Eve", b"Brown"])?;

    bufmgr.flush()?;
    Ok(())
}
