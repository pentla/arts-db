use anyhow::Result;

use artsdb::btree::BTree;
use artsdb::buffer::BufferPool;
use artsdb::buffer_pool_manager::BufferPoolManager;
use artsdb::disk::DiskManager;

fn main() -> Result<()> {
    let disk = DiskManager::open("test.btr")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = BufferPoolManager::new(disk, pool);

    println!("buffer initialized");
    let btree = BTree::create(&mut bufmgr)?;
    println!("btree initialized");

    btree.insert(&mut bufmgr, b"Kanagawa", b"Yokohama")?;
    btree.insert(&mut bufmgr, b"Osaka", b"Osaka")?;
    btree.insert(&mut bufmgr, b"Aichi", b"Nagoya")?;
    btree.insert(&mut bufmgr, b"Hokkaido", b"Sapporo")?;
    btree.insert(&mut bufmgr, b"Fukuoka", b"Fukuoka")?;
    btree.insert(&mut bufmgr, b"Hyogo", b"Kobe")?;

    println!("btree inserted");
    bufmgr.flush()?;

    Ok(())
}
