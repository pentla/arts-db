use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use super::page::{PageId, PAGE_SIZE};

pub struct DiskManager {
    heap_file: File,
    next_page_id: u64,
}

impl DiskManager {
    pub fn new(heap_file: File) -> io::Result<Self> {
        // ファイルサイズを取得
        let heap_file_size = heap_file.metadata()?.len();
        let next_page_id = heap_file_size / PAGE_SIZE as u64;
        Ok(Self {
            heap_file,
            next_page_id,
        })
    }
    pub fn open(heap_file_path: impl AsRef<Path>) -> io::Result<Self> {
        let heap_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(heap_file_path)?;
        Self::new(heap_file)
    }
    // 新しいPageIdを採番する
    pub fn allocate_page(&mut self) -> PageId {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        PageId(page_id)
    }
    // 指定されたページIDのページデータを読み込み、バイト配列に書き込みます。
    pub fn read_page_data(&mut self, page_id: PageId, data: &mut [u8]) -> io::Result<()> {
        //　pageIDが不正な場合はエラーを返す
        if page_id.to_u64() >= self.next_page_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid page ID",
            ));
        }

        // データサイズがPAGE_SIZEと一致しない場合はエラーを返す
        if data.len() != PAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid data buffer size",
            ));
        }

        let offset = PAGE_SIZE as u64 * page_id.to_u64();
        self.heap_file.seek(SeekFrom::Start(offset))?;
        self.heap_file.read_exact(data)
    }
    // 指定されたページIDの位置にページデータを書き込みます。
    pub fn write_page_data(&mut self, page_id: PageId, data: &[u8]) -> io::Result<()> {
        // offsetを計算
        let offset = PAGE_SIZE as u64 * page_id.to_u64();
        self.heap_file.seek(SeekFrom::Start(offset))?;
        self.heap_file.write_all(data)
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.heap_file.flush()?;
        self.heap_file.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test() {
        let (data_file, data_file_path) = NamedTempFile::new().unwrap().into_parts();
        let mut disk = DiskManager::new(data_file).unwrap();
        let mut hello = Vec::with_capacity(PAGE_SIZE);
        hello.extend_from_slice(b"hello");
        hello.resize(PAGE_SIZE, 0);
        let hello_page_id = disk.allocate_page();
        disk.write_page_data(hello_page_id, &hello).unwrap();
        let mut world = Vec::with_capacity(PAGE_SIZE);
        world.extend_from_slice(b"world");
        world.resize(PAGE_SIZE, 0);
        let world_page_id = disk.allocate_page();
        disk.write_page_data(world_page_id, &world).unwrap();
        drop(disk);
        let mut disk2 = DiskManager::open(&data_file_path).unwrap();
        let mut buf = vec![0; PAGE_SIZE];
        disk2.read_page_data(hello_page_id, &mut buf).unwrap();
        assert_eq!(hello, buf);
        disk2.read_page_data(world_page_id, &mut buf).unwrap();
        assert_eq!(world, buf);
    }
}
