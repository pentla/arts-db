use std::collections::HashMap;
use std::ops::{Index, IndexMut};
use std::rc::Rc;

use crate::buffer::{Buffer, BufferId, BufferPool, Error, Frame};
use crate::disk::{DiskManager, PageId};

/*
    Diskは遅いので、直接扱わず、メモリで結果を返すようにするための仕組み。
*/
pub struct BufferPoolManager {
    disk: DiskManager,
    // メモリ上に管理するバッファプール
    pool: BufferPool,
    // どのページのデータがどのバッファに入っているかの対応関係を管理する
    page_table: HashMap<PageId, BufferId>,
}

impl BufferPoolManager {
    pub fn new(disk: DiskManager, pool: BufferPool) -> Self {
        let page_table = HashMap::new();
        Self {
            disk,
            pool,
            page_table,
        }
    }
    // bufferの貸し出し処理をする
    pub fn fetch_page(&mut self, page_id: PageId) -> Result<Rc<Buffer>, Error> {
        // pageがbuffer_poolにある場合はそのバッファを貸し出す
        if let Some(&buffer_id) = self.page_table.get(&page_id) {
            let frame = &mut self.pool[buffer_id];
            frame.usage_count += 1;
            return Ok(frame.buffer.clone());
        }

        // これから読み込むページを格納するbufferを決定する
        let buffer_id = self.pool.evict().ok_or(Error::NoFreeBuffer)?;
        let frame = &mut self.pool[buffer_id];
        let evict_page_id = frame.buffer.page_id;
        {
            // 取得したbufferがis_dirtyだった場合は、そのバッファをdiskに書き出す。
            // is_dirtyはバッファの内容が変更されていて、disk上の内容が古くなっていることを示す
            let buffer = Rc::get_mut(&mut frame.buffer).unwrap();
            if buffer.is_dirty.get() {
                self.disk.write_page_data(page_id, buffer.page.get_mut())?;
            }
            buffer.page_id = page_id;
            buffer.is_dirty.set(false);

            // ページを読み出す。
            self.disk.read_page_data(page_id, buffer.page.get_mut())?;
            frame.usage_count = 1;
        }
        // バッファに入っているページが入れ替わったので、page_tableを更新する
        let page = Rc::clone(&frame.buffer);
        self.page_table.remove(&evict_page_id);
        self.page_table.insert(page_id, buffer_id);
        Ok(page)
    }

    pub fn create_page(&mut self) -> Result<Rc<Buffer>, Error> {
        let buffer_id = self.pool.evict().ok_or(Error::NoFreeBuffer)?;
        let frame = &mut self.pool[buffer_id];
        let evict_page_id = frame.buffer.page_id;
        let page_id = {
            let buffer = Rc::get_mut(&mut frame.buffer).unwrap();
            if buffer.is_dirty.get() {
                self.disk
                    .write_page_data(evict_page_id, buffer.page.get_mut())?;
            }
            let page_id = self.disk.allocate_page();
            *buffer = Buffer::default();
            buffer.page_id = page_id;
            buffer.is_dirty.set(true);
            frame.usage_count = 1;
            page_id
        };
        let page = Rc::clone(&frame.buffer);
        self.page_table.remove(&evict_page_id);
        self.page_table.insert(page_id, buffer_id);
        Ok(page)
    }

    pub fn flush(&mut self) -> Result<(), Error> {
        for (&page_id, &buffer_id) in self.page_table.iter() {
            let frame = &self.pool[buffer_id];
            let mut page = frame.buffer.page.borrow_mut();
            self.disk.write_page_data(page_id, page.as_mut())?;
            frame.buffer.is_dirty.set(false);
        }
        self.disk.sync()?;
        Ok(())
    }
}

impl Index<BufferId> for BufferPool {
    type Output = Frame;
    fn index(&self, index: BufferId) -> &Self::Output {
        &self.buffers[index.0]
    }
}

impl IndexMut<BufferId> for BufferPool {
    fn index_mut(&mut self, index: BufferId) -> &mut Self::Output {
        &mut self.buffers[index.0]
    }
}
