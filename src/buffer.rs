use std::{
    cell::{Cell, RefCell},
    io,
    rc::Rc,
};

use crate::disk::{PageId, PAGE_SIZE};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("no free buffer available in buffer pool")]
    NoFreeBuffer,
}

pub type Page = [u8; PAGE_SIZE];

#[derive(Debug)]
pub struct Buffer {
    pub page_id: PageId,
    pub page: RefCell<Page>,
    pub is_dirty: Cell<bool>,
}

impl Default for Buffer {
    fn default() -> Self {
        Self {
            page_id: Default::default(),
            page: RefCell::new([0u8; PAGE_SIZE]),
            is_dirty: Cell::new(false),
        }
    }
}

#[derive(Debug, Default, Copy, Clone)]
pub struct BufferId(pub usize);

#[derive(Default)]
pub struct Frame {
    // bufferの使用回数
    pub usage_count: u64,
    pub buffer: Rc<Buffer>,
}

pub struct BufferPool {
    pub buffers: Vec<Frame>,
    pub next_victim_id: BufferId,
}

impl BufferPool {
    pub fn new(pool_size: usize) -> Self {
        let mut buffers: Vec<Frame> = vec![];
        buffers.resize_with(pool_size, Default::default);
        let next_victim_id = BufferId::default();
        Self {
            buffers,
            next_victim_id,
        }
    }

    fn size(&self) -> usize {
        self.buffers.len()
    }
    /*
        Clock-sweepアルゴリズムを実装する。
        bufferの大きさには限りがあるので、
        再利用しなさそうなBufferを捨てるアルゴリズム
    */
    pub fn evict(&mut self) -> Option<BufferId> {
        let pool_size = self.size();
        let mut consecutive_pinned = 0;
        // bufferpoolの全てのbufferを巡回しながら捨てるものを決める
        let victim_id = loop {
            let next_victim_id = self.next_victim_id;
            let frame = &mut self[next_victim_id];
            if frame.usage_count == 0 {
                break self.next_victim_id;
            }
            // 巡回中に貸出中でなければデクリメントされる
            if Rc::get_mut(&mut frame.buffer).is_some() {
                frame.usage_count -= 1;
                consecutive_pinned = 0;
            } else {
                /*
                    貸出中だった場合はconsective_pinnedカウンタを増やす。
                    カウンタがbuffer_poolと同じになった場合にはすべてのbufferが貸出中ということなので、
                    諦めてNoneを返す
                */
                consecutive_pinned += 1;
                if consecutive_pinned >= pool_size {
                    return None;
                }
            }
            self.next_victim_id = self.increment_id(self.next_victim_id);
        };
        Some(victim_id)
    }

    fn increment_id(&self, buffer_id: BufferId) -> BufferId {
        BufferId((buffer_id.0 + 1) % self.size())
    }
}
