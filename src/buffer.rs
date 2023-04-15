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

// u8の型の配列をPAGE_SIXE(4096個)確保する。
pub type Page = [u8; PAGE_SIZE];

#[derive(Debug)]
pub struct Buffer {
    // Disk側のpageID
    pub page_id: PageId,
    // バッファとしてデータを保存するPAGE_SIZEの大きさの配列
    pub page: RefCell<Page>,
    // バッファの値が書き換えられており、ディスク上の値が古くなっている状態のこと
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
    // bufferの使用回数。多いほどクリアされづらくなる
    pub usage_count: u64,
    pub buffer: Rc<Buffer>,
}

/*
    ページデータをメモリ上に管理するためのBufferPoolを定義しています。
    BufferPoolはBufferの集合体で、
    Bufferはディスク上のページIDとそのページデータを保存する配列、
    そしてBufferの状態が古くなっているかどうかを示すis_dirtyフラグを持っています。

    BufferPoolには、
    新しいBufferを追加するためのメソッドと、古いBufferを捨てるためのメソッドが定義されています。
    古いBufferを捨てるメソッドでは、Clock-sweepアルゴリズムを使用して、再利用しなさそうなBufferを捨てるようにしています。
*/
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
        Clock-sweepアルゴリズムは、特定の条件を満たすフレームを置き換えるために使用されます。
        置き換えるフレームを選択するために、clock-sweepアルゴリズムは単純なカウンタを使用し、
        バッファプール内のすべてのフレームを周回します。
        フレームが使用されていない場合、または使用回数が低い場合は、そのフレームを置き換えることができます。
        使用回数が高いフレームは、しばらく使用され続ける可能性が高いため、置き換えるのが難しいとされます。
        clock-sweepアルゴリズムは、バッファプールが大きくなるにつれて、
        時間がかかる傾向があるため、大規模なシステムでは使用しない方が良い場合もあります。
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
