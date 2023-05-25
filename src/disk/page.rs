use zerocopy::{AsBytes, FromBytes};

/*
    ファイルサイズの単位。
    Linuxのext4のファイルサイズが4096のため、ページサイズはこの整数倍とされていることが多い。
*/
pub const PAGE_SIZE: usize = 4096;

#[derive(Debug, Default, Eq, PartialEq, Hash, Copy, Clone, FromBytes, AsBytes)]
#[repr(C)]
pub struct PageId(pub u64);

impl PageId {
    pub const INVALID_PAGE_ID: PageId = PageId(u64::MAX);

    pub fn valid(self) -> Option<PageId> {
        if self == Self::INVALID_PAGE_ID {
            None
        } else {
            Some(self)
        }
    }

    pub fn to_u64(self) -> u64 {
        self.0
    }
}

impl From<Option<PageId>> for PageId {
    fn from(page_id: Option<PageId>) -> Self {
        page_id.unwrap_or_default()
    }
}

impl From<&[u8]> for PageId {
    fn from(bytes: &[u8]) -> Self {
        let arr = bytes.try_into().unwrap();
        PageId(u64::from_ne_bytes(arr))
    }
}
