// ─────────────────────────────────────────────────────────────────────────────
//  page.rs  —  Fixed 4 KB slotted page
// ─────────────────────────────────────────────────────────────────────────────
//
//  Layout of data[0..4096]:
//
//  [0..3]   next_page_id : u32   (0xFFFFFFFF = no next page)
//  [4..5]   num_slots    : u16
//  [6..7]   free_end     : u16   (grows upward from PAGE_SIZE)
//  [8..]    slot dir     : [offset:u16, length:u16] × num_slots  (grows ↓)
//           ...free space...
//           row data packed from bottom of page upward (grows ↑)

pub const PAGE_SIZE:  usize = 4096;
pub const NO_NEXT:    u32   = 0xFFFF_FFFF;

const OFF_NEXT:   usize = 0;
const OFF_SLOTS:  usize = 4;
const OFF_FREE:   usize = 6;
const HEADER:     usize = 8;
const SLOT_BYTES: usize = 4; // u16 offset + u16 length

pub struct Page {
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new() -> Self {
        let mut p = Page { data: [0u8; PAGE_SIZE] };
        p.set_next_page_id(NO_NEXT);
        p.set_num_slots(0);
        p.set_free_end(PAGE_SIZE as u16);
        p
    }

    pub fn from_bytes(bytes: [u8; PAGE_SIZE]) -> Self {
        Page { data: bytes }
    }

    // ─── Header ───────────────────────────────────────────────────────────

    pub fn next_page_id(&self) -> u32 {
        u32::from_be_bytes(self.data[OFF_NEXT..OFF_NEXT+4].try_into().unwrap())
    }
    pub fn set_next_page_id(&mut self, id: u32) {
        self.data[OFF_NEXT..OFF_NEXT+4].copy_from_slice(&id.to_be_bytes());
    }
    pub fn has_next(&self) -> bool { self.next_page_id() != NO_NEXT }

    pub fn num_slots(&self) -> u16 {
        u16::from_be_bytes(self.data[OFF_SLOTS..OFF_SLOTS+2].try_into().unwrap())
    }
    fn set_num_slots(&mut self, n: u16) {
        self.data[OFF_SLOTS..OFF_SLOTS+2].copy_from_slice(&n.to_be_bytes());
    }

    fn free_end(&self) -> u16 {
        u16::from_be_bytes(self.data[OFF_FREE..OFF_FREE+2].try_into().unwrap())
    }
    fn set_free_end(&mut self, v: u16) {
        self.data[OFF_FREE..OFF_FREE+2].copy_from_slice(&v.to_be_bytes());
    }

    // ─── Slot directory ───────────────────────────────────────────────────

    fn slot_offset(slot_id: u16) -> usize {
        HEADER + slot_id as usize * SLOT_BYTES
    }

    fn read_slot(&self, slot_id: u16) -> (u16, u16) {
        let b = Self::slot_offset(slot_id);
        let off = u16::from_be_bytes(self.data[b..b+2].try_into().unwrap());
        let len = u16::from_be_bytes(self.data[b+2..b+4].try_into().unwrap());
        (off, len)
    }

    fn write_slot(&mut self, slot_id: u16, offset: u16, length: u16) {
        let b = Self::slot_offset(slot_id);
        self.data[b..b+2].copy_from_slice(&offset.to_be_bytes());
        self.data[b+2..b+4].copy_from_slice(&length.to_be_bytes());
    }

    // ─── Free space ───────────────────────────────────────────────────────

    fn slot_dir_end(&self) -> usize {
        HEADER + self.num_slots() as usize * SLOT_BYTES
    }

    pub fn free_space(&self) -> usize {
        let row_start = self.free_end() as usize;
        let dir_end   = self.slot_dir_end();
        if row_start > dir_end { row_start - dir_end } else { 0 }
    }

    pub fn has_room_for(&self, needed: usize) -> bool {
        self.free_space() >= needed + SLOT_BYTES
    }

    // ─── Row insert / read ────────────────────────────────────────────────

    /// Append row bytes to the page. Returns slot id or None if full.
    pub fn add_row(&mut self, row_bytes: &[u8]) -> Option<u16> {
        let len = row_bytes.len();
        if !self.has_room_for(len) { return None; }

        let new_end = self.free_end() - len as u16;
        self.set_free_end(new_end);
        self.data[new_end as usize..new_end as usize + len].copy_from_slice(row_bytes);

        let slot_id = self.num_slots();
        self.write_slot(slot_id, new_end, len as u16);
        self.set_num_slots(slot_id + 1);
        Some(slot_id)
    }

    /// Read raw bytes for a given slot id.
    pub fn get_row_bytes(&self, slot_id: u16) -> Option<&[u8]> {
        if slot_id >= self.num_slots() { return None; }
        let (off, len) = self.read_slot(slot_id);
        Some(&self.data[off as usize..off as usize + len as usize])
    }

    /// Iterate over all row byte slices on this page.
    pub fn iter_rows(&self) -> impl Iterator<Item = &[u8]> {
        (0..self.num_slots()).filter_map(|s| self.get_row_bytes(s))
    }

    // ─── Tombstone (soft delete) ──────────────────────────────────────────
    // A deleted slot has offset=0 AND length=0 — we skip it on scan.

    pub fn delete_slot(&mut self, slot_id: u16) {
        if slot_id < self.num_slots() {
            self.write_slot(slot_id, 0, 0);
        }
    }

    pub fn is_deleted(&self, slot_id: u16) -> bool {
        let (off, len) = self.read_slot(slot_id);
        off == 0 && len == 0
    }

    /// Iterate only live (non-deleted) rows with their slot ids.
    pub fn iter_live_rows(&self) -> impl Iterator<Item = (u16, &[u8])> {
        (0..self.num_slots()).filter_map(|s| {
            if self.is_deleted(s) { return None; }
            self.get_row_bytes(s).map(|b| (s, b))
        })
    }
}
