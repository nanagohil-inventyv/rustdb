// ─────────────────────────────────────────────────────────────────────────────
//  buffer_pool.rs  —  Per-table in-memory page cache
// ─────────────────────────────────────────────────────────────────────────────

use std::collections::HashMap;

use crate::disk_manager::DiskManager;
use crate::page::Page;
use crate::types::DbResult;

struct Frame {
    page:  Page,
    dirty: bool,
}

pub struct BufferPool {
    frames:   HashMap<u32, Frame>,
    capacity: usize,
    pub disk: DiskManager,
}

impl BufferPool {
    pub fn new(disk: DiskManager, capacity: usize) -> Self {
        BufferPool { frames: HashMap::new(), capacity, disk }
    }

    // ─── Public page access ───────────────────────────────────────────────

    pub fn fetch_page(&mut self, page_id: u32) -> DbResult<&Page> {
        self.ensure_loaded(page_id)?;
        Ok(&self.frames[&page_id].page)
    }

    pub fn fetch_page_mut(&mut self, page_id: u32) -> DbResult<&mut Page> {
        self.ensure_loaded(page_id)?;
        let frame = self.frames.get_mut(&page_id).unwrap();
        frame.dirty = true;
        Ok(&mut frame.page)
    }

    /// Allocate a new page on disk, load it into the pool, return its id.
    pub fn new_page(&mut self) -> DbResult<u32> {
        let page_id = self.disk.allocate_page()?;
        self.maybe_evict()?;
        self.frames.insert(page_id, Frame { page: Page::new(), dirty: true });
        Ok(page_id)
    }

    pub fn num_disk_pages(&self) -> u32 { self.disk.num_pages }

    // ─── Flush ────────────────────────────────────────────────────────────

    pub fn flush_all(&mut self) -> DbResult<()> {
        let dirty: Vec<u32> = self.frames.iter()
            .filter(|(_, f)| f.dirty)
            .map(|(&id, _)| id)
            .collect();
        for id in dirty {
            let frame = self.frames.get_mut(&id).unwrap();
            self.disk.write_page(id, &frame.page)?;
            frame.dirty = false;
        }
        Ok(())
    }

    pub fn flush_page(&mut self, page_id: u32) -> DbResult<()> {
        if let Some(frame) = self.frames.get_mut(&page_id) {
            if frame.dirty {
                self.disk.write_page(page_id, &frame.page)?;
                frame.dirty = false;
            }
        }
        Ok(())
    }

    // ─── Stats ────────────────────────────────────────────────────────────

    pub fn cached_count(&self) -> usize  { self.frames.len() }
    pub fn dirty_count(&self)  -> usize  { self.frames.values().filter(|f| f.dirty).count() }

    // ─── Internals ────────────────────────────────────────────────────────

    fn ensure_loaded(&mut self, page_id: u32) -> DbResult<()> {
        if self.frames.contains_key(&page_id) { return Ok(()); }
        self.maybe_evict()?;
        let page = self.disk.read_page(page_id)?;
        self.frames.insert(page_id, Frame { page, dirty: false });
        Ok(())
    }

    fn maybe_evict(&mut self) -> DbResult<()> {
        if self.frames.len() < self.capacity { return Ok(()); }
        let victim = *self.frames.keys().min().unwrap();
        let frame  = self.frames.remove(&victim).unwrap();
        if frame.dirty { self.disk.write_page(victim, &frame.page)?; }
        Ok(())
    }
}
