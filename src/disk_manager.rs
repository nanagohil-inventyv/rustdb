// ─────────────────────────────────────────────────────────────────────────────
//  disk_manager.rs  —  Read / write pages as raw bytes in a .db file
// ─────────────────────────────────────────────────────────────────────────────
//
//  The file is a flat array of PAGE_SIZE-byte blocks:
//    byte 0           → Page 0
//    byte PAGE_SIZE   → Page 1
//    byte N*PAGE_SIZE → Page N
//
//  One file per table:  <data_dir>/<table_name>.db

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::page::{Page, PAGE_SIZE};
use crate::types::DbResult;

pub struct DiskManager {
    file:          File,
    pub num_pages: u32,
}

impl DiskManager {
    pub fn open(path: &str) -> DbResult<Self> {
        let file = OpenOptions::new()
            .read(true).write(true).create(true)
            .open(path)?;
        let file_len = file.metadata()?.len() as usize;
        let num_pages = (file_len / PAGE_SIZE) as u32;
        Ok(DiskManager { file, num_pages })
    }

    pub fn read_page(&mut self, page_id: u32) -> DbResult<Page> {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = [0u8; PAGE_SIZE];
        self.file.read_exact(&mut buf)?;
        Ok(Page::from_bytes(buf))
    }

    pub fn write_page(&mut self, page_id: u32, page: &Page) -> DbResult<()> {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&page.data)?;
        self.file.flush()?;
        Ok(())
    }

    /// Extend the file by one blank page and return the new page_id.
    pub fn allocate_page(&mut self) -> DbResult<u32> {
        let new_id = self.num_pages;
        let offset = new_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&[0u8; PAGE_SIZE])?;
        self.file.flush()?;
        self.num_pages += 1;
        Ok(new_id)
    }

    pub fn file_size(&self) -> u64 {
        self.file.metadata().map(|m| m.len()).unwrap_or(0)
    }
}

/// Build the .db file path for a given table inside a data directory.
pub fn table_db_path(data_dir: &str, table_name: &str) -> String {
    let mut p = PathBuf::from(data_dir);
    p.push(format!("{}.db", table_name.to_lowercase()));
    p.to_string_lossy().to_string()
}
