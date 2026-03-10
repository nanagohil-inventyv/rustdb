// ─────────────────────────────────────────────────────────────────────────────
//  catalog.rs  —  Persist table schemas (names, columns, first_page_id)
// ─────────────────────────────────────────────────────────────────────────────
//
//  The catalog is stored as a simple binary file: catalog.bin
//
//  File format:
//    [4 bytes] num_tables : u32
//    For each table:
//      [4 bytes]  name_len    : u32
//      [N bytes]  name        : UTF-8
//      [4 bytes]  first_page_id : u32
//      [4 bytes]  num_columns : u32
//      For each column:
//        [4 bytes]  col_name_len : u32
//        [N bytes]  col_name     : UTF-8
//        [1 byte]   data_type    : u8  (0=Int 1=Float 2=Text 3=Bool)
//        [1 byte]   nullable     : u8  (0=false 1=true)

use std::collections::HashMap;
use std::fs;
use std::io::{Cursor, Read, Write};
use std::path::PathBuf;

use crate::types::{ColumnDef, DataType, DbError, DbResult};

// ─── Schema record stored in the catalog ─────────────────────────────────────

#[derive(Debug, Clone)]
pub struct TableMeta {
    pub name:          String,
    pub columns:       Vec<ColumnDef>,
    pub first_page_id: u32,
}

// ─── Catalog ─────────────────────────────────────────────────────────────────

pub struct Catalog {
    path:   PathBuf,
    pub tables: HashMap<String, TableMeta>,  // key = lower-case name
}

impl Catalog {
    pub fn open(data_dir: &str) -> DbResult<Self> {
        let mut path = PathBuf::from(data_dir);
        path.push("catalog.bin");

        let mut catalog = Catalog { path, tables: HashMap::new() };

        if catalog.path.exists() {
            catalog.load()?;
        }

        Ok(catalog)
    }

    pub fn save(&self) -> DbResult<()> {
        let mut buf: Vec<u8> = Vec::new();

        write_u32(&mut buf, self.tables.len() as u32);

        for meta in self.tables.values() {
            write_str(&mut buf, &meta.name);
            write_u32(&mut buf, meta.first_page_id);
            write_u32(&mut buf, meta.columns.len() as u32);
            for col in &meta.columns {
                write_str(&mut buf, &col.name);
                buf.push(encode_type(&col.data_type));
                buf.push(if col.nullable { 1 } else { 0 });
            }
        }

        fs::write(&self.path, &buf).map_err(|e| DbError::IoError(e.to_string()))?;
        Ok(())
    }

    fn load(&mut self) -> DbResult<()> {
        let bytes = fs::read(&self.path).map_err(|e| DbError::IoError(e.to_string()))?;
        let mut c = Cursor::new(bytes);

        let num_tables = read_u32(&mut c);
        for _ in 0..num_tables {
            let name          = read_str(&mut c);
            let first_page_id = read_u32(&mut c);
            let num_cols      = read_u32(&mut c);

            let mut columns = Vec::new();
            for _ in 0..num_cols {
                let col_name  = read_str(&mut c);
                let type_byte = read_u8(&mut c);
                let nullable  = read_u8(&mut c) == 1;
                columns.push(ColumnDef {
                    name:      col_name,
                    data_type: decode_type(type_byte),
                    nullable,
                });
            }

            let key = name.to_lowercase();
            self.tables.insert(key, TableMeta { name, columns, first_page_id });
        }

        Ok(())
    }
}

// ─── Binary helpers ───────────────────────────────────────────────────────────

fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn write_str(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    write_u32(buf, b.len() as u32);
    buf.extend_from_slice(b);
}

fn read_u32(c: &mut Cursor<Vec<u8>>) -> u32 {
    let mut arr = [0u8; 4];
    c.read_exact(&mut arr).unwrap();
    u32::from_be_bytes(arr)
}

fn read_u8(c: &mut Cursor<Vec<u8>>) -> u8 {
    let mut arr = [0u8; 1];
    c.read_exact(&mut arr).unwrap();
    arr[0]
}

fn read_str(c: &mut Cursor<Vec<u8>>) -> String {
    let len = read_u32(c) as usize;
    let mut b = vec![0u8; len];
    c.read_exact(&mut b).unwrap();
    String::from_utf8(b).unwrap()
}

fn encode_type(dt: &DataType) -> u8 {
    match dt {
        DataType::Integer => 0,
        DataType::Float   => 1,
        DataType::Text    => 2,
        DataType::Boolean => 3,
    }
}

fn decode_type(b: u8) -> DataType {
    match b {
        0 => DataType::Integer,
        1 => DataType::Float,
        2 => DataType::Text,
        3 => DataType::Boolean,
        _ => panic!("Unknown type byte {}", b),
    }
}
