// ─────────────────────────────────────────────────────────────────────────────
//  engine.rs  —  Database engine: executes SQL statements against page storage
// ─────────────────────────────────────────────────────────────────────────────
//
//  The engine owns:
//    • catalog   — schema metadata (persisted to catalog.bin)
//    • pools     — one BufferPool per open table (HashMap<table_name, pool>)
//    • data_dir  — directory where .db files live
//
//  Flow for every SQL statement:
//    parser::parse(sql) → Statement → engine.execute(stmt) → DbResult
//
//  INSERT:  validate types → serialize → page.add_row → mark dirty
//  SELECT:  scan all pages → deserialize → filter with WHERE → project columns
//  UPDATE:  scan → find matching rows → re-serialize updated row → replace slot
//  DELETE:  scan → mark matching slots as tombstones → flush
//  CREATE:  add to catalog → allocate first page → save catalog
//  DROP:    remove from catalog → delete .db file → save catalog

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use crate::buffer_pool::BufferPool;
use crate::catalog::{Catalog, TableMeta};
use crate::disk_manager::{table_db_path, DiskManager};
use crate::page::NO_NEXT;
use crate::serializer::{deserialize_row, serialize_row};
use crate::types::{ColumnDef, Condition, DataType, DbError, DbResult, Operator, Value};

const POOL_CAPACITY: usize = 16;

pub type Row = Vec<Value>;

pub struct Engine {
    data_dir: String,
    catalog: Catalog,
    pools: HashMap<String, BufferPool>,
}

impl Engine {
    // ─── Open ──────────────────────────────────────────────────────────────

    pub fn open(data_dir: &str) -> DbResult<Self> {
        fs::create_dir_all(data_dir).map_err(|e| DbError::IoError(e.to_string()))?;

        let catalog = Catalog::open(data_dir)?;

        Ok(Engine {
            data_dir: data_dir.to_string(),
            catalog,
            pools: HashMap::new(),
        })
    }

    /// Flush all dirty pages and save the catalog on shutdown.
    pub fn close(&mut self) -> DbResult<()> {
        for pool in self.pools.values_mut() {
            pool.flush_all()?;
        }
        self.catalog.save()?;
        Ok(())
    }

    // ─── DDL ──────────────────────────────────────────────────────────────

    pub fn create_table(&mut self, name: String, columns: Vec<ColumnDef>) -> DbResult<()> {
        let key = name.to_lowercase();
        if self.catalog.tables.contains_key(&key) {
            return Err(DbError::TableAlreadyExists(name));
        }

        // Open a fresh pool for this table and allocate page 0
        let path = table_db_path(&self.data_dir, &key);
        let disk = DiskManager::open(&path)?;
        let mut pool = BufferPool::new(disk, POOL_CAPACITY);
        let first_page_id = pool.new_page()?;
        pool.flush_all()?;

        self.catalog.tables.insert(
            key.clone(),
            TableMeta {
                name: name.clone(),
                columns,
                first_page_id,
            },
        );
        self.catalog.save()?;
        self.pools.insert(key, pool);

        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> DbResult<()> {
        let key = name.to_lowercase();
        if !self.catalog.tables.contains_key(&key) {
            return Err(DbError::TableNotFound(name.to_string()));
        }

        // Remove from pool and catalog
        self.pools.remove(&key);
        self.catalog.tables.remove(&key);
        self.catalog.save()?;

        // Delete the .db file from disk
        let path = table_db_path(&self.data_dir, &key);
        if PathBuf::from(&path).exists() {
            fs::remove_file(&path).map_err(|e| DbError::IoError(e.to_string()))?;
        }

        Ok(())
    }

    // ─── INSERT ───────────────────────────────────────────────────────────

    pub fn insert(
        &mut self,
        table_name: &str,
        col_names: Option<Vec<String>>,
        values: Vec<Value>,
    ) -> DbResult<()> {
        let key = table_name.to_lowercase();
        let meta = self
            .catalog
            .tables
            .get(&key)
            .ok_or_else(|| DbError::TableNotFound(table_name.to_string()))?
            .clone();

        // Build the full row in schema order
        let row = self.build_row(&meta.columns, col_names, values)?;

        // Validate types + nullability
        validate_row(&row, &meta.columns)?;

        let row_bytes = serialize_row(&row);
        let pool = self.get_pool(&key)?;

        // Walk the page chain; insert into the first page with room
        let mut current_id = meta.first_page_id;
        loop {
            let (has_room, next_id) = {
                let page = pool.fetch_page(current_id)?;
                (page.has_room_for(row_bytes.len()), page.next_page_id())
            };

            if has_room {
                let page = pool.fetch_page_mut(current_id)?;
                page.add_row(&row_bytes)
                    .expect("has_room returned true but add_row failed");
                pool.flush_page(current_id)?;
                return Ok(());
            }

            if next_id == NO_NEXT {
                break;
            }
            current_id = next_id;
        }

        // No page had room — allocate a new one and link it
        let new_page_id = pool.new_page()?;
        {
            let tail = pool.fetch_page_mut(current_id)?;
            tail.set_next_page_id(new_page_id);
        }
        pool.flush_page(current_id)?;

        {
            let new_page = pool.fetch_page_mut(new_page_id)?;
            new_page
                .add_row(&row_bytes)
                .expect("fresh page must have room");
        }
        pool.flush_page(new_page_id)?;

        Ok(())
    }

    // ─── SELECT ───────────────────────────────────────────────────────────

    pub fn select(
        &mut self,
        table_name: &str,
        col_names: &[String],
        condition: &Option<Condition>,
    ) -> DbResult<(Vec<String>, Vec<Row>)> {
        let key = table_name.to_lowercase();
        let meta = self
            .catalog
            .tables
            .get(&key)
            .ok_or_else(|| DbError::TableNotFound(table_name.to_string()))?
            .clone();

        let star = col_names.len() == 1 && col_names[0] == "*";

        // Resolve which column indices to project
        let project_indices: Vec<usize> = if star {
            (0..meta.columns.len()).collect()
        } else {
            col_names
                .iter()
                .map(|c| column_index(&meta.columns, c))
                .collect::<DbResult<_>>()?
        };

        let headers: Vec<String> = project_indices
            .iter()
            .map(|&i| meta.columns[i].name.clone())
            .collect();

        // Resolve WHERE column index (if any)
        let cond_col_idx: Option<usize> = match condition {
            Some(c) => Some(column_index(&meta.columns, &c.column)?),
            None => None,
        };

        let schema_types: Vec<DataType> =
            meta.columns.iter().map(|c| c.data_type.clone()).collect();

        let pool = self.get_pool(&key)?;
        let mut results: Vec<Row> = Vec::new();
        let mut current_id = meta.first_page_id;

        loop {
            // Read all rows from this page (cloned out to release the borrow)
            let (raw_rows, next_id) = {
                let page = pool.fetch_page(current_id)?;
                let rows: Vec<Vec<u8>> = page.iter_live_rows().map(|(_, b)| b.to_vec()).collect();
                (rows, page.next_page_id())
            };

            for raw in raw_rows {
                let row = deserialize_row(&raw, &schema_types);

                // Apply WHERE filter
                if let (Some(cond), Some(idx)) = (condition, cond_col_idx) {
                    if !cond.evaluate(&row[idx]) {
                        continue;
                    }
                }

                // Project requested columns
                let projected: Row = project_indices.iter().map(|&i| row[i].clone()).collect();
                results.push(projected);
            }

            if next_id == NO_NEXT {
                break;
            }
            current_id = next_id;
        }

        Ok((headers, results))
    }

    // ─── UPDATE ───────────────────────────────────────────────────────────

    pub fn update(
        &mut self,
        table_name: &str,
        assignments: Vec<(String, Value)>,
        condition: &Option<Condition>,
    ) -> DbResult<usize> {
        let key = table_name.to_lowercase();
        let meta = self
            .catalog
            .tables
            .get(&key)
            .ok_or_else(|| DbError::TableNotFound(table_name.to_string()))?
            .clone();

        // Resolve assignment column indices + type-check up front
        let indexed_assignments: Vec<(usize, Value)> = assignments
            .into_iter()
            .map(|(col, val)| {
                let idx = column_index(&meta.columns, &col)?;
                let expected = &meta.columns[idx].data_type;
                if !val.matches_type(expected) {
                    return Err(DbError::TypeMismatch {
                        column: col,
                        expected: expected.clone(),
                        got: format!("{:?}", val),
                    });
                }
                Ok((idx, val))
            })
            .collect::<DbResult<_>>()?;

        let cond_col_idx: Option<usize> = match condition {
            Some(c) => Some(column_index(&meta.columns, &c.column)?),
            None => None,
        };

        let schema_types: Vec<DataType> =
            meta.columns.iter().map(|c| c.data_type.clone()).collect();

        // ── Phase 1: SCAN (read-only) ────────────────────────────────────────
        // Collect everything we need to change BEFORE touching the pool again.
        // The `pool` borrow lives only inside this block — it is fully dropped
        // at the closing brace, freeing `self` for the write phase below.

        let mut slots_to_delete: Vec<(u32, u16)> = Vec::new(); // (page_id, slot_id)
        let mut rows_to_insert: Vec<Vec<u8>> = Vec::new(); // updated row bytes

        {
            let pool = self.get_pool(&key)?;
            let mut current_id = meta.first_page_id;

            loop {
                // Read page header info, then let the borrow drop
                let (num_slots, next_id) = {
                    let page = pool.fetch_page(current_id)?;
                    (page.num_slots(), page.next_page_id())
                };

                for slot_id in 0..num_slots {
                    // Clone the raw bytes out immediately so the page borrow ends
                    let raw = {
                        let page = pool.fetch_page(current_id)?;
                        if page.is_deleted(slot_id) {
                            continue;
                        }
                        match page.get_row_bytes(slot_id) {
                            Some(b) => b.to_vec(),
                            None => continue,
                        }
                    };

                    let mut row = deserialize_row(&raw, &schema_types);

                    // WHERE check — skip rows that don't match
                    if let (Some(cond), Some(idx)) = (condition, cond_col_idx) {
                        if !cond.evaluate(&row[idx]) {
                            continue;
                        }
                    }

                    // Apply SET assignments to produce the new version of the row
                    for (col_idx, val) in &indexed_assignments {
                        row[*col_idx] = val.clone();
                    }

                    // Record what to delete and what to re-insert
                    slots_to_delete.push((current_id, slot_id));
                    rows_to_insert.push(serialize_row(&row));
                }

                if next_id == NO_NEXT {
                    break;
                }
                current_id = next_id;
            }
        } // ← pool borrow ends here; self is fully free again

        // ── Phase 2: WRITE ───────────────────────────────────────────────────
        // `self` has no active borrows now. Safe to call any &mut self method.

        let updated = slots_to_delete.len();

        // Tombstone the old slot on its page
        for (page_id, slot_id) in slots_to_delete {
            let pool = self.get_pool(&key)?;
            {
                let page = pool.fetch_page_mut(page_id)?;
                page.delete_slot(slot_id);
            }
            pool.flush_page(page_id)?;
        } // pool borrow drops at end of each iteration — no issue

        // Append the updated rows to the page chain
        for new_bytes in rows_to_insert {
            self.append_row_to_chain(&key, meta.first_page_id, &new_bytes)?;
        }

        Ok(updated)
    }

    // ─── DELETE ───────────────────────────────────────────────────────────

    pub fn delete(&mut self, table_name: &str, condition: &Option<Condition>) -> DbResult<usize> {
        let key = table_name.to_lowercase();
        let meta = self
            .catalog
            .tables
            .get(&key)
            .ok_or_else(|| DbError::TableNotFound(table_name.to_string()))?
            .clone();

        let cond_col_idx: Option<usize> = match condition {
            Some(c) => Some(column_index(&meta.columns, &c.column)?),
            None => None,
        };

        let schema_types: Vec<DataType> =
            meta.columns.iter().map(|c| c.data_type.clone()).collect();

        let pool = self.get_pool(&key)?;
        let mut deleted = 0usize;
        let mut current_id = meta.first_page_id;

        loop {
            let num_slots = {
                let page = pool.fetch_page(current_id)?;
                page.num_slots()
            };
            let next_id = {
                let page = pool.fetch_page(current_id)?;
                page.next_page_id()
            };

            for slot_id in 0..num_slots {
                let raw = {
                    let page = pool.fetch_page(current_id)?;
                    if page.is_deleted(slot_id) {
                        continue;
                    }
                    page.get_row_bytes(slot_id).map(|b| b.to_vec())
                };
                let raw = match raw {
                    Some(r) => r,
                    None => continue,
                };
                let row = deserialize_row(&raw, &schema_types);

                let should_delete = match (condition, cond_col_idx) {
                    (Some(cond), Some(idx)) => cond.evaluate(&row[idx]),
                    _ => true, // no WHERE = delete all
                };

                if should_delete {
                    let page = pool.fetch_page_mut(current_id)?;
                    page.delete_slot(slot_id);
                    pool.flush_page(current_id)?;
                    deleted += 1;
                }
            }

            if next_id == NO_NEXT {
                break;
            }
            current_id = next_id;
        }

        Ok(deleted)
    }

    // ─── SHOW / DESCRIBE ──────────────────────────────────────────────────

    pub fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .catalog
            .tables
            .values()
            .map(|m| m.name.clone())
            .collect();
        names.sort();
        names
    }

    pub fn table_meta(&self, name: &str) -> DbResult<&TableMeta> {
        self.catalog
            .tables
            .get(&name.to_lowercase())
            .ok_or_else(|| DbError::TableNotFound(name.to_string()))
    }

    pub fn page_stats(&mut self, table_name: &str) -> DbResult<Vec<(u32, u16)>> {
        let key = table_name.to_lowercase();
        let first = self
            .catalog
            .tables
            .get(&key)
            .ok_or_else(|| DbError::TableNotFound(table_name.to_string()))?
            .first_page_id;

        let pool = self.get_pool(&key)?;
        let mut stats = Vec::new();
        let mut current_id = first;
        loop {
            let (slots, next) = {
                let page = pool.fetch_page(current_id)?;
                (page.num_slots(), page.next_page_id())
            };
            stats.push((current_id, slots));
            if next == NO_NEXT {
                break;
            }
            current_id = next;
        }
        Ok(stats)
    }

    // ─── Private helpers ──────────────────────────────────────────────────

    /// Get (or lazily open) the buffer pool for a table.
    fn get_pool(&mut self, key: &str) -> DbResult<&mut BufferPool> {
        if !self.pools.contains_key(key) {
            let path = table_db_path(&self.data_dir, key);
            let disk = DiskManager::open(&path)?;
            self.pools
                .insert(key.to_string(), BufferPool::new(disk, POOL_CAPACITY));
        }
        Ok(self.pools.get_mut(key).unwrap())
    }

    /// Build a full-width row in schema order from optional named columns.
    fn build_row(
        &self,
        columns: &[ColumnDef],
        col_names: Option<Vec<String>>,
        values: Vec<Value>,
    ) -> DbResult<Row> {
        match col_names {
            None => Ok(values), // positional
            Some(names) => {
                if names.len() != values.len() {
                    return Err(DbError::InvalidQuery(format!(
                        "Column count ({}) != value count ({})",
                        names.len(),
                        values.len()
                    )));
                }
                let mut row = vec![Value::Null; columns.len()];
                for (name, val) in names.iter().zip(values.into_iter()) {
                    let idx = column_index(columns, name)?;
                    row[idx] = val;
                }
                Ok(row)
            }
        }
    }

    /// Append pre-serialized row bytes to a page chain (used by UPDATE).
    fn append_row_to_chain(
        &mut self,
        key: &str,
        first_page_id: u32,
        row_bytes: &[u8],
    ) -> DbResult<()> {
        let pool = self.get_pool(key)?;
        let mut current_id = first_page_id;

        loop {
            let (has_room, next_id) = {
                let page = pool.fetch_page(current_id)?;
                (page.has_room_for(row_bytes.len()), page.next_page_id())
            };
            if has_room {
                let page = pool.fetch_page_mut(current_id)?;
                page.add_row(row_bytes).unwrap();
                pool.flush_page(current_id)?;
                return Ok(());
            }
            if next_id == NO_NEXT {
                break;
            }
            current_id = next_id;
        }

        let new_id = pool.new_page()?;
        {
            let tail = pool.fetch_page_mut(current_id)?;
            tail.set_next_page_id(new_id);
        }
        pool.flush_page(current_id)?;
        {
            let p = pool.fetch_page_mut(new_id)?;
            p.add_row(row_bytes).unwrap();
        }
        pool.flush_page(new_id)?;
        Ok(())
    }
}

// ─── Free helpers ─────────────────────────────────────────────────────────────

pub fn column_index(columns: &[ColumnDef], name: &str) -> DbResult<usize> {
    columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| DbError::ColumnNotFound(name.to_string()))
}

pub fn validate_row(row: &[Value], columns: &[ColumnDef]) -> DbResult<()> {
    if row.len() != columns.len() {
        return Err(DbError::InvalidQuery(format!(
            "Expected {} values, got {}",
            columns.len(),
            row.len()
        )));
    }
    for (val, col) in row.iter().zip(columns.iter()) {
        if !val.matches_type(&col.data_type) {
            return Err(DbError::TypeMismatch {
                column: col.name.clone(),
                expected: col.data_type.clone(),
                got: format!("{:?}", val),
            });
        }
        if matches!(val, Value::Null) && !col.nullable {
            return Err(DbError::InvalidQuery(format!(
                "Column '{}' is NOT NULL",
                col.name
            )));
        }
    }
    Ok(())
}
