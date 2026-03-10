# RustDB — Full RDBMS with Binary Page Storage

A complete relational database engine in Rust.
---

## Quick Start

```bash
cargo run
cargo run -- --data ./mydb    # custom data directory
```

---

## Architecture

```
SQL string typed by user
        │
   parser.rs          tokenizer + recursive descent parser → AST Statement
        │
   engine.rs          executes Statement against page storage
        │
   catalog.rs         persists table schemas (catalog.bin)
        │
   buffer_pool.rs     in-memory page cache (HashMap + dirty flag)
        │
   disk_manager.rs    seek to N × 4096, read/write exactly 4096 bytes
        │
   page.rs            slotted [u8; 4096] — slot dir grows ↓, rows grow ↑
        │
   serializer.rs      Value ↔ bytes (null flag + type-specific encoding)
        │
   types.rs           DataType, Value, ColumnDef, Condition, DbError
```

---

## Data directory layout

```
./data/
  catalog.bin        ← binary file: all table names, schemas, first_page_ids
  users.db           ← binary page file for the "users" table
  orders.db          ← binary page file for the "orders" table
  ...
```

Each `.db` file is a flat array of 4096-byte pages:

```
byte 0         → Page 0   (header: next_page_id, num_slots, free_end)
byte 4096      → Page 1
byte 8192      → Page 2
...
```

---

## Supported SQL

```sql
CREATE TABLE users (id INTEGER NOT NULL, name TEXT, score FLOAT, active BOOLEAN);
DROP TABLE users;

INSERT INTO users VALUES (1, 'Alice', 9.5, true);
INSERT INTO users (id, name) VALUES (2, 'Bob');

SELECT * FROM users;
SELECT name, score FROM users WHERE score > 5.0;

UPDATE users SET score = 10.0 WHERE id = 1;

DELETE FROM users WHERE active = false;

SHOW TABLES;
DESCRIBE users;
PAGES users;        ← shows how many pages the table uses and rows per page
```

---

## On-disk binary formats

### Value wire format
```
[1 byte]  null flag   0x00 = value follows  |  0x01 = NULL
[N bytes] payload:
  INTEGER  → 8 bytes big-endian i64
  FLOAT    → 8 bytes big-endian u64 (f64 bit pattern)
  BOOLEAN  → 1 byte
  TEXT     → 4 bytes u32 length + N bytes UTF-8
```

### Page layout (4096 bytes)
```
[0..3]   next_page_id  u32
[4..5]   num_slots     u16
[6..7]   free_end      u16
[8..]    slot[n]: offset u16 + length u16   (grows downward ↓)
         ...free space...
         row bytes packed from bottom up    (grows upward ↑)
```

### Catalog format (catalog.bin)
```
[4 bytes] num_tables
for each table:
  [4+N bytes] name (length-prefixed string)
  [4 bytes]   first_page_id
  [4 bytes]   num_columns
  for each column:
    [4+N bytes] col_name
    [1 byte]    data_type  (0=INT 1=FLOAT 2=TEXT 3=BOOL)
    [1 byte]    nullable
```