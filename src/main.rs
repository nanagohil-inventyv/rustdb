// ─────────────────────────────────────────────────────────────────────────────
//  main.rs  —  Interactive SQL shell
// ─────────────────────────────────────────────────────────────────────────────
//
//  cargo run
//  cargo run -- --data ./mydb    (custom data directory)

mod buffer_pool;
mod catalog;
mod disk_manager;
mod display;
mod engine;
mod page;
mod parser;
mod serializer;
mod types;

use std::io::{self, Write};

use engine::Engine;
use parser::{parse, Statement};
use types::{ColumnDef, DbResult};

const BANNER: &str = r#"
 ____           _   ____  ____
|  _ \ _   _ __| |_|  _ \| __ )
| |_) | | | / _` | | | | |  _ \
|  _ <| |_| \_ _| | |_| | |_) |
|_| \_\\__,_|__,_|_|____/|____/

  Binary page-based RDBMS in Rust
  Data stored as 4 KB pages on disk
  Type .help for commands, .exit to quit
"#;

const HELP: &str = r#"
SQL Commands:
  CREATE TABLE t (col1 INTEGER NOT NULL, col2 TEXT, col3 FLOAT, col4 BOOLEAN)
  DROP TABLE t
  INSERT INTO t (col1, col2) VALUES (1, 'hello')
  INSERT INTO t VALUES (1, 'hello', 3.14, true)
  SELECT * FROM t
  SELECT col1, col2 FROM t WHERE col1 > 5
  UPDATE t SET col2 = 'world' WHERE col1 = 1
  DELETE FROM t WHERE col1 = 1
  SHOW TABLES
  DESCRIBE t
  PAGES t                ← show page-level storage stats for table t

Meta commands:
  .help                  this message
  .exit / .quit          flush everything and exit

Data types:   INTEGER | FLOAT | TEXT | BOOLEAN
Operators:    =  !=  <  <=  >  >=
Literals:     42 | 3.14 | 'text' | true | false | NULL
"#;

fn main() {
    println!("{}", BANNER);

    // Allow --data <dir> flag
    let args: Vec<String> = std::env::args().collect();
    let data_dir = if let Some(pos) = args.iter().position(|a| a == "--data") {
        args.get(pos + 1).map(|s| s.as_str()).unwrap_or("./data")
    } else {
        "./data"
    };

    println!("  Data directory: {}\n", data_dir);

    let mut engine = match Engine::open(data_dir) {
        Ok(e) => e,
        Err(e) => { eprintln!("Failed to open database: {}", e); std::process::exit(1); }
    };

    let mut buffer = String::new();

    loop {
        let prompt = if buffer.trim().is_empty() { "rustdb> " } else { "     -> " };
        print!("{}", prompt);
        io::stdout().flush().unwrap();

        let mut line = String::new();
        match io::stdin().read_line(&mut line) {
            Ok(0) => { shutdown(&mut engine); break; }
            Ok(_) => {}
            Err(e) => { eprintln!("Read error: {}", e); break; }
        }

        let trimmed = line.trim();

        match trimmed {
            ".exit" | ".quit" => { shutdown(&mut engine); break; }
            ".help"           => { println!("{}", HELP); buffer.clear(); continue; }
            ""                => continue,
            _                 => {}
        }

        buffer.push(' ');
        buffer.push_str(trimmed);

        // Execute when we see a ';' or when the input looks like a complete one-liner
        let ready = trimmed.ends_with(';')
            || (!trimmed.is_empty() && looks_complete(buffer.trim()));

        if ready {
            let query = buffer.trim().to_string();
            buffer.clear();
            if query.is_empty() { continue; }

            match parse(&query) {
                Err(e)   => println!("Parse error: {}", e),
                Ok(stmt) => {
                    if let Err(e) = execute(&mut engine, stmt) {
                        println!("Error: {}", e);
                    }
                }
            }
        }
    }
}

// ─── Execute one parsed statement ────────────────────────────────────────────

fn execute(engine: &mut Engine, stmt: Statement) -> DbResult<()> {
    match stmt {

        Statement::CreateTable { table, columns } => {
            let cols: Vec<ColumnDef> = columns.into_iter()
                .map(|c| ColumnDef::new(&c.name, c.data_type, c.nullable))
                .collect();
            engine.create_table(table.clone(), cols)?;
            println!("Table '{}' created.", table);
        }

        Statement::DropTable { table } => {
            engine.drop_table(&table)?;
            println!("Table '{}' dropped.", table);
        }

        Statement::Insert { table, columns, values } => {
            engine.insert(&table, columns, values)?;
            println!("1 row inserted.");
        }

        Statement::Select { table, columns, condition } => {
            let (headers, rows) = engine.select(&table, &columns, &condition)?;
            display::print_table(&headers, &rows);
        }

        Statement::Update { table, assignments, condition } => {
            let n = engine.update(&table, assignments, &condition)?;
            println!("{} row(s) updated.", n);
        }

        Statement::Delete { table, condition } => {
            let n = engine.delete(&table, &condition)?;
            println!("{} row(s) deleted.", n);
        }

        Statement::ShowTables => {
            let names = engine.table_names();
            if names.is_empty() {
                println!("(no tables)");
            } else {
                println!("Tables:");
                for n in &names { println!("  {}", n); }
            }
        }

        Statement::Describe(table) => {
            let meta = engine.table_meta(&table)?;
            println!("Table: {}", meta.name);
            println!("{:<20} {:<12} {}", "Column", "Type", "Nullable");
            println!("{}", "-".repeat(46));
            for col in &meta.columns {
                println!("{:<20} {:<12} {}",
                    col.name, col.data_type,
                    if col.nullable { "YES" } else { "NO" });
            }
        }

        Statement::PageStats(table) => {
            let stats = engine.page_stats(&table)?;
            println!("Page storage for '{}':", table);
            println!("{:<10} {}", "Page ID", "Rows (slots)");
            println!("{}", "-".repeat(25));
            let total: u16 = stats.iter().map(|(_, s)| s).sum();
            for (page_id, slots) in &stats {
                println!("{:<10} {}", page_id, slots);
            }
            println!("{}", "-".repeat(25));
            println!("  {} page(s), {} total slot(s)", stats.len(), total);
        }
    }
    Ok(())
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn shutdown(engine: &mut Engine) {
    if let Err(e) = engine.close() {
        eprintln!("Warning: error during shutdown: {}", e);
    }
    println!("Bye! All pages flushed to disk.");
}

fn looks_complete(input: &str) -> bool {
    let up = input.trim().to_uppercase();
    ["SELECT","INSERT","UPDATE","DELETE","CREATE","DROP","SHOW","DESCRIBE","DESC","PAGES"]
        .iter().any(|kw| up.starts_with(kw))
}
