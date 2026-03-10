// ─────────────────────────────────────────────────────────────────────────────
//  display.rs  —  Pretty-print result sets as ASCII tables
// ─────────────────────────────────────────────────────────────────────────────

use crate::types::Value;

pub type Row = Vec<Value>;

pub fn print_table(headers: &[String], rows: &[Row]) {
    if headers.is_empty() { println!("(no columns)"); return; }

    // Column widths = max of header width and widest value in that column
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(format!("{}", val).len());
            }
        }
    }

    let sep = separator(&widths);
    println!("{}", sep);
    print_cells(headers.iter().map(|s| s.as_str()), &widths);
    println!("{}", sep);

    if rows.is_empty() {
        println!(" (0 rows)");
    } else {
        for row in rows {
            let cells: Vec<String> = row.iter().map(|v| format!("{}", v)).collect();
            print_cells(cells.iter().map(|s| s.as_str()), &widths);
        }
    }
    println!("{}", sep);
    println!("{} row(s)", rows.len());
}

fn separator(widths: &[usize]) -> String {
    let parts: Vec<String> = widths.iter().map(|&w| "-".repeat(w + 2)).collect();
    format!("+{}+", parts.join("+"))
}

fn print_cells<'a>(cells: impl Iterator<Item = &'a str>, widths: &[usize]) {
    let parts: Vec<String> = cells.zip(widths.iter())
        .map(|(c, &w)| format!(" {:width$} ", c, width = w))
        .collect();
    println!("|{}|", parts.join("|"));
}
