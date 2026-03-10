// ─────────────────────────────────────────────────────────────────────────────
//  types.rs  —  All shared types: DataType, Value, ColumnDef, Condition, errors
// ─────────────────────────────────────────────────────────────────────────────

use std::fmt;

// ─── Data types ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Integer,  // i64  — 8 bytes on disk
    Float,    // f64  — 8 bytes on disk
    Text,     // variable — 4-byte length prefix + N UTF-8 bytes
    Boolean,  // bool — 1 byte on disk
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Float   => write!(f, "FLOAT"),
            DataType::Text    => write!(f, "TEXT"),
            DataType::Boolean => write!(f, "BOOLEAN"),
        }
    }
}

// ─── Values ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
    Null,
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(fl)  => write!(f, "{:.4}", fl),
            Value::Text(s)    => write!(f, "{}", s),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Null       => write!(f, "NULL"),
        }
    }
}

impl Value {
    /// Check whether this value is compatible with a declared column type.
    pub fn matches_type(&self, dtype: &DataType) -> bool {
        matches!(
            (self, dtype),
            (Value::Integer(_), DataType::Integer)
                | (Value::Float(_), DataType::Float)
                | (Value::Text(_), DataType::Text)
                | (Value::Boolean(_), DataType::Boolean)
                | (Value::Null, _)   // NULL is always allowed (nullable check is separate)
        )
    }

    /// Compare two values of the same type for WHERE clause evaluation.
    pub fn compare(&self, other: &Value) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Float(a),   Value::Float(b))   => a.partial_cmp(b),
            (Value::Text(a),    Value::Text(b))    => a.partial_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

// ─── Schema ───────────────────────────────────────────────────────────────────

/// One column in a table schema.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name:      String,
    pub data_type: DataType,
    pub nullable:  bool,
}

impl ColumnDef {
    pub fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        ColumnDef { name: name.to_string(), data_type, nullable }
    }
}

// ─── Conditions (WHERE clause) ────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Operator { Eq, Ne, Lt, Le, Gt, Ge }

#[derive(Debug, Clone)]
pub struct Condition {
    pub column:   String,
    pub operator: Operator,
    pub value:    Value,
}

impl Condition {
    /// Evaluate this condition against a row.
    /// `col_index` is the pre-resolved index of `self.column` in the row.
    pub fn evaluate(&self, cell: &Value) -> bool {
        let ord = cell.compare(&self.value);
        match self.operator {
            Operator::Eq => cell == &self.value,
            Operator::Ne => cell != &self.value,
            Operator::Lt => matches!(ord, Some(std::cmp::Ordering::Less)),
            Operator::Le => matches!(ord, Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)),
            Operator::Gt => matches!(ord, Some(std::cmp::Ordering::Greater)),
            Operator::Ge => matches!(ord, Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)),
        }
    }
}

// ─── Errors ───────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum DbError {
    TableNotFound(String),
    TableAlreadyExists(String),
    ColumnNotFound(String),
    TypeMismatch { column: String, expected: DataType, got: String },
    ParseError(String),
    IoError(String),
    InvalidQuery(String),
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::TableNotFound(t)      => write!(f, "Table not found: '{}'", t),
            DbError::TableAlreadyExists(t) => write!(f, "Table already exists: '{}'", t),
            DbError::ColumnNotFound(c)     => write!(f, "Column not found: '{}'", c),
            DbError::TypeMismatch { column, expected, got } =>
                write!(f, "Type mismatch in '{}': expected {}, got '{}'", column, expected, got),
            DbError::ParseError(msg)  => write!(f, "Parse error: {}", msg),
            DbError::IoError(msg)     => write!(f, "I/O error: {}", msg),
            DbError::InvalidQuery(msg)=> write!(f, "Invalid query: {}", msg),
        }
    }
}

// Convert std::io::Error automatically into DbError::IoError
impl From<std::io::Error> for DbError {
    fn from(e: std::io::Error) -> Self {
        DbError::IoError(e.to_string())
    }
}

pub type DbResult<T> = Result<T, DbError>;
