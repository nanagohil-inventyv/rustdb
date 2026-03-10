// ─────────────────────────────────────────────────────────────────────────────
//  parser.rs  —  SQL tokenizer + recursive descent parser
// ─────────────────────────────────────────────────────────────────────────────

use crate::types::{Condition, DataType, DbError, DbResult, Operator, Value};

// ─── AST ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name:      String,
    pub data_type: DataType,
    pub nullable:  bool,
}

#[derive(Debug)]
pub enum Statement {
    CreateTable { table: String, columns: Vec<ColumnDef> },
    DropTable   { table: String },
    Insert      { table: String, columns: Option<Vec<String>>, values: Vec<Value> },
    Select      { table: String, columns: Vec<String>, condition: Option<Condition> },
    Update      { table: String, assignments: Vec<(String, Value)>, condition: Option<Condition> },
    Delete      { table: String, condition: Option<Condition> },
    ShowTables,
    Describe(String),
    PageStats(String),
}

// ─── Tokens ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Keyword(String),
    Ident(String),
    IntLit(i64),
    FloatLit(f64),
    StrLit(String),
    BoolLit(bool),
    Null,
    Comma, Semicolon, LParen, RParen, Star,
    Eq, Ne, Lt, Le, Gt, Ge,
    Eof,
}

const KEYWORDS: &[&str] = &[
    "CREATE","DROP","TABLE","INSERT","INTO","VALUES","SELECT","FROM",
    "WHERE","UPDATE","SET","DELETE","SHOW","TABLES","DESCRIBE","DESC",
    "NOT","NULL","AND","INTEGER","FLOAT","TEXT","BOOLEAN","BOOL",
    "PAGES",
];

fn tokenize(input: &str) -> DbResult<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut chars  = input.chars().peekable();

    while let Some(&ch) = chars.peek() {
        match ch {
            ' '|'\t'|'\r'|'\n' => { chars.next(); }
            ',' => { chars.next(); tokens.push(Token::Comma); }
            ';' => { chars.next(); tokens.push(Token::Semicolon); }
            '(' => { chars.next(); tokens.push(Token::LParen); }
            ')' => { chars.next(); tokens.push(Token::RParen); }
            '*' => { chars.next(); tokens.push(Token::Star); }
            '=' => { chars.next(); tokens.push(Token::Eq); }
            '!' => {
                chars.next();
                if chars.peek() == Some(&'=') { chars.next(); tokens.push(Token::Ne); }
                else { return Err(DbError::ParseError("Expected '=' after '!'".into())); }
            }
            '<' => {
                chars.next();
                if chars.peek() == Some(&'=') { chars.next(); tokens.push(Token::Le); }
                else { tokens.push(Token::Lt); }
            }
            '>' => {
                chars.next();
                if chars.peek() == Some(&'=') { chars.next(); tokens.push(Token::Ge); }
                else { tokens.push(Token::Gt); }
            }
            '\''|'"' => {
                let q = ch; chars.next();
                let mut s = String::new();
                loop {
                    match chars.next() {
                        Some(c) if c == q => break,
                        Some(c) => s.push(c),
                        None => return Err(DbError::ParseError("Unterminated string".into())),
                    }
                }
                tokens.push(Token::StrLit(s));
            }
            c if c.is_ascii_digit() || (c == '-' && matches!(chars.clone().nth(1), Some(d) if d.is_ascii_digit())) => {
                let mut num = String::new();
                if c == '-' { num.push(c); chars.next(); }
                let mut is_float = false;
                while let Some(&d) = chars.peek() {
                    if d.is_ascii_digit()         { num.push(d); chars.next(); }
                    else if d == '.' && !is_float { is_float = true; num.push(d); chars.next(); }
                    else                          { break; }
                }
                if is_float {
                    tokens.push(Token::FloatLit(num.parse().map_err(|_| DbError::ParseError(format!("Bad float: {}", num)))?));
                } else {
                    tokens.push(Token::IntLit(num.parse().map_err(|_| DbError::ParseError(format!("Bad int: {}", num)))?));
                }
            }
            c if c.is_alphabetic() || c == '_' => {
                let mut word = String::new();
                while let Some(&d) = chars.peek() {
                    if d.is_alphanumeric() || d == '_' { word.push(d); chars.next(); }
                    else { break; }
                }
                let upper = word.to_uppercase();
                let tok = match upper.as_str() {
                    "TRUE"  => Token::BoolLit(true),
                    "FALSE" => Token::BoolLit(false),
                    "NULL"  => Token::Null,
                    kw if KEYWORDS.contains(&kw) => Token::Keyword(kw.to_string()),
                    _       => Token::Ident(word),
                };
                tokens.push(tok);
            }
            other => return Err(DbError::ParseError(format!("Unexpected char: '{}'", other))),
        }
    }
    tokens.push(Token::Eof);
    Ok(tokens)
}

// ─── Parser ───────────────────────────────────────────────────────────────────

struct Parser { tokens: Vec<Token>, pos: usize }

impl Parser {
    fn new(tokens: Vec<Token>) -> Self { Parser { tokens, pos: 0 } }

    fn peek(&self) -> &Token { &self.tokens[self.pos] }

    fn advance(&mut self) -> Token {
        let tok = self.tokens[self.pos].clone();
        if self.pos + 1 < self.tokens.len() { self.pos += 1; }
        tok
    }

    fn expect_keyword(&mut self, kw: &str) -> DbResult<()> {
        match self.advance() {
            Token::Keyword(k) if k == kw => Ok(()),
            other => Err(DbError::ParseError(format!("Expected '{}', got {:?}", kw, other))),
        }
    }

    fn expect_ident(&mut self) -> DbResult<String> {
        match self.advance() {
            Token::Ident(s)   => Ok(s),
            Token::Keyword(k) => Ok(k.to_lowercase()),
            other => Err(DbError::ParseError(format!("Expected identifier, got {:?}", other))),
        }
    }

    fn expect_token(&mut self, expected: &Token) -> DbResult<()> {
        let tok = self.advance();
        if &tok == expected { Ok(()) }
        else { Err(DbError::ParseError(format!("Expected {:?}, got {:?}", expected, tok))) }
    }

    fn parse_type(&mut self) -> DbResult<DataType> {
        match self.advance() {
            Token::Keyword(k) => match k.as_str() {
                "INTEGER"        => Ok(DataType::Integer),
                "FLOAT"          => Ok(DataType::Float),
                "TEXT"           => Ok(DataType::Text),
                "BOOLEAN"|"BOOL" => Ok(DataType::Boolean),
                other => Err(DbError::ParseError(format!("Unknown type '{}'", other))),
            },
            other => Err(DbError::ParseError(format!("Expected type, got {:?}", other))),
        }
    }

    fn parse_value(&mut self) -> DbResult<Value> {
        match self.advance() {
            Token::IntLit(i)  => Ok(Value::Integer(i)),
            Token::FloatLit(f)=> Ok(Value::Float(f)),
            Token::StrLit(s)  => Ok(Value::Text(s)),
            Token::BoolLit(b) => Ok(Value::Boolean(b)),
            Token::Null       => Ok(Value::Null),
            other => Err(DbError::ParseError(format!("Expected value, got {:?}", other))),
        }
    }

    fn parse_op(&mut self) -> DbResult<Operator> {
        match self.advance() {
            Token::Eq => Ok(Operator::Eq), Token::Ne => Ok(Operator::Ne),
            Token::Lt => Ok(Operator::Lt), Token::Le => Ok(Operator::Le),
            Token::Gt => Ok(Operator::Gt), Token::Ge => Ok(Operator::Ge),
            other => Err(DbError::ParseError(format!("Expected operator, got {:?}", other))),
        }
    }

    fn parse_condition(&mut self) -> DbResult<Condition> {
        let column   = self.expect_ident()?;
        let operator = self.parse_op()?;
        let value    = self.parse_value()?;
        Ok(Condition { column, operator, value })
    }

    fn maybe_where(&mut self) -> DbResult<Option<Condition>> {
        if matches!(self.peek(), Token::Keyword(k) if k == "WHERE") {
            self.advance();
            Ok(Some(self.parse_condition()?))
        } else { Ok(None) }
    }

    // ─── Statement parsers ────────────────────────────────────────────────

    fn parse_create_table(&mut self) -> DbResult<Statement> {
        self.expect_keyword("TABLE")?;
        let table = self.expect_ident()?;
        self.expect_token(&Token::LParen)?;
        let mut columns = Vec::new();
        loop {
            let name      = self.expect_ident()?;
            let data_type = self.parse_type()?;
            let nullable  = if matches!(self.peek(), Token::Keyword(k) if k == "NOT") {
                self.advance(); self.expect_keyword("NULL")?; false
            } else { true };
            columns.push(ColumnDef { name, data_type, nullable });
            match self.peek() {
                Token::Comma  => { self.advance(); }
                Token::RParen => { self.advance(); break; }
                other => return Err(DbError::ParseError(format!("Expected ',' or ')', got {:?}", other))),
            }
        }
        Ok(Statement::CreateTable { table, columns })
    }

    fn parse_insert(&mut self) -> DbResult<Statement> {
        self.expect_keyword("INTO")?;
        let table = self.expect_ident()?;
        let columns = if matches!(self.peek(), Token::LParen) {
            self.advance();
            let mut cols = Vec::new();
            loop {
                cols.push(self.expect_ident()?);
                match self.peek() {
                    Token::Comma  => { self.advance(); }
                    Token::RParen => { self.advance(); break; }
                    other => return Err(DbError::ParseError(format!("Expected ',' or ')', got {:?}", other))),
                }
            }
            Some(cols)
        } else { None };
        self.expect_keyword("VALUES")?;
        self.expect_token(&Token::LParen)?;
        let mut values = Vec::new();
        loop {
            values.push(self.parse_value()?);
            match self.peek() {
                Token::Comma  => { self.advance(); }
                Token::RParen => { self.advance(); break; }
                other => return Err(DbError::ParseError(format!("Expected ',' or ')', got {:?}", other))),
            }
        }
        Ok(Statement::Insert { table, columns, values })
    }

    fn parse_select(&mut self) -> DbResult<Statement> {
        let columns = if matches!(self.peek(), Token::Star) {
            self.advance(); vec!["*".to_string()]
        } else {
            let mut cols = Vec::new();
            loop {
                cols.push(self.expect_ident()?);
                if matches!(self.peek(), Token::Comma) { self.advance(); } else { break; }
            }
            cols
        };
        self.expect_keyword("FROM")?;
        let table     = self.expect_ident()?;
        let condition = self.maybe_where()?;
        Ok(Statement::Select { table, columns, condition })
    }

    fn parse_update(&mut self) -> DbResult<Statement> {
        let table = self.expect_ident()?;
        self.expect_keyword("SET")?;
        let mut assignments = Vec::new();
        loop {
            let col = self.expect_ident()?;
            self.expect_token(&Token::Eq)?;
            let val = self.parse_value()?;
            assignments.push((col, val));
            if matches!(self.peek(), Token::Comma) { self.advance(); } else { break; }
        }
        let condition = self.maybe_where()?;
        Ok(Statement::Update { table, assignments, condition })
    }

    fn parse_delete(&mut self) -> DbResult<Statement> {
        self.expect_keyword("FROM")?;
        let table     = self.expect_ident()?;
        let condition = self.maybe_where()?;
        Ok(Statement::Delete { table, condition })
    }

    fn parse_statement(&mut self) -> DbResult<Statement> {
        match self.advance() {
            Token::Keyword(k) => match k.as_str() {
                "CREATE"          => self.parse_create_table(),
                "DROP"            => { self.expect_keyword("TABLE")?; Ok(Statement::DropTable { table: self.expect_ident()? }) }
                "INSERT"          => self.parse_insert(),
                "SELECT"          => self.parse_select(),
                "UPDATE"          => self.parse_update(),
                "DELETE"          => self.parse_delete(),
                "SHOW"            => { self.expect_keyword("TABLES")?; Ok(Statement::ShowTables) }
                "DESCRIBE"|"DESC" => Ok(Statement::Describe(self.expect_ident()?)),
                "PAGES"           => Ok(Statement::PageStats(self.expect_ident()?)),
                other => Err(DbError::ParseError(format!("Unknown statement: '{}'", other))),
            },
            Token::Eof => Err(DbError::ParseError("Empty input".into())),
            other => Err(DbError::ParseError(format!("Unexpected token: {:?}", other))),
        }
    }
}

pub fn parse(input: &str) -> DbResult<Statement> {
    let input  = input.trim().trim_end_matches(';');
    let tokens = tokenize(input)?;
    Parser::new(tokens).parse_statement()
}
