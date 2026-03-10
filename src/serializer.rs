// ─────────────────────────────────────────────────────────────────────────────
//  serializer.rs  —  Value ↔ binary bytes
// ─────────────────────────────────────────────────────────────────────────────
//
//  Wire format per value:
//    [1 byte]  null_flag   0x00 = value present | 0x01 = NULL
//    [N bytes] payload     (absent when null_flag == 0x01)
//
//  Payload encoding:
//    INTEGER  → 8 bytes big-endian i64
//    FLOAT    → 8 bytes big-endian u64 (f64 bit pattern via to_bits())
//    BOOLEAN  → 1 byte  (0x01=true, 0x00=false)
//    TEXT     → 4 bytes u32 length (big-endian) + that many UTF-8 bytes
//
//  A row is all its values serialized back-to-back with no separator.
//  Deserialization uses a cursor integer that advances through the byte slice.

use crate::types::{DataType, Value};

// ─── Serialize ────────────────────────────────────────────────────────────────

pub fn serialize_value(value: &Value, buf: &mut Vec<u8>) {
    match value {
        Value::Null => {
            buf.push(0x01); // null flag — no payload
        }
        Value::Integer(i) => {
            buf.push(0x00);
            buf.extend_from_slice(&i.to_be_bytes());
        }
        Value::Float(f) => {
            buf.push(0x00);
            buf.extend_from_slice(&f.to_bits().to_be_bytes());
        }
        Value::Boolean(b) => {
            buf.push(0x00);
            buf.push(if *b { 0x01 } else { 0x00 });
        }
        Value::Text(s) => {
            buf.push(0x00);
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes()); // 4-byte length
            buf.extend_from_slice(bytes);
        }
    }
}

pub fn serialize_row(row: &[Value]) -> Vec<u8> {
    let mut buf = Vec::new();
    for v in row { serialize_value(v, &mut buf); }
    buf
}

// ─── Deserialize ─────────────────────────────────────────────────────────────

pub fn deserialize_value(bytes: &[u8], cursor: &mut usize, dtype: &DataType) -> Value {
    let null_flag = bytes[*cursor];
    *cursor += 1;

    if null_flag == 0x01 {
        return Value::Null;
    }

    match dtype {
        DataType::Integer => {
            let arr: [u8; 8] = bytes[*cursor..*cursor + 8].try_into().unwrap();
            *cursor += 8;
            Value::Integer(i64::from_be_bytes(arr))
        }
        DataType::Float => {
            let arr: [u8; 8] = bytes[*cursor..*cursor + 8].try_into().unwrap();
            *cursor += 8;
            Value::Float(f64::from_bits(u64::from_be_bytes(arr)))
        }
        DataType::Boolean => {
            let b = bytes[*cursor] == 0x01;
            *cursor += 1;
            Value::Boolean(b)
        }
        DataType::Text => {
            let len_arr: [u8; 4] = bytes[*cursor..*cursor + 4].try_into().unwrap();
            let len = u32::from_be_bytes(len_arr) as usize;
            *cursor += 4;
            let s = std::str::from_utf8(&bytes[*cursor..*cursor + len]).unwrap().to_string();
            *cursor += len;
            Value::Text(s)
        }
    }
}

pub fn deserialize_row(bytes: &[u8], schema: &[DataType]) -> Vec<Value> {
    let mut cursor = 0;
    schema.iter().map(|dt| deserialize_value(bytes, &mut cursor, dt)).collect()
}
