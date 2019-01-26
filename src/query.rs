use bytes::{BufMut, Bytes, BytesMut};
use tokio::codec::{Decoder, Encoder};

use std::fmt;
use std::io;
use std::str;

#[derive(Clone, Debug, PartialEq)]
pub enum QueryType {
    Get,
    Put,
    Del,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    pub(crate) qtype: QueryType,
    pub(crate) key: Bytes,
    pub(crate) value: Option<Bytes>,
}

#[derive(Clone, Default, PartialEq)]
pub struct QueryCodec {
    pos: usize
}

impl Query {
    pub fn new(qtype: QueryType, key: Bytes, value: Option<Bytes>) -> Query {
        Query {
            qtype: qtype,
            key: key,
            value: value,
        }
    }

    pub fn from_buffer(buf: BytesMut) -> Result<Query, io::Error> {
        let qvec = buf.splitn(3, |c| *c == 32 as u8).collect::<Vec<&[u8]>>();

        let qtype = match qvec[0] {
            b"[get]" => QueryType::Get,
            b"[put]" => QueryType::Put,
            b"[del]" => QueryType::Del,
            _ => return Err(io::Error::new(io::ErrorKind::InvalidInput, "Error parsing query")),
        };
        let mut key = BytesMut::from(qvec[1]);
        let value = if qvec.len() == 3 {
            // chop off the \r\n we get from the QueryCodec implementation
            let value = qvec[2].split(|c| *c == 13 as u8).take(1).collect::<Vec<&[u8]>>();
            Some(Bytes::from(value[0]))
        } else {
            None
        };

        if value == None { key.truncate(key.len() - 2); }

        Ok(Query {
            qtype: qtype,
            key: Bytes::from(key),
            value: value,
        })
    }

    pub fn len(&self) -> usize {
        let v_len = match self.value {
            Some(ref v) => v.len() + 1,
            None => 1 as usize,
        };

        5 + self.key.len() + 1 + v_len
    }

    pub fn to_string(&self) -> Result<String, str::Utf8Error> {
        let key = str::from_utf8(&self.key)?;
        match self.value {
            Some(ref value) => {
                let value = str::from_utf8(value)?;
                Ok(format!("{} {} {}", self.qtype, key, value))
            },
            None => Ok(format!("{} {}", self.qtype, key)),
        }
    }
}

impl fmt::Display for QueryType {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            QueryType::Get => write!(f, "[get]"),
            QueryType::Put => write!(f, "[put]"),
            QueryType::Del => write!(f, "[del]"),
        }
    }
}

impl QueryCodec {
    pub fn new() -> QueryCodec {
        QueryCodec { pos: 0 }
    }
}

impl Decoder for QueryCodec {
    type Item = Query;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let endline = buf[self.pos..].windows(2).position(|chars| chars == b"\r\n");
        let q_bytes = match endline {
            Some(offset) => {
                let index = offset + self.pos;
                let mut proto_query = buf.split_to(index + 2);
                let _ = proto_query.split_off(index + 2);
                self.pos = 0;

                proto_query
            },
            None => {
                self.pos = buf.len();
                return Ok(None)
            },
        };

        match Query::from_buffer(q_bytes) {
            Ok(q) => Ok(Some(q)),
            Err(e) => Err(e),
        }
    }
}

impl Encoder for QueryCodec {
    type Item = Query;
    type Error = io::Error;

    fn encode(&mut self, query: Query, buf: &mut BytesMut) -> Result<(), Self::Error> {
        buf.reserve(query.len() + 2);
        let squery = match query.to_string() {
            Ok(query) => query,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, format!("UTF8 error encoding query: {}", e))),
        };
        buf.put(squery);
        buf.put_slice(b"\r\n");

        Ok(())
    }
}
