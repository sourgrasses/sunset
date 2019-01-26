use crate::query::{Query, QueryType};

use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender};
use memmap::Mmap;

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::str;

#[derive(Debug)]
enum Error {
    KeyNotFound,
    InsertFailed,
    Io(io::Error),
    Other(String),
}

#[derive(Debug)]
pub struct SunsetDb {
    filename: String,
    file: File,
    map: Mmap,
    pos: usize,
    tx: Sender<Bytes>,
    rx: Receiver<Query>,
}

impl SunsetDb {
    pub fn new(filename: String, tx: Sender<Bytes>, rx: Receiver<Query>) -> Result<SunsetDb, io::Error> {
        let file = match OpenOptions::new().read(true).write(true).open(&filename) {
            Ok(f) => f,
            Err(e) => return Err(e),
        };
        let map = unsafe {
            match Mmap::map(&file) {
                Ok(map) => map,
                Err(e) => return Err(e),
            }
        };

        Ok(SunsetDb {
            filename: filename,
            file: file,
            map: map,
            pos: 9,
            tx: tx,
            rx: rx,
        })
    }

    pub fn serve(&mut self) {
        loop {
            let query = match self.rx.recv() {
                Ok(query) => query,
                Err(e) => {
                    eprintln!("{}", e);
                    continue
                },
            };

            let qres = match query.qtype {
                QueryType::Get => {
                    match self.get(&query.key) {
                        Ok(value) => value,
                        Err(e) => {
                            eprintln!("{}", e);
                            continue
                        },
                    }
                },
                QueryType::Put => {
                    let value = match query.value {
                        Some(value) => value,
                        None => {
                            eprintln!("Error: query missing value");
                            continue
                        },
                    };
                    match self.put(&query.key, &value) {
                        Ok(_) => continue,
                        Err(e) => {
                            eprintln!("{}", e);
                            continue
                        },
                    };
                },
                QueryType::Del => {
                    match self.del(&query.key) {
                        Ok(_) => continue,
                        Err(e) => {
                            eprintln!("{}", e);
                            continue
                        },
                    };
                },
            };

            self.pos = 9;

            match self.tx.send(qres) {
                Ok(_) => (),
                Err(e) => eprintln!("{}", e),
            };
        }
    }

    fn get(&mut self, key: &Bytes) -> Result<Bytes, Error> {
        loop {
            // check to see if we've reached the end of the file, and if we have
            // return an error saying we were not able to find the key
            if self.pos + 9 >= self.map.len() {
                self.pos = 9;
                return Err(Error::KeyNotFound);
            }

            let mut len_buf = [0u8; 4];
            len_buf.copy_from_slice(&self.map[self.pos..self.pos + 4]);
            let key_len = u32::from_le_bytes(len_buf);
            len_buf.copy_from_slice(&self.map[self.pos + 4..self.pos + 8]);
            let value_len = u32::from_le_bytes(len_buf);

            self.pos += 8;

            if Bytes::from(&self.map[self.pos..self.pos + key_len as usize]) == *key {
                self.pos += key_len as usize;
                return Ok(Bytes::from(&self.map[self.pos..self.pos + value_len as usize]));
            }

            self.pos += key_len as usize + value_len as usize;
        }
    }

    fn put(&mut self, key: &Bytes, value: &Bytes) -> Result<(), Error> {
        // advance the seek pointer to the current end of the file
        match self.file.seek(SeekFrom::End(0)) {
            Ok(_) => (),
            Err(e) => return Err(Error::Io(e)),
        };

        let key_len = key.len() as u32;
        self.file.write_all(&key_len.to_le_bytes()).unwrap();
        let value_len = value.len() as u32;
        self.file.write_all(&value_len.to_le_bytes()).unwrap();

        self.file.write_all(&key).unwrap();
        self.file.write_all(&value).unwrap();

        Ok(())
    }

    fn del(&mut self, key: &Bytes) -> Result<(), Error> {
        Ok(())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            Error::KeyNotFound => write!(f, "Key not found in database"),
            Error::InsertFailed => write!(f, "Failed to insert value into database"),
            Error::Io(ref e) => write!(f, "IO error: {}", e),
            Error::Other(ref e) => write!(f, "Other error: {}", e),
        }
    }
}
