use crate::file::SunsetDb;
use crate::query::{Query, QueryCodec, QueryType};

use bytes::{Bytes, BytesMut};

use crossbeam_channel::{Receiver, Sender};

use tokio::prelude::*;
use tokio::codec::Framed;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};

use std::net::SocketAddr;
use std::thread;

//#[derive(Debug)]
pub struct SunsetServer {
    addr: SocketAddr,
    filename: String,
}

impl SunsetServer {
    pub fn new(addr: &str, filename: &str) -> Result<SunsetServer, String> {
        let addr = match addr.parse() {
            Ok(addr) => addr,
            Err(e) => return Err(format!("{}", e)),
        };

        Ok(SunsetServer {
            addr: addr,
            filename: filename.to_owned(),
        })
    }

    pub fn start(&self) -> Result<(), String> {
        let (file_tx, rx) = crossbeam_channel::unbounded::<Bytes>();
        let (tx, file_rx) = crossbeam_channel::unbounded::<Query>();
        let mut file = match SunsetDb::new(self.filename.clone(), file_tx, file_rx) {
            Ok(file) => file,
            Err(e) => return Err(format!("{}", e)),
        };

        thread::spawn(move || file.serve());

        let tcp = TcpListener::bind(&self.addr).unwrap();
        let server = tcp.incoming()
            .for_each(move |mut sock| {
                let txc = tx.clone();
                let rxc = rx.clone();
                let fsock = Framed::new(sock, QueryCodec::new())
                    .for_each(move |query| {
                        let get = query.qtype == QueryType::Get;
                        match txc.send(query) {
                            Ok(_) => (),
                            Err(e) => eprintln!("{}", e),
                        };
                        let mut res = Vec::new();
                        if get {
                            match rxc.recv() {
                                Ok(rec) => res = rec.to_vec(),
                                Err(e) => eprintln!("{}", e),
                            };
                            //sock.write_all(&res);
                            println!("received: {}", std::str::from_utf8(&res).unwrap());
                        }

                        Ok(())
                    })
                    .map_err(|err| { eprintln!("{}", err) });

                tokio::spawn(fsock);

                Ok(())
            })
            .map_err(|err| { eprintln!("{}", err) });

        tokio::run(server);
        Ok(())
    }
}
