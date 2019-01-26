#![allow(unused_imports)]

mod file;
mod query;
mod server;

use crate::server::SunsetServer;

fn main() {
    let server = match SunsetServer::new("127.0.0.1:2600", "sunset.db") {
        Ok(server) => server,
        Err(e) => {
            eprintln!("Error starting sunset server: {}", e);
            return ()
        },
    };
    match server.start() {
        Ok(_) => (),
        Err(e) => eprintln!("Error starting sunset server: {}", e),
    }
}
