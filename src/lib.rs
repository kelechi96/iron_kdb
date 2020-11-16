pub mod codec;

use std::net::TcpStream;
use std::net::ToSocketAddrs;
use std::io::{Write, Read};
use crate::codec::Payload;

pub struct KdbConnection {
    tcp_connection: TcpStream
}

impl KdbConnection {
    pub fn new<T: ToSocketAddrs>(address: T) -> std::io::Result<KdbConnection> {
        TcpStream::connect(address).map(|x| { KdbConnection { tcp_connection: x } })
    }

    /// Sends handshake byte
    /// TODO: ASCII STRING NOT UTF8
    pub fn connect(&mut self, user: &str, pwd: &str) -> std::io::Result<()> {
        let mut user_pass = String::from(user);
        user_pass += ":";
        user_pass += pwd;
        user_pass.push(3 as char);
        user_pass.push(0 as char);
        self.tcp_connection.write_all(user_pass.as_bytes())?;
        let mut buf = [0u8; 1];
        self.tcp_connection.read_exact(&mut buf)?;
        Ok(())
    }

    pub fn query(&mut self, msg: codec::KdbRequest) -> Result<Payload, String> {
        let vec: Vec<u8> = msg.to_bytes();

        println!("Sent: {:?}", hex::encode(vec.clone()));
        self.tcp_connection.write_all(vec.as_slice()).map_err(|x| x.to_string())?;
        self.receive()
    }

    fn receive(&mut self) -> std::result::Result<Payload, String> {
        let mut header = [0u8; 8];
        self.tcp_connection.read_exact(&mut header).map_err(|x| x.to_string())?;
        let mut msg_size_array = [0u8; 4];
        msg_size_array.clone_from_slice(&header[4..8]);
        let msg_size: u32 = u32::from_le_bytes(msg_size_array);
        let mut buf = Vec::with_capacity((msg_size - 8) as usize);

        buf.extend_from_slice(&header);

        std::io::Read::by_ref(&mut self.tcp_connection).take((msg_size - 8) as u64).read_to_end(&mut buf).map_err(|x| x.to_string())?;

        println!("Received: {:?}", hex::encode(buf.clone()));
        let start = std::time::Instant::now();
        let ret_val = Ok(Payload::from_bytes(&buf.as_slice()[8..])?);
        println!("{:?}", std::time::Instant::now() - start);
        ret_val
    }
}


#[cfg(test)]
mod tests {
    use crate::KdbConnection;
    use crate::codec::{KdbRequest, Payload};

    #[test]
    fn local_integration_test() {
        let mut connection = KdbConnection::new("127.0.0.1:1550").unwrap();
        connection.connect("", "").unwrap();

        if let Payload::CharVector(_, vec) = connection.query(KdbRequest::new("\"ABC\"").unwrap()).unwrap() {
            assert_eq!("ABC", vec);
        }

        if let Payload::Long(val) = connection.query(KdbRequest::new("1+5").unwrap()).unwrap() {
            assert_eq!(6, val);
        }
    }
}
