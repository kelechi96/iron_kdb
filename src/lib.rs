pub mod codec;

use std::net::TcpStream;
use std::net::ToSocketAddrs;
use std::io::{Write, Read};
use crate::codec::Payload;
use ascii::IntoAsciiString;
use std::convert::TryInto;

pub struct KdbConnection {
    tcp_connection: TcpStream
}

impl KdbConnection {
    pub fn new<T: ToSocketAddrs>(address: T) -> std::io::Result<KdbConnection> {
        TcpStream::connect(address).map(|x| { KdbConnection { tcp_connection: x } })
    }

    /// Sends handshake byte
    pub fn connect(&mut self, user: &str, pwd: &str) -> std::io::Result<()> {
        let mut user_pass = format!("{}:{}", user, pwd);
        user_pass.push(3 as char);
        user_pass.push(0 as char);
        self.tcp_connection.write_all(user_pass.into_ascii_string().unwrap().as_bytes())?;
        let mut buf = [0u8; 1];
        self.tcp_connection.read_exact(&mut buf)?;
        Ok(())
    }

    pub fn query(&mut self, msg: codec::KdbRequest) -> Result<Payload, String> {
        let vec: Vec<u8> = msg.to_bytes();

        //println!("Sent: {:?}", hex::encode(vec.clone()));
        self.tcp_connection.write_all(vec.as_slice()).map_err(|x| x.to_string())?;
        self.receive()
    }

    fn receive(&mut self) -> std::result::Result<Payload, String> {
        let mut header = [0u8; 8];
        self.tcp_connection.read_exact(&mut header).map_err(|x| x.to_string())?;
        let mut msg_size_array = [0u8; 4];
        msg_size_array.clone_from_slice(&header[4..8]);
        let msg_size: u32 = u32::from_le_bytes(msg_size_array);
        let mut buf = Vec::with_capacity(msg_size as usize);
        // Alignment - Potential performance improvement at the cost of perhaps portability,
        // and having to deal with endianness - easy optimisation if both source and target are the same
        // endianness
        // buf.extend_from_slice(&[0;10]);


        buf.extend_from_slice(&header);

        std::io::Read::by_ref(&mut self.tcp_connection).take((msg_size - 8) as u64).read_to_end(&mut buf).map_err(|x| x.to_string())?;

        if header[2] == 1 {
            let uncompressed = uncompress(&buf[8..])?;
            buf = Vec::with_capacity(uncompressed.len());
            buf.extend_from_slice(&header);
            buf.extend_from_slice(&uncompressed[8..]);
        }

        //println!("Received: {:?}", hex::encode(buf.clone()));
        let start = std::time::Instant::now();
        let ret_val = Ok(Payload::from_bytes(&buf.as_slice()[8..])?);
        println!("{:?}", std::time::Instant::now() - start);
        ret_val
    }
}

pub fn uncompress(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let mut n = 0;
    let mut r ;
    let mut f = 0;
    let mut s = 8;
    let mut p = 8;
    let mut i = 0;
    let result_size = bytes[0..4].try_into().map(u32::from_le_bytes).map_err(|x| x.to_string())?;
    let mut d = 4;
    let mut dst = vec![0u8; result_size as usize];
    let mut aa = [0u32; 256];
    while s < result_size {
        if i == 0 {
            f = 0xff & (bytes[d] as u32);
            d += 1;
            i = 1;
        }
        if (f & i) != 0 {
            r = aa[(0xff & (bytes[d] as u32)) as usize];
            d += 1;
            dst[s as usize] = dst[r as usize];
            s += 1;
            r += 1;
            dst[s as usize] = dst[r as usize];
            s += 1;
            r += 1;
            n = 0xff & (bytes[d] as u32);
            d += 1;
            for m in 0..n {
                dst[(s + m) as usize] = dst[(r + m) as usize];
            }
        } else {
            dst[s as usize] = bytes[d as usize];
            s += 1;
            d += 1;
        }
        while p < (s - 1) {
            aa[((0xff & (dst[p as usize] as u32)) ^ (0xff & (dst[(p + 1) as usize] as u32))) as usize] = p as u32;
            p += 1;
        }
        if (f & i) != 0 {
            s += n;
            p = s;
        }
        i *= 2;
        if i == 256 {
            i = 0;
        }
    }
    Ok(dst)
}

#[cfg(test)]
mod tests {
    use crate::uncompress;
    use crate::codec::Payload;
    use crate::codec::Payload::LongVector;
    use crate::codec::VectorAttribute::NoAttribute;
    use std::ops::Range;

    #[test]
    pub fn test_uncompress() {
        let a = hex::decode("ae0f0000c00700f401000000060106aa0200050300050400050500052e0600050700000408000400095500050a00050b00050c00050d5500050e00050f00051000051155000512000513000514000515550005160005170005180005195500051a00051b00051c00051d5500051e00051f00052000052155000522000523000524000525550005260005270005280005295500052a00052b00052c00052d5500052e00052f00053000053155000532000533000534000535550005360005370005380005395500053a00053b00053c00053d5500053e00053f00054000054155000542000543000544000545550005460005470005480005495500054a00054b00054c00054d5500054e00054f00055000055155000552000553000554000555550005560005570005580005595500055a00055b00055c00055d5500055e00055f00056000056155000562000563000564000565550005660005670005680005695500056a00056b00056c00056d5500056e00056f00057000057155000572000573000574000575550005760005770005780005795500057a00057b00057c00057d5500057e00057f00058000058155000582000583000584000585550005860005870005880005895500058a00058b00058c00058d5500058e00058f00059000059155000592000593000594000595550005960005970005980005995500059a00059b00059c00059d5500059e00059f0005a00005a1550005a20005a30005a40005a5550005a60005a70005a80005a9550005aa0005ab0005ac0005ad550005ae0005af0005b00005b1550005b20005b30005b40005b5550005b60005b70005b80005b9550005ba0005bb0005bc0005bd550005be0005bf0005c00005c1550005c20005c30005c40005c5550005c60005c70005c80005c9550005ca0005cb0005cc0005cd550005ce0005cf0005d00005d1550005d20005d30005d40005d5550005d60005d70005d80005d9550005da0005db0005dc0005dd550005de0005df0005e00005e1550005e20005e30005e40005e5550005e60005e70005e80005e9550005ea0005eb0005ec0005ed550005ee0005ef0005f00005f1550005f20005f30005f40005f5550005f60005f70005f80005f9550005fa0005fb0005fc0005fd550005fe0005ff00050001050155010502010503010504010505550105060105070105080105095501050a01050b01050c01050d5501050e01050f01051001051155010512010513010514010515550105160105170105180105195501051a01051b01051c01051d5501051e01051f01052001052155010522010523010524010525550105260105270105280105295501052a01052b01052c01052d5501052e01052f01053001053155010532010533010534010535550105360105370105380105395501053a01053b01053c01053d5501053e01053f01054001054155010542010543010544010545550105460105470105480105495501054a01054b01054c01054d5501054e01054f01055001055155010552010553010554010555550105560105570105580105595501055a01055b01055c01055d5501055e01055f01056001056155010562010563010564010565550105660105670105680105695501056a01056b01056c01056d5501056e01056f01057001057155010572010573010574010575550105760105770105780105795501057a01057b01057c01057d5501057e01057f01058001058155010582010583010584010585550105860105870105880105895501058a01058b01058c01058d5501058e01058f01059001059155010592010593010594010595550105960105970105980105995501059a01059b01059c01059d5501059e01059f0105a00105a1550105a20105a30105a40105a5550105a60105a70105a80105a9550105aa0105ab0105ac0105ad550105ae0105af0105b00105b1550105b20105b30105b40105b5550105b60105b70105b80105b9550105ba0105bb0105bc0105bd550105be0105bf0105c00105c1550105c20105c30105c40105c5550105c60105c70105c80105c9550105ca0105cb0105cc0105cd550105ce0105cf0105d00105d1550105d20105d30105d40105d5550105d60105d70105d80105d9550105da0105db0105dc0105dd550105de0105df0105e00105e1550105e20105e30105e40105e5550105e60105e70105e80105e9550105ea0105eb0105ec0105ed550105ee0105ef0105f00105f1150105f20105f30105").unwrap();
        assert_eq!(Payload::from_bytes(&uncompress(&a).unwrap()[8..]).unwrap(), LongVector(NoAttribute,Range::from(0..500).into_iter().collect()))
    }
}
