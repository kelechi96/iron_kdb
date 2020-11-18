use std::convert::{TryInto, TryFrom};
use ascii::{AsciiStr, AsAsciiStrError, AsciiString};
use std::array::TryFromSliceError;
use crate::codec::VectorAttribute::{Sorted, Unique, Grouped, NoAttribute};

const HEADER_LEN: u32 = 8;
const TYPE_LEN: u32 = 1;
const ATTRIBUTE_LEN: u32 = 1;
const VECTOR_LEN: u32 = 4;
const PADDING_BYTES: [u8; 2] = [0, 0];

#[derive(Debug, Eq, PartialEq)]
pub enum VectorAttribute {
    NoAttribute = 0,
    Sorted = 1,
    Unique = 2,
    Grouped = 3,
}

impl TryFrom<u8> for VectorAttribute {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(NoAttribute),
            1 => Ok(Sorted),
            2 => Ok(Unique),
            3 => Ok(Grouped),
            _ => Err(format!("Value {} out of range.", value))
        }
    }
}

#[derive(Copy, Clone)]
enum Architecture {
    //TODO: BigEndian = 0,
    LittleEndian = 1,
}

#[derive(Copy, Clone)]
enum SynchronisationType {
    //TODO: Async = 0,
    Sync = 1,
    //Response = 2,
}

pub struct KdbRequest<'a> {
    /// Byte 0
    architecture: Architecture,
    /// Byte 1
    synchronisation_type: SynchronisationType,

    request: &'a AsciiStr,
}


impl<'a> KdbRequest<'a> {
    pub fn new(request: &'a str) -> Result<KdbRequest, AsAsciiStrError> {
        Ok(KdbRequest {
            architecture: Architecture::LittleEndian,
            synchronisation_type: SynchronisationType::Sync,
            request: AsciiStr::from_ascii(request)?,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut ret_val = vec![0; (HEADER_LEN + TYPE_LEN + ATTRIBUTE_LEN + VECTOR_LEN + self.request.len() as u32) as usize];
        ret_val[0] = self.architecture as u8;
        ret_val[1] = self.synchronisation_type as u8;
        ret_val[2..4].copy_from_slice(&PADDING_BYTES);
        ret_val[4..8].copy_from_slice(&(HEADER_LEN + TYPE_LEN + ATTRIBUTE_LEN + VECTOR_LEN + self.request.len() as u32).to_le_bytes());
        ret_val[8] = 10;
        ret_val[9] = 0;
        ret_val[10..14].copy_from_slice(&(self.request.as_bytes().len() as u32).to_le_bytes());
        ret_val[14..].copy_from_slice(self.request.as_bytes());
        ret_val
    }
}

#[derive(Debug, PartialEq)]
pub enum Payload {
    List(VectorAttribute, Vec<Payload>),
    Bool(bool),
    BoolVector(VectorAttribute, Vec<bool>),
    GUID(u128),
    GUIDVector(VectorAttribute, Vec<u128>),
    Byte(u8),
    ByteVector(VectorAttribute, Vec<u8>),
    Short(u16),
    ShortVector(VectorAttribute, Vec<u16>),
    Int(u32),
    IntVector(VectorAttribute, Vec<u32>),
    Long(u64),
    LongVector(VectorAttribute, Vec<u64>),
    Real(f32),
    RealVector(VectorAttribute, Vec<f32>),
    Float(f64),
    FloatVector(VectorAttribute, Vec<f64>),
    Char(char),
    CharVector(VectorAttribute, AsciiString),
    Symbol(AsciiString),
    SymbolVector(VectorAttribute, Vec<AsciiString>),
    Error(AsciiString),
    Timestamp(u64),
    TimestampVector(VectorAttribute, Vec<u64>),
    Month(u32),
    MonthVector(VectorAttribute, Vec<u32>),
    Date(u32),
    DateVector(VectorAttribute, Vec<u32>),
    DateTime(u64),
    DateTimeVector(VectorAttribute, Vec<u64>),
    TimeSpan(u64),
    TimeSpanVector(VectorAttribute, Vec<u64>),
    Minute(u32),
    MinuteVector(VectorAttribute, Vec<u32>),
    Second(u32),
    SecondVector(VectorAttribute, Vec<u32>),
    Time(u32),
    TimeVector(VectorAttribute, Vec<u32>),
    Table(VectorAttribute, Box<Payload>),
    Dictionary(Box<Payload>, Box<Payload>),
    Nil,
    NilVector(VectorAttribute, Vec<()>),
}

impl Payload {
    #[inline]
    pub fn from_bytes(bytes: &[u8]) -> Result<Payload, String> {
        let type_byte = bytes[0] as i8;

        match type_byte {
            0 => {
                let list_len = Self::get_vec_size(&bytes[2..6])?;
                let mut list_contents = Vec::with_capacity(Self::get_vec_size(&bytes[2..6])?);
                let mut index = 6;
                for _ in 0..list_len {
                    let sub_payload = Payload::from_bytes(&bytes[index..]).unwrap();
                    index += sub_payload.get_size() + 1;
                    list_contents.push(sub_payload);
                }
                Ok(Payload::List(bytes[1].try_into()?, list_contents))
            }
            -1 => if bytes[1] < 2 && bytes.len() == 2 { Ok(Payload::Bool(bytes[1] != 0)) } else { Err(String::from("Failed to parse type")) },
            1 => Ok(Payload::BoolVector(bytes[1].try_into()?, bytes[6..6 + Self::get_vec_size(&bytes[2..6])?].iter().map(|x| *x != 0).collect())),
            -2 => Ok(Payload::GUID(u128::from_le_bytes(bytes[1..17].try_into().map_err(|_| String::from("Failed to parse type"))?))),
            2 => Ok(Payload::GUIDVector(bytes[1].try_into()?, bytes[6..6 + 16 * Self::get_vec_size(&bytes[2..6])?].chunks_exact(2)
                .map(|x| x.try_into().map(u128::from_le_bytes)).collect::<Result<Vec<u128>, TryFromSliceError>>().map_err(|x| x.to_string())?)),
            -4 => Ok(Payload::Byte(bytes[1])),
            4 => Ok(Payload::ByteVector(bytes[1].try_into()?, bytes[6..6 + Self::get_vec_size(&bytes[2..6])?].iter().copied().collect())),
            -5 => Ok(Payload::Short(u16::from_le_bytes(bytes[1..3].try_into().map_err(|_| String::from("Failed to parse type"))?))),
            5 => Ok(Payload::ShortVector(bytes[1].try_into()?, bytes[6..6 + 2 * Self::get_vec_size(&bytes[2..6])?].chunks_exact(2)
                .map(|x| x.try_into().map(u16::from_le_bytes)).collect::<Result<Vec<u16>, TryFromSliceError>>().map_err(|x| x.to_string())?)),
            -6 => Ok(Payload::Int(u32::from_le_bytes(bytes[1..5].try_into().map_err(|_| String::from("Failed to parse type"))?))),
            6 => Ok(Payload::IntVector(bytes[1].try_into()?, bytes[6..6 + 4 * Self::get_vec_size(&bytes[2..6])?].chunks_exact(4)
                .map(|x| x.try_into().map(u32::from_le_bytes)).collect::<Result<Vec<u32>, TryFromSliceError>>().map_err(|x| x.to_string())?)),
            -7 => Ok(Payload::Long(u64::from_le_bytes(bytes[1..9].try_into().map_err(|_| String::from("Failed to parse type"))?))),
            7 => Ok(Payload::LongVector(bytes[1].try_into()?, bytes[6..6 + 8 * Self::get_vec_size(&bytes[2..6])?].chunks_exact(8)
                .map(|x| x.try_into().map(u64::from_le_bytes)).collect::<Result<Vec<u64>, TryFromSliceError>>().map_err(|x| x.to_string())?)),
            -8 => Ok(Payload::Real(f32::from_le_bytes(bytes[1..5].try_into().map_err(|_| String::from("Failed to parse type"))?))),
            8 => Ok(Payload::RealVector(bytes[1].try_into()?, bytes[6..6 + 4 * Self::get_vec_size(&bytes[2..6])?].chunks_exact(4)
                .map(|x| x.try_into().map(f32::from_le_bytes)).collect::<Result<Vec<f32>, TryFromSliceError>>().map_err(|x| x.to_string())?)),
            -9 => Ok(Payload::Float(f64::from_le_bytes(bytes[1..9].try_into().map_err(|_| String::from("Failed to parse type"))?))),
            9 => Ok(Payload::FloatVector(bytes[1].try_into()?, bytes[6..6 + 8 * Self::get_vec_size(&bytes[2..6])?].chunks_exact(8)
                .map(|x| x.try_into().map(f64::from_le_bytes)).collect::<Result<Vec<f64>, TryFromSliceError>>().map_err(|x| x.to_string())?)),
            -10 => Ok(Payload::Char(char::from(bytes[1]))),
            10 => Ok(Payload::CharVector(bytes[1].try_into()?, AsciiString::from_ascii::<&[u8]>(
                &bytes[6..6 + Self::get_vec_size(&bytes[2..6])?]).map_err(|x| x.to_string())?)),
            -11 => Ok(Payload::Symbol(AsciiString::from_ascii::<&[u8]>(&bytes[1..].iter().copied().take_while(|x| *x != 0).collect::<Vec<u8>>().as_slice()).map_err(|x| x.to_string())?)),
            11 => {
                let vec_size = Self::get_vec_size(&bytes[2..6])?;
                let mut vec = Vec::new();
                let mut index = 6;
                for _ in 0..vec_size {
                    let string = AsciiString::from_ascii::<&[u8]>(&bytes[index..].iter().copied().take_while(|x| *x != 0).collect::<Vec<u8>>().as_slice()).map_err(|x| x.to_string())?;
                    let len = string.len();
                    vec.push(string);
                    index += len + 1;
                }
                Ok(Payload::SymbolVector(bytes[1].try_into()?, vec))
            }
            -12 => Ok(Payload::Timestamp(u64::from_le_bytes(bytes[1..9].try_into().map_err(|_| String::from("Failed to parse type"))?))),
            12 => Ok(Payload::TimestampVector(bytes[1].try_into()?, bytes[6..6 + 8 * Self::get_vec_size(&bytes[2..6])?].chunks_exact(8)
                .map(|x| x.try_into().map(u64::from_le_bytes)).collect::<Result<Vec<u64>, TryFromSliceError>>().map_err(|x| x.to_string())?)),
            -13 => Ok(Payload::Month(u32::from_le_bytes(bytes[1..5].try_into().map_err(|_| String::from("Failed to parse type"))?))),
            13 => Ok(Payload::MonthVector(bytes[1].try_into()?, bytes[6..6 + 4 * Self::get_vec_size(&bytes[2..6])?].chunks_exact(4)
                .map(|x| x.try_into().map(u32::from_le_bytes)).collect::<Result<Vec<u32>, TryFromSliceError>>().map_err(|x| x.to_string())?)),
            -14 => Ok(Payload::Date(u32::from_le_bytes(bytes[1..5].try_into().map_err(|_| String::from("Failed to parse type"))?))),
            14 => Ok(Payload::DateVector(bytes[1].try_into()?, bytes[6..6 + 4 * Self::get_vec_size(&bytes[2..6])?].chunks_exact(4)
                .map(|x| x.try_into().map(u32::from_le_bytes)).collect::<Result<Vec<u32>, TryFromSliceError>>().map_err(|x| x.to_string())?)),
            -15 => Ok(Payload::DateTime(u64::from_le_bytes(bytes[1..9].try_into().map_err(|_| String::from("Failed to parse type"))?))),
            15 => Ok(Payload::DateTimeVector(bytes[1].try_into()?, bytes[6..6 + 8 * Self::get_vec_size(&bytes[2..6])?].chunks_exact(8)
                .map(|x| x.try_into().map(u64::from_le_bytes)).collect::<Result<Vec<u64>, TryFromSliceError>>().map_err(|x| x.to_string())?)),
            -16 => Ok(Payload::TimeSpan(u64::from_le_bytes(bytes[1..9].try_into().map_err(|_| String::from("Failed to parse type"))?))),
            16 => Ok(Payload::TimeSpanVector(bytes[1].try_into()?, bytes[6..6 + 8 * Self::get_vec_size(&bytes[2..6])?].chunks_exact(8)
                .map(|x| x.try_into().map(u64::from_le_bytes)).collect::<Result<Vec<u64>, TryFromSliceError>>().map_err(|x| x.to_string())?)),
            -17 => Ok(Payload::Minute(u32::from_le_bytes(bytes[1..5].try_into().map_err(|_| String::from("Failed to parse type"))?))),
            17 => Ok(Payload::MinuteVector(bytes[1].try_into()?, bytes[6..6 + 4 * Self::get_vec_size(&bytes[2..6])?].chunks_exact(4)
                .map(|x| x.try_into().map(u32::from_le_bytes)).collect::<Result<Vec<u32>, TryFromSliceError>>().map_err(|x| x.to_string())?)),
            -18 => Ok(Payload::Second(u32::from_le_bytes(bytes[1..5].try_into().map_err(|_| String::from("Failed to parse type"))?))),
            18 => Ok(Payload::SecondVector(bytes[1].try_into()?, bytes[6..6 + 4 * Self::get_vec_size(&bytes[2..6])?].chunks_exact(4)
                .map(|x| x.try_into().map(u32::from_le_bytes)).collect::<Result<Vec<u32>, TryFromSliceError>>().map_err(|x| x.to_string())?)),
            -19 => Ok(Payload::Time(u32::from_le_bytes(bytes[1..5].try_into().map_err(|_| String::from("Failed to parse type"))?))),
            19 => Ok(Payload::TimeVector(bytes[1].try_into()?, bytes[6..6 + 4 * Self::get_vec_size(&bytes[2..6])?].chunks_exact(4)
                .map(|x| x.try_into().map(u32::from_le_bytes)).collect::<Result<Vec<u32>, TryFromSliceError>>().map_err(|x| x.to_string())?)),
            98 => Ok(Payload::Table(bytes[1].try_into()?, Box::new(Payload::from_bytes(&bytes[2..])?))),
            99 => {
                let key_payload = Payload::from_bytes(&bytes[1..])?;
                let value_payload = Payload::from_bytes(&bytes[key_payload.get_size() + 2..])?;
                Ok(Payload::Dictionary(Box::from(key_payload), Box::new(value_payload)))
            }
            -101 => Ok(Payload::Nil),
            101 => Ok(Payload::Nil),
            -128 => Ok(Payload::Error(AsciiString::from_ascii::<&[u8]>(&bytes[1..].iter().copied().take_while(|x| *x != 0).collect::<Vec<u8>>().as_slice()).map_err(|x| x.to_string())?)),
            _ => Err(format!("Failed to find type, {}", type_byte))
        }
    }

    fn get_vec_size(bytes: &[u8]) -> Result<usize, String> {
        bytes.try_into().map(|x| u32::from_le_bytes(x) as usize).map_err(|_| String::from("Failed to find vector size"))
    }

    pub const fn type_byte(&self) -> i8 {
        match self {
            Payload::List(_, _) => 0,
            Payload::Bool(_) => -1,
            Payload::BoolVector(_, _) => 1,
            Payload::GUID(_) => -2,
            Payload::GUIDVector(_, _) => 2,
            Payload::Byte(_) => -4,
            Payload::ByteVector(_, _) => 4,
            Payload::Short(_) => -5,
            Payload::ShortVector(_, _) => 5,
            Payload::Int(_) => -6,
            Payload::IntVector(_, _) => 6,
            Payload::Long(_) => -7,
            Payload::LongVector(_, _) => 7,
            Payload::Real(_) => -8,
            Payload::RealVector(_, _) => 8,
            Payload::Float(_) => -9,
            Payload::FloatVector(_, _) => 9,
            Payload::Char(_) => -10,
            Payload::CharVector(_, _) => 10,
            Payload::Symbol(_) => -11,
            Payload::SymbolVector(_, _) => 11,
            Payload::Timestamp(_) => -12,
            Payload::TimestampVector(_, _) => 12,
            Payload::Month(_) => -13,
            Payload::MonthVector(_, _) => 13,
            Payload::Date(_) => -14,
            Payload::DateVector(_, _) => 14,
            Payload::DateTime(_) => -15,
            Payload::DateTimeVector(_, _) => 15,
            Payload::TimeSpan(_) => -16,
            Payload::TimeSpanVector(_, _) => 16,
            Payload::Minute(_) => -17,
            Payload::MinuteVector(_, _) => 17,
            Payload::Second(_) => -18,
            Payload::SecondVector(_, _) => 18,
            Payload::Time(_) => -19,
            Payload::TimeVector(_, _) => 19,
            Payload::Table(_, _) => 98,
            Payload::Dictionary(_, _) => 99,
            Payload::Nil => -101,
            Payload::NilVector(_, _) => 101,
            Payload::Error(_) => -128,
        }
    }

    pub fn get_size(&self) -> usize {
        match self {
            Payload::List(_, x) => ATTRIBUTE_LEN as usize + VECTOR_LEN as usize + x.len() + x.iter().fold(0, |acc, val| acc + val.get_size()),
            Payload::Bool(_) => 1,
            Payload::BoolVector(_, x) => ATTRIBUTE_LEN as usize + VECTOR_LEN as usize + x.len(),
            Payload::GUID(_) => 16,
            Payload::GUIDVector(_, x) => ATTRIBUTE_LEN as usize + VECTOR_LEN as usize + 16 * x.len(),
            Payload::Byte(_) => 1,
            Payload::ByteVector(_, x) => ATTRIBUTE_LEN as usize + VECTOR_LEN as usize + x.len(),
            Payload::Short(_) => 2,
            Payload::ShortVector(_, x) => ATTRIBUTE_LEN as usize + VECTOR_LEN as usize + 2 * x.len(),
            Payload::Int(_) => 4,
            Payload::IntVector(_, x) => ATTRIBUTE_LEN as usize + VECTOR_LEN as usize + 4 * x.len(),
            Payload::Long(_) => 8,
            Payload::LongVector(_, x) => ATTRIBUTE_LEN as usize + VECTOR_LEN as usize + 8 * x.len(),
            Payload::Real(_) => 4,
            Payload::RealVector(_, x) => ATTRIBUTE_LEN as usize + VECTOR_LEN as usize + 4 * x.len(),
            Payload::Float(_) => 8,
            Payload::FloatVector(_, x) => ATTRIBUTE_LEN as usize + VECTOR_LEN as usize + 8 * x.len(),
            Payload::Char(_) => 1,
            Payload::CharVector(_, x) => 1 + 4 + x.len(),
            Payload::Symbol(x) => 1 + x.len(),
            Payload::SymbolVector(_, x) => ATTRIBUTE_LEN as usize + VECTOR_LEN as usize + x.len() + x.iter().fold(0, |acc, val| acc + val.len()),
            Payload::Timestamp(_) => 8,
            Payload::TimestampVector(_, x) => ATTRIBUTE_LEN as usize + VECTOR_LEN as usize + 8 * x.len(),
            Payload::Month(_) => 4,
            Payload::MonthVector(_, x) => ATTRIBUTE_LEN as usize + VECTOR_LEN as usize + 4 * x.len(),
            Payload::Date(_) => 4,
            Payload::DateVector(_, x) => ATTRIBUTE_LEN as usize + VECTOR_LEN as usize + 4 * x.len(),
            Payload::DateTime(_) => 8,
            Payload::DateTimeVector(_, x) => ATTRIBUTE_LEN as usize + VECTOR_LEN as usize + 8 * x.len(),
            Payload::TimeSpan(_) => 8,
            Payload::TimeSpanVector(_, x) => ATTRIBUTE_LEN as usize + VECTOR_LEN as usize + 8 * x.len(),
            Payload::Minute(_) => 4,
            Payload::MinuteVector(_, x) => ATTRIBUTE_LEN as usize + VECTOR_LEN as usize + 4 * x.len(),
            Payload::Second(_) => 4,
            Payload::SecondVector(_, x) => ATTRIBUTE_LEN as usize + VECTOR_LEN as usize + 4 * x.len(),
            Payload::Time(_) => 4,
            Payload::TimeVector(_, x) => ATTRIBUTE_LEN as usize + VECTOR_LEN as usize + 4 * x.len(),
            Payload::Table(_, y) => ATTRIBUTE_LEN as usize + y.get_size(),
            Payload::Dictionary(x, y) => x.get_size() + y.get_size(),
            Payload::Nil => 1,
            Payload::NilVector(_, x) => ATTRIBUTE_LEN as usize + VECTOR_LEN as usize + x.len(),
            Payload::Error(x) => x.len(),
        }
    }
}


#[cfg(test)]
mod tests {
    use ascii::{AsciiStr, AsciiString};
    use crate::codec::Payload;
    use crate::codec::VectorAttribute::NoAttribute;

    #[test]
    pub fn test_list_marshalling() {
        let char_vec_hex_str = hex::decode("0100000018000000000002000000f6610a00020000006162").unwrap();
        if let Payload::List(x, vals) = Payload::from_bytes(&char_vec_hex_str[8..]).unwrap() {
            assert_eq!(x, NoAttribute);
            assert_eq!(vals[0], Payload::Char('a'));
            assert_eq!(vals[1], Payload::CharVector(NoAttribute, AsciiString::from_ascii("ab").unwrap()));
        } else {
            panic!("Failed to get the right type");
        }
    }

    #[test]
    pub fn test_symbol_vector_marshalling() {
        let char_vec_hex_str = hex::decode("01000000260000000b000300000044656e7400426565626c6562726f78005072656665637400").unwrap();
        if let Payload::SymbolVector(x, string) = Payload::from_bytes(&char_vec_hex_str[8..]).unwrap() {
            assert_eq!(x, NoAttribute);
            assert_eq!(string, vec!["Dent", "Beeblebrox", "Prefect"]);
        }
    }

    #[test]
    pub fn test_char_vector_marshalling() {
        let char_vec_hex_str = hex::decode("01000000180000000a000a00000074686174736372617a79").unwrap();
        if let Payload::CharVector(x, string) = Payload::from_bytes(&char_vec_hex_str[8..]).unwrap() {
            assert_eq!(x, NoAttribute);
            assert_eq!(string, AsciiStr::from_ascii("thatscrazy").unwrap());
        }
    }

    #[test]
    pub fn test_long_vector_marshalling() {
        let char_vec_hex_str = hex::decode("010200001e00000007000200000001000000000000000200000000000000").unwrap();
        if let Payload::LongVector(x, string) = Payload::from_bytes(&char_vec_hex_str[8..]).unwrap() {
            assert_eq!(x, NoAttribute);
            assert_eq!(string, vec![1, 2])
        }
    }

    #[test]
    pub fn test_bool_marshalling() -> Result<(), ()> {
        let true_bool_hex_str = hex::decode("010000000a000000ff01").unwrap();
        let false_bool_hex_str = hex::decode("010000000a000000ff00").unwrap();
        if let Payload::Bool(x) = Payload::from_bytes(&true_bool_hex_str[8..]).unwrap() {
            assert!(x, true);
            Ok(())
        } else {
            Err(())
        }?;
        if let Payload::Bool(x) = Payload::from_bytes(&false_bool_hex_str[8..]).unwrap() {
            assert_eq!(x, false);
            Ok(())
        } else {
            Err(())
        }
    }

    #[test]
    pub fn test_guid_marshalling() -> Result<(), String> {
        let guid_hex_str = hex::decode("0100000019000000feddb87915b6722c32a6cf296061671e9d").unwrap();
        if let Payload::GUID(x) = Payload::from_bytes(&guid_hex_str[8..])? {
            assert_eq!(x, 0xddb87915b6722c32a6cf296061671e9du128.to_be());
            Ok(())
        } else {
            panic!()
        }
    }

    #[test]
    pub fn test_byte_marshalling() -> Result<(), String> {
        let guid_hex_str = hex::decode("010000000a000000fc2a").unwrap();
        if let Payload::Byte(x) = Payload::from_bytes(&guid_hex_str[8..])? {
            assert_eq!(x, 0x2a);
            Ok(())
        } else {
            panic!()
        }
    }

    #[test]
    pub fn test_short_marshalling() -> Result<(), String> {
        let guid_hex_str = hex::decode("010000000b000000fb7e00").unwrap();
        if let Payload::Short(x) = Payload::from_bytes(&guid_hex_str[8..])? {
            assert_eq!(x, 126);
            Ok(())
        } else {
            panic!()
        }
    }

    #[test]
    pub fn test_int_marshalling() -> Result<(), String> {
        let guid_hex_str = hex::decode("010000000d000000faa1b0b912").unwrap();
        if let Payload::Int(x) = Payload::from_bytes(&guid_hex_str[8..])? {
            assert_eq!(x, 314159265);
            Ok(())
        } else {
            panic!()
        }
    }

    #[test]
    pub fn test_long_marshalling() -> Result<(), String> {
        let guid_hex_str = hex::decode("0100000011000000f90000000008000000").unwrap();
        if let Payload::Long(x) = Payload::from_bytes(&guid_hex_str[8..])? {
            assert_eq!(x, 34359738368);
            Ok(())
        } else {
            panic!()
        }
    }

    #[test]
    pub fn test_real_marshalling() -> Result<(), String> {
        let guid_hex_str = hex::decode("010000000d000000f800004841").unwrap();
        if let Payload::Real(x) = Payload::from_bytes(&guid_hex_str[8..])? {
            assert_eq!(x, 12.5);
            Ok(())
        } else {
            panic!()
        }
    }

    #[test]
    pub fn test_float_marshalling() -> Result<(), String> {
        let guid_hex_str = hex::decode("0100000011000000f700000000c888d840").unwrap();
        if let Payload::Float(x) = Payload::from_bytes(&guid_hex_str[8..])? {
            assert_eq!(x, 25123.125);
            Ok(())
        } else {
            panic!()
        }
    }

    #[test]
    pub fn test_char_marshalling() -> Result<(), String> {
        let guid_hex_str = hex::decode("010000000a000000f661").unwrap();
        if let Payload::Char(x) = Payload::from_bytes(&guid_hex_str[8..])? {
            assert_eq!(x, 'a');
            Ok(())
        } else {
            panic!()
        }
    }
}