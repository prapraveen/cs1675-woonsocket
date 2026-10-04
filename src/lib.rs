mod client_run;
pub mod closed_loop;
pub mod open_loop;

use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};
use woonsocket_work::Work;

#[derive(Serialize, Deserialize, Debug)]
pub struct Request {
    pub request_id: u64,
    pub scheduled_ns: u64,
    pub generated_ns: u64,
    pub work: Work,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Response {
    pub request_id: u64,
    pub scheduled_ns: u64,
    pub generated_ns: u64,
    pub server_processing_time: u64,
    pub payload: Vec<u8>,
}

pub fn read_request(reader: &mut impl Read) -> io::Result<Request> {
    let mut header = [0u8; 4];
    reader.read_exact(&mut header[..])?;

    let length = u32::from_be_bytes(header) as usize;
    if length == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid request length",
        ));
    }

    let mut bytes = vec![0u8; length];
    reader.read_exact(&mut bytes)?;
    match serde_json::from_slice::<Request>(&bytes) {
        Ok(request) => Ok(request),
        Err(error) => Err(io::Error::new(io::ErrorKind::InvalidData, error)),
    }
}

pub fn write_response(writer: &mut impl Write, response: &Response) -> io::Result<()> {
    let bytes = serde_json::to_vec(response)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let length = u32::try_from(bytes.len())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    writer.write_all(&length.to_be_bytes())?;
    writer.write_all(&bytes)
}

pub fn write_request(writer: &mut impl Write, request: &Request) -> io::Result<()> {
    let bytes = serde_json::to_vec(request)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let length = u32::try_from(bytes.len())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    writer.write_all(&length.to_be_bytes())?;
    writer.write_all(&bytes)
}

pub fn read_response(reader: &mut impl Read) -> io::Result<Response> {
    let mut header = [0u8; 4];
    reader.read_exact(&mut header)?;
    let length = u32::from_be_bytes(header) as usize;
    let mut bytes = vec![0u8; length];
    reader.read_exact(&mut bytes)?;
    serde_json::from_slice(&bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}
