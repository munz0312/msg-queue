use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    SubmitMessage { id: u64, payload: Vec<u8> },
    Ack { request_id: u64 },
    GetMessage {},
}

pub async fn write_message(stream: &mut TcpStream, msg: &Message) -> std::io::Result<()> {
    let bytes = postcard::to_allocvec(msg).expect("Serialisation failed");
    let len = bytes.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    Ok(())
}

pub async fn read_message(stream: &mut TcpStream) -> std::io::Result<Message> {
    let mut buf = [0u8; 4];
    match stream.read_exact(&mut buf).await {
        Ok(_) => {}
        Err(e) => return Err(e),
    }

    let len = u32::from_be_bytes(buf) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    let msg = postcard::from_bytes(&payload)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    Ok(msg)
}
