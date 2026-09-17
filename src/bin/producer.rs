use msg_queue::{Message, read_message, write_message};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut stream = tokio::net::TcpStream::connect("127.0.0.1:8989").await?;

    let job = Message::SubmitMessage {
        id: 1,
        payload: "hello worker".as_bytes().to_vec(),
    };
    write_message(&mut stream, &job).await?;
    println!("sent message");

    if let Ok(reply) = read_message(&mut stream).await {
        println!("received: {reply:?}");
    }

    Ok(())
}
