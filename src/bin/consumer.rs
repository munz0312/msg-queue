use task_queue::{Message, read_message, write_message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = tokio::net::TcpStream::connect("127.0.0.1:8989").await?;

    let job = Message::GetTask;
    write_message(&mut stream, &job).await?;
    println!("sent msg");

    if let Ok(reply) = read_message(&mut stream).await {
        match reply {
            Message::Task { id: _, payload } => {
                let msg = str::from_utf8(&payload)?;
                println!("received: {msg}");
            }
            _ => {
                println!("lol")
            }
        }
    }

    Ok(())
}
