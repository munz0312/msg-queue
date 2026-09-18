use std::net::SocketAddr;

use task_queue::{
    Message::{self},
    TaskQueueHandler, read_message, write_message,
};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8989").await?;

    let queue = TaskQueueHandler::new();
    loop {
        let (mut socket, addr) = listener.accept().await?;
        let queue_clone = queue.clone();

        tokio::spawn(async move {
            handle_connection(&mut socket, addr, queue_clone).await;
        });
    }
}

async fn handle_connection(socket: &mut TcpStream, addr: SocketAddr, queue: TaskQueueHandler) {
    match read_message(socket).await {
        Ok(Message::SubmitTask { id, payload }) => {
            println!("got message {id} ({} bytes) from {addr}", payload.len());
            queue.submit_task(id, payload).await;
            let ack = Message::Ack { request_id: id };
            let _ = write_message(socket, &ack).await;
        }

        Ok(Message::GetTask) => {
            let msg = queue.get_task().await;
            let _ = write_message(socket, &msg).await;
        }

        Ok(_) => println!("invalid msg type"),
        Err(e) => eprintln!("{e}"),
    }
}
