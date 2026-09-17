use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use msg_queue::{
    Message::{self, GetMessage, SubmitMessage},
    read_message, write_message,
};
use tokio::net::TcpStream;

struct MessageQueue {
    buffer: VecDeque<Message>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8989").await?;

    let queue = Arc::new(Mutex::new(MessageQueue {
        buffer: VecDeque::new(),
    }));
    loop {
        let (mut socket, addr) = listener.accept().await?;
        let queue_clone = queue.clone();

        tokio::spawn(async move {
            handle_connection(&mut socket, addr, queue_clone).await;
        });
    }
}

async fn handle_connection(
    socket: &mut TcpStream,
    addr: SocketAddr,
    queue: Arc<Mutex<MessageQueue>>,
) {
    match read_message(socket).await {
        Ok(SubmitMessage { id, payload }) => {
            println!("got message {id} ({} bytes) from {addr}", payload.len());
            queue
                .lock()
                .expect("Couldn't acquire mutex")
                .buffer
                .push_front(SubmitMessage { id, payload });
            let ack = Message::Ack { request_id: id };
            let _ = write_message(socket, &ack).await;
        }

        Ok(GetMessage {}) => {
            let msg = queue
                .lock()
                .expect("Couldn't acquire mutex")
                .buffer
                .pop_front();
            match msg {
                Some(message) => {
                    let _ = write_message(socket, &message).await;
                }
                None => println!("queue is empty"),
            }
        }

        Ok(_) => println!("invalid msg type"),
        Err(e) => eprintln!("{e}"),
    }
}
