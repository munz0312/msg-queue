use std::collections::VecDeque;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    SubmitTask { id: u64, payload: Vec<u8> },
    Task { id: u64, payload: Vec<u8> },
    Ack { request_id: u64 },
    GetTask,
    QueueEmpty,
}

pub enum ActorMessage {
    SubmitTask {
        id: u64,
        payload: Vec<u8>,
    },
    GetTask {
        respond_to: oneshot::Sender<Message>,
    },
}

struct TaskQueueActor {
    receiver: mpsc::Receiver<ActorMessage>,
    data: VecDeque<Message>,
}

impl TaskQueueActor {
    fn new(receiver: mpsc::Receiver<ActorMessage>) -> Self {
        Self {
            receiver,
            data: VecDeque::new(),
        }
    }

    fn push(&mut self, message: Message) {
        self.data.push_front(message);
    }

    fn pop(&mut self) -> Option<Message> {
        self.data.pop_back()
    }

    fn handle_message(&mut self, message: ActorMessage) {
        match message {
            ActorMessage::SubmitTask { id, payload } => {
                self.push(Message::Task { id, payload });
            }

            ActorMessage::GetTask { respond_to } => {
                if let Some(task) = self.pop() {
                    let _ = respond_to.send(task);
                } else {
                    let _ = respond_to.send(Message::QueueEmpty);
                }
            } // _ => {
              //     eprintln!("invalid message received")
              // }
        }
    }
}

async fn run_taskqueue_actor(mut actor: TaskQueueActor) {
    while let Some(message) = actor.receiver.recv().await {
        actor.handle_message(message);
    }
}

#[derive(Clone)]
pub struct TaskQueueHandler {
    sender: mpsc::Sender<ActorMessage>,
}

impl TaskQueueHandler {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(32);
        let actor = TaskQueueActor::new(receiver);
        tokio::spawn(run_taskqueue_actor(actor));

        Self { sender }
    }

    pub async fn submit_task(&self, id: u64, payload: Vec<u8>) {
        let msg = ActorMessage::SubmitTask { id, payload };
        let _ = self.sender.send(msg).await;
    }

    pub async fn get_task(&self) -> Message {
        let (send, recv) = oneshot::channel();
        let msg = ActorMessage::GetTask { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

impl Default for TaskQueueHandler {
    fn default() -> Self {
        Self::new()
    }
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
