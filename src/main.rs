use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(handle_connection(stream));
            }
            Err(e) => {
                anyhow::bail!("error: {}", e);
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream) -> anyhow::Result<()> {
    loop {
        let reader = BufReader::new(&mut stream);
        let mut lines_reader = reader.lines();
        if let Some(_next_line) = lines_reader.next_line().await? {
            stream.write_all(b"+PONG\r\n").await?;
        }
    }
}
