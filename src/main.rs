use std::{io::Write, net::TcpListener};

fn main() -> anyhow::Result<()> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                write!(stream, "+PONG\r\n")?;
            }
            Err(e) => {
                anyhow::bail!("error: {}", e);
            }
        }
    }
    Ok(())
}
