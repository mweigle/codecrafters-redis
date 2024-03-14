use std::{
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
};

fn main() -> anyhow::Result<()> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                handle_connection(stream)?;
            }
            Err(e) => {
                anyhow::bail!("error: {}", e);
            }
        }
    }
    Ok(())
}

fn handle_connection(mut stream: TcpStream) -> anyhow::Result<()> {
    loop {
        let reader = BufReader::new(&mut stream);
        let mut lines_reader = reader.lines();
        if let Some(next_line) = lines_reader.next() {
            let _line = next_line?;
            write!(stream, "+PONG\r\n")?;
        }
    }
}
