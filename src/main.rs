use anyhow::Context;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

use bytes::Buf;

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

#[derive(PartialEq, Eq, Debug)]
enum DataType {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<DataType>),
}

async fn handle_connection(mut stream: TcpStream) -> anyhow::Result<()> {
    loop {
        let mut reader = BufReader::new(&mut stream);
        let data_type = parse_data_type(&mut reader).await?;
        match data_type {
            DataType::Array(arr) => {
                if let DataType::BulkString(command) = arr.get(0).context("no command given")? {
                    match command.as_str() {
                        "echo" => {
                            let DataType::BulkString(echo_string) =
                                arr.get(1).with_context(|| {
                                    format!("command {command} needs a second argument")
                                })?
                            else {
                                anyhow::bail!("wrong data type for argument to {command}");
                            };
                            send_bulk_string(&mut stream, &echo_string).await?;
                        }
                        "ping" => stream.write_all(b"+PONG\r\n").await?,
                        other => anyhow::bail!("command {other} is not yet implemented"),
                    }
                }
            }
            other => anyhow::bail!("{:?} not yet implemented!", other),
        }
    }
}

async fn send_bulk_string(stream: &mut TcpStream, msg: &str) -> anyhow::Result<()> {
    stream
        .write_all(format!("${}\r\n{}\r\n", msg.len(), msg).as_bytes())
        .await
        .with_context(|| format!("failed to send bulk string '{msg}'"))
}

async fn parse_data_type(reader: &mut BufReader<&mut TcpStream>) -> anyhow::Result<DataType> {
    let mut current_array = None;
    let mut s = String::new();
    loop {
        s.clear();
        reader.read_line(&mut s).await?;
        println!("read at start of loop: {s}");
        let mut bytes = s.chars();
        let dt = match bytes.next().context("no data type given")? {
            '+' => DataType::SimpleString(s[1..].to_string()),
            '-' => DataType::SimpleError(s[1..].to_string()),
            ':' => DataType::Integer(todo!()), // TODO: probably does not work
            '$' => {
                println!("bytes at start of bulk string parsing: {:?}", bytes);
                let length = s[1..bytes.take_while(|c| *c != '\r').count() + 1]
                    .parse()
                    .unwrap();
                let mut data = String::new();
                reader.read_line(&mut data).await?;
                assert_eq!(data.len(), length, "string length was wrong");
                DataType::BulkString(data)
            }
            '*' => {
                let element_count = s[1..bytes.take_while(|c| *c != '\r').count() + 1]
                    .parse()
                    .unwrap();
                println!("array detected, element count: {element_count}");
                if element_count > 0 {
                    current_array = Some((Vec::with_capacity(element_count), element_count));
                    continue;
                } else {
                    DataType::Array(Vec::new())
                }
            }
            other => anyhow::bail!("data type {other} is not implemented"),
        };
        if let Some((arr, element_count)) = &mut current_array {
            arr.push(dt);
            if arr.len() == *element_count {
                return Ok(DataType::Array(current_array.take().unwrap().0));
            }
        } else {
            return Ok(dt);
        }
    }
}
