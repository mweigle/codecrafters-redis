use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let store = Arc::new(Mutex::new(HashMap::new()));
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(handle_connection(stream, Arc::clone(&store)));
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

async fn handle_connection(
    mut stream: TcpStream,
    store: Arc<Mutex<HashMap<String, String>>>,
) -> anyhow::Result<()> {
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
                        "ping" => send_simple_string(&mut stream, "PONG").await?,
                        "set" => {
                            let (DataType::BulkString(k), DataType::BulkString(v)) =
                                (&arr[1], &arr[2])
                            else {
                                anyhow::bail!("key and value must be given");
                            };
                            store.lock().await.insert(k.clone(), v.clone());
                            send_simple_string(&mut stream, "OK").await?;
                        }
                        "get" => {
                            let DataType::BulkString(k) = &arr[1] else {
                                anyhow::bail!("key must be given!");
                            };
                            match store.lock().await.get(k) {
                                Some(v) => send_bulk_string(&mut stream, v).await?,
                                None => send_null(&mut stream).await?,
                            }
                        }
                        other => anyhow::bail!("command {other} is not yet implemented"),
                    }
                }
            }
            other => anyhow::bail!("{:?} not yet implemented!", other),
        }
    }
}

async fn send_simple_string(stream: &mut TcpStream, msg: &str) -> anyhow::Result<()> {
    stream
        .write_all(format!("+{}\r\n", msg).as_bytes())
        .await
        .with_context(|| format!("failed to send simple string '{msg}'"))
}

async fn send_bulk_string(stream: &mut TcpStream, msg: &str) -> anyhow::Result<()> {
    stream
        .write_all(format!("${}\r\n{}\r\n", msg.len(), msg).as_bytes())
        .await
        .with_context(|| format!("failed to send bulk string '{msg}'"))
}

async fn send_null(stream: &mut TcpStream) -> anyhow::Result<()> {
    stream
        .write_all(b"$-1\r\n")
        .await
        .context("failed to send <null> bulk string")
}

async fn parse_data_type(reader: &mut BufReader<&mut TcpStream>) -> anyhow::Result<DataType> {
    let mut current_array = None;
    let mut s = String::new();
    loop {
        s.clear();
        reader.read_line(&mut s).await?;
        // println!("read at start of loop: {s}");
        let mut bytes = s.chars();
        let dt = match bytes.next().context("no data type given")? {
            '+' => DataType::SimpleString(s[1..].trim_end().to_string()),
            '-' => DataType::SimpleError(s[1..].trim_end().to_string()),
            ':' => {
                let value_str = s[1..].trim_end();
                let value = value_str
                    .parse::<i64>()
                    .with_context(|| format!("{value_str} is not a valid integer"))?;
                DataType::Integer(value)
            }
            '$' => {
                let length_str = &s[1..bytes.take_while(|c| *c != '\r').count() + 1];
                let length = length_str.parse().unwrap();
                let mut data = String::new();
                reader.read_line(&mut data).await?;
                let data = data.trim_end().to_string();
                assert_eq!(data.len(), length, "string length was wrong");
                DataType::BulkString(data)
            }
            '*' => {
                let element_count = s[1..bytes.take_while(|c| *c != '\r').count() + 1]
                    .parse()
                    .unwrap();
                // println!("array detected, element count: {element_count}");
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
