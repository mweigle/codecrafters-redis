use std::{
    collections::HashMap,
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

const DEFAULT_PORT: &str = "6379";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut port = DEFAULT_PORT.to_string();
    let mut args = env::args();
    while let Some(arg) = args.next() {
        if arg == "--port" {
            if let Some(p) = args.next() {
                port = p;
            }
        }
    }
    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;
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

type Store = Arc<Mutex<HashMap<String, StoreValue>>>;

#[derive(Debug)]
struct StoreValue {
    value: String,
    expiry: Option<Instant>,
}

async fn handle_connection(mut stream: TcpStream, store: Store) -> anyhow::Result<()> {
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
                        "set" => invoke_set(&mut stream, arr, &store).await?,
                        "get" => invoke_get(&mut stream, arr, &store).await?,
                        other => anyhow::bail!("command {other} is not yet implemented"),
                    }
                }
            }
            other => anyhow::bail!("{:?} not yet implemented!", other),
        }
    }
}

async fn invoke_set(
    stream: &mut TcpStream,
    arr: Vec<DataType>,
    store: &Store,
) -> anyhow::Result<()> {
    let mut args = arr.into_iter().skip(1);
    let (Some(DataType::BulkString(k)), Some(DataType::BulkString(v))) = (args.next(), args.next())
    else {
        anyhow::bail!("key and value must be bulk strings");
    };
    let mut value = StoreValue {
        value: v,
        expiry: None,
    };
    if let Some(DataType::BulkString(arg)) = args.next() {
        if arg == "px" {
            let Some(DataType::BulkString(millis)) = args.next() else {
                anyhow::bail!("PX without expiry");
            };
            let millis = millis
                .parse()
                .with_context(|| format!("{millis} is not a valid integer"))?;
            value.expiry = Some(Instant::now() + Duration::from_millis(millis));
        }
    }
    store.lock().await.insert(k, value);
    send_simple_string(stream, "OK").await
}

async fn invoke_get(
    stream: &mut TcpStream,
    arr: Vec<DataType>,
    store: &Store,
) -> anyhow::Result<()> {
    let DataType::BulkString(k) = &arr[1] else {
        anyhow::bail!("key must be given!");
    };
    println!("get '{}': {:?}", k, store.lock().await.get(k));
    println!("store atm: {:?}", store);
    match store.lock().await.get(k) {
        Some(v) => match v.expiry {
            Some(expiry) if expiry <= Instant::now() => {
                println!("expired");
                // entry exists but is expired
                // let _ = store.lock().await.remove(k);
                send_null(stream).await
            }
            _ => {
                println!("send");
                send_bulk_string(stream, &v.value).await
            }
        },
        None => {
            println!("does not exist");
            send_null(stream).await
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
