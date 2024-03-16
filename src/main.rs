use std::{
    collections::HashMap,
    env,
    net::{IpAddr, Ipv4Addr},
    str::FromStr,
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

#[derive(Debug)]
struct ReplicaOf {
    master_host: IpAddr,
    master_port: u16,
}

#[derive(Debug)]
struct Config {
    port: String,
    replica_of: Option<ReplicaOf>,
    replication_id: String,
    replication_offset: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: DEFAULT_PORT.to_string(),
            replica_of: None,
            replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            replication_offset: 0,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut config = Config::default();
    let mut args = env::args();
    while let Some(arg) = args.next() {
        if arg == "--port" {
            if let Some(p) = args.next() {
                config.port = p;
            }
        }
        if arg == "--replicaof" {
            if let (Some(mut host), Some(port)) = (args.next(), args.next()) {
                if host == "localhost" {
                    host = "127.0.0.1".to_string();
                }
                config.replica_of = Some(ReplicaOf {
                    master_host: IpAddr::V4(Ipv4Addr::from_str(&host)?),
                    master_port: port.parse()?,
                });
            }
        }
    }
    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).await?;
    let store = Arc::new(Mutex::new(HashMap::new()));

    if let Some(repl_config) = &config.replica_of {
        master_handshake(repl_config, &config.port).await?;
    }

    let config = Arc::new(config);
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(handle_connection(
                    stream,
                    Arc::clone(&store),
                    Arc::clone(&config),
                ));
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

async fn master_handshake(repl_config: &ReplicaOf, port: &str) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(format!(
        "{}:{}",
        repl_config.master_host, repl_config.master_port
    ))
    .await?;
    send_array(&mut stream, &[DataType::BulkString("ping".to_string())]).await?;
    let mut reader = BufReader::new(&mut stream);
    let data_type = parse_data_type(&mut reader).await?;
    let DataType::SimpleString(s) = data_type else {
        anyhow::bail!("master sent unexpected response");
    };
    assert_eq!(s, "PONG", "'{s}' was sent instead of PONG");
    send_array(
        &mut stream,
        &[
            DataType::BulkString("REPLCONF".to_string()),
            DataType::BulkString("listening-port".to_string()),
            DataType::BulkString(port.to_string()),
        ],
    )
    .await?;
    let mut reader = BufReader::new(&mut stream);
    let data_type = parse_data_type(&mut reader).await?;
    let DataType::SimpleString(s) = data_type else {
        anyhow::bail!("master sent unexpected response");
    };
    assert_eq!(s, "OK", "'{s}' was sent instead of OK");
    send_array(
        &mut stream,
        &[
            DataType::BulkString("REPLCONF".to_string()),
            DataType::BulkString("capa".to_string()),
            DataType::BulkString("psync2".to_string()),
        ],
    )
    .await?;
    let mut reader = BufReader::new(&mut stream);
    let data_type = parse_data_type(&mut reader).await?;
    let DataType::SimpleString(s) = data_type else {
        anyhow::bail!("master sent unexpected response");
    };
    assert_eq!(s, "OK", "'{s}' was sent instead of OK"); // TODO: lots of duplication here, make function "wait_for_X"
    send_array(
        &mut stream,
        &[
            DataType::BulkString("PSYNC".to_string()),
            DataType::BulkString("?".to_string()),
            DataType::BulkString("-1".to_string()),
        ],
    )
    .await
}

async fn handle_connection(
    mut stream: TcpStream,
    store: Store,
    config: Arc<Config>,
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
                        "set" => invoke_set(&mut stream, arr, &store).await?,
                        "get" => invoke_get(&mut stream, arr, &store).await?,
                        "info" => invoke_info(&mut stream, arr, &config).await?,
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
    // println!("get '{}': {:?}", k, store.lock().await.get(k));
    // println!("store atm: {:?}", store);
    match store.lock().await.get(k) {
        Some(v) => match v.expiry {
            Some(expiry) if expiry <= Instant::now() => {
                // entry exists but is expired
                send_null(stream).await
            }
            _ => send_bulk_string(stream, &v.value).await,
        },
        None => send_null(stream).await,
    }
}

async fn invoke_info(
    stream: &mut TcpStream,
    arr: Vec<DataType>,
    config: &Arc<Config>,
) -> anyhow::Result<()> {
    let mut args = arr.into_iter().skip(1);
    let Some(DataType::BulkString(command)) = args.next() else {
        anyhow::bail!("command must be given!")
    };
    if command == "replication" {
        let role = if config.replica_of.is_none() {
            "master"
        } else {
            "slave"
        };
        return send_bulk_string(
            stream,
            &format!(
                "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
                role, config.replication_id, config.replication_offset
            ),
        )
        .await;
    }
    // send_bulk_string(stream, "").await
    todo!()
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

async fn send_array(stream: &mut TcpStream, data: &[DataType]) -> anyhow::Result<()> {
    stream
        .write_all(format!("*{}\r\n", data.len()).as_bytes())
        .await
        .with_context(|| format!("failed to send array length for {:?}", data))?;
    for dt in data {
        match dt {
            DataType::BulkString(bs) => send_bulk_string(stream, bs).await?,
            _ => anyhow::bail!("not yet implemented!"),
        }
    }
    Ok(())
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
