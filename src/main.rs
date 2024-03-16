use std::{
    borrow::Cow,
    collections::HashMap,
    env,
    net::{IpAddr, Ipv4Addr},
    str::FromStr,
    sync::Arc,
};

use tokio::{
    io::BufReader,
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::Instant,
};

use crate::protocol::DataType;

mod commands;
mod protocol;

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
    protocol::send_array(&mut stream, &[DataType::BulkString(Cow::Borrowed("PING"))]).await?;
    protocol::wait_for(&mut stream, DataType::SimpleString(Cow::Borrowed("PONG"))).await?;
    protocol::send_array(
        &mut stream,
        &[
            DataType::BulkString(Cow::Borrowed("REPLCONF")),
            DataType::BulkString(Cow::Borrowed("listening-port")),
            DataType::BulkString(Cow::Borrowed(port)),
        ],
    )
    .await?;
    protocol::wait_for(&mut stream, DataType::SimpleString(Cow::Borrowed("OK"))).await?;
    protocol::send_array(
        &mut stream,
        &[
            DataType::BulkString(Cow::Borrowed("REPLCONF")),
            DataType::BulkString(Cow::Borrowed("capa")),
            DataType::BulkString(Cow::Borrowed("psync2")),
        ],
    )
    .await?;
    protocol::wait_for(&mut stream, DataType::SimpleString(Cow::Borrowed("OK"))).await?;
    protocol::send_array(
        &mut stream,
        &[
            DataType::BulkString(Cow::Borrowed("PSYNC")),
            DataType::BulkString(Cow::Borrowed("?")),
            DataType::BulkString(Cow::Borrowed("-1")),
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
        let data_type = protocol::parse_data_type(&mut reader).await?;
        match data_type {
            DataType::Array(arr) => {
                let mut args = arr.into_iter();
                let Some(DataType::BulkString(command)) = args.next() else {
                    continue;
                };
                match command.to_ascii_uppercase().as_str() {
                    "ECHO" => commands::invoke_echo(&mut stream, args).await?,
                    "PING" => protocol::send_simple_string(&mut stream, "PONG").await?,
                    "SET" => {
                        commands::invoke_set(&mut stream, args, &store).await?;
                    }
                    "GET" => commands::invoke_get(&mut stream, args, &store).await?,
                    "INFO" => commands::invoke_info(&mut stream, args, &config).await?,
                    "REPLCONF" => protocol::send_simple_string(&mut stream, "OK").await?,
                    "PSYNC" => commands::invoke_psync(&mut stream, &config).await?,
                    other => anyhow::bail!("command {other} is not yet implemented"),
                }
            }
            other => anyhow::bail!("{:?} not yet implemented!", other),
        }
    }
}
