use std::{ops::Deref, sync::Arc};

use anyhow::Context;
use bytes::Bytes;
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    time::{Duration, Instant},
};

use crate::{
    protocol::{self, DataType},
    Config, Store, StoreValue,
};

pub async fn invoke_echo<'a>(
    stream: &mut TcpStream,
    mut args: impl Iterator<Item = DataType<'a>>,
) -> anyhow::Result<()> {
    let Some(DataType::BulkString(echo_string)) = args.next() else {
        anyhow::bail!("invalid argument argument given to 'echo' command");
    };
    protocol::send_bulk_string(stream, &echo_string).await
}

pub async fn invoke_set<'a>(
    stream: &mut TcpStream,
    mut args: impl Iterator<Item = DataType<'a>>,
    store: &Store,
) -> anyhow::Result<()> {
    let (Some(DataType::BulkString(k)), Some(DataType::BulkString(v))) = (args.next(), args.next())
    else {
        anyhow::bail!("key and value must be bulk strings");
    };
    let mut value = StoreValue {
        value: v.into_owned(),
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
    store.lock().await.insert(k.into_owned(), value);
    protocol::send_simple_string(stream, "OK").await
}

pub async fn invoke_get<'a>(
    stream: &mut TcpStream,
    mut args: impl Iterator<Item = DataType<'a>>,
    store: &Store,
) -> anyhow::Result<()> {
    let Some(DataType::BulkString(k)) = args.next() else {
        anyhow::bail!("key must be given!");
    };
    // println!("get '{}': {:?}", k, store.lock().await.get(k));
    // println!("store atm: {:?}", store);
    match store.lock().await.get(k.deref()) {
        Some(v) => match v.expiry {
            Some(expiry) if expiry <= Instant::now() => {
                // entry exists but is expired
                protocol::send_null(stream).await
            }
            _ => protocol::send_bulk_string(stream, &v.value).await,
        },
        None => protocol::send_null(stream).await,
    }
}

pub async fn invoke_info<'a>(
    stream: &mut TcpStream,
    mut args: impl Iterator<Item = DataType<'a>>,
    config: &Arc<Config>,
) -> anyhow::Result<()> {
    let Some(DataType::BulkString(command)) = args.next() else {
        anyhow::bail!("command must be given!")
    };
    if command == "replication" {
        let role = if config.replica_of.is_none() {
            "master"
        } else {
            "slave"
        };
        return protocol::send_bulk_string(
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

pub async fn invoke_psync(stream: &mut TcpStream, config: &Arc<Config>) -> anyhow::Result<()> {
    protocol::send_simple_string(
        stream,
        &format!(
            "FULLRESYNC {} {}",
            config.replication_id, config.replication_offset
        ),
    )
    .await?;
    let rdb = Bytes::from_static(&[
        82, 69, 68, 73, 83, 48, 48, 49, 49, 250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114, 5,
        55, 46, 50, 46, 48, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192, 64, 250,
        5, 99, 116, 105, 109, 101, 194, 109, 8, 188, 101, 250, 8, 117, 115, 101, 100, 45, 109, 101,
        109, 194, 176, 196, 16, 0, 250, 8, 97, 111, 102, 45, 98, 97, 115, 101, 192, 0, 255, 240,
        110, 59, 254, 192, 255, 90, 162,
    ]);
    stream
        .write_all(format!("${}\r\n", rdb.len()).as_bytes())
        .await?;
    stream.write_all(&rdb).await.context("failed to send file")
}
