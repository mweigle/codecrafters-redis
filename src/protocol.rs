use std::borrow::Cow;

use anyhow::Context;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

#[derive(PartialEq, Eq, Debug)]
pub enum DataType<'a> {
    SimpleString(Cow<'a, str>),
    SimpleError(Cow<'a, str>),
    Integer(i64),
    BulkString(Cow<'a, str>),
    Array(Vec<DataType<'a>>),
}

pub async fn parse_data_type<'a>(
    reader: &mut BufReader<&mut TcpStream>,
) -> anyhow::Result<DataType<'a>> {
    let mut current_array = None;
    let mut s = String::new();
    loop {
        s.clear();
        reader.read_line(&mut s).await?;
        // println!("read at start of loop: {s}");
        let mut bytes = s.chars();
        let dt = match bytes.next().context("no data type given")? {
            '+' => DataType::SimpleString(Cow::Owned(s[1..].trim_end().to_string())),
            '-' => DataType::SimpleError(Cow::Owned(s[1..].trim_end().to_string())),
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
                DataType::BulkString(Cow::Owned(data))
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

pub async fn send_simple_string(stream: &mut TcpStream, msg: &str) -> anyhow::Result<()> {
    stream
        .write_all(format!("+{}\r\n", msg).as_bytes())
        .await
        .with_context(|| format!("failed to send simple string '{msg}'"))
}

pub async fn send_bulk_string(stream: &mut TcpStream, msg: &str) -> anyhow::Result<()> {
    stream
        .write_all(format!("${}\r\n{}\r\n", msg.len(), msg).as_bytes())
        .await
        .with_context(|| format!("failed to send bulk string '{msg}'"))
}

pub async fn send_null(stream: &mut TcpStream) -> anyhow::Result<()> {
    stream
        .write_all(b"$-1\r\n")
        .await
        .context("failed to send <null> bulk string")
}

pub async fn send_array<'a>(stream: &mut TcpStream, data: &[DataType<'a>]) -> anyhow::Result<()> {
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

pub async fn wait_for<'a>(stream: &mut TcpStream, expected: DataType<'a>) -> anyhow::Result<()> {
    let mut reader = BufReader::new(stream);
    let response = parse_data_type(&mut reader).await?;
    anyhow::ensure!(
        response == expected,
        "response differed from expected value"
    );
    Ok(())
}
