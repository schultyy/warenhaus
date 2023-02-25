use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::Parser;
use kafka::consumer::{Consumer, FetchOffset};
use serde::Deserialize;

#[derive(Debug, Parser)]
struct Cli {
    ///Defaults to localhost:9092
    #[arg(short, long)]
    kafka_broker: Option<String>,
    #[arg(short = 't', long)]
    kafka_topic: String,
    ///Path to Mapping File, e.g. mappings.json
    #[arg(short, long)]
    mapping_file_path: String,
}

#[derive(Deserialize)]
struct Mapping {
    kafka_field: String,
    database_field: String,
    /////Can be Int, String, Float or Bool
    //database_type: String
}

fn load_mapping_file(mapping_file: &str) -> Result<Vec<Mapping>> {
    let data = fs::read_to_string(mapping_file)?;
    let json: Vec<Mapping> = serde_json::from_str(&data)
        .with_context(|| format!("{} does not have the correct format", mapping_file))?;

    Ok(json)
}

fn insert_record(fields: Vec<String>, values: Vec<serde_json::Value>) -> Result<()> {
    let mut payload = serde_json::Map::new();

    let fields = fields
        .into_iter()
        .map(|f| serde_json::Value::String(f))
        .collect();

    payload.insert("fields".to_string(), serde_json::Value::Array(fields));
    payload.insert("values".to_string(), serde_json::Value::Array(values));
    let payload = serde_json::Value::Object(payload);

    let client = reqwest::blocking::Client::new();
    let _request = client
        .post("http://localhost:3030/index")
        .body(payload.to_string())
        .send()?;
    Ok(())
}

fn map_value(json_str: &str, config: &Vec<Mapping>) -> Result<()> {
    let kafka_payload: serde_json::Value =
        serde_json::from_str(json_str)
        .with_context(|| format!("Failed to deserialize Kafka payload: {}", json_str))?;

    println!("Deserialized payload");

    let mut fields = vec![];
    let mut values = vec![];

    for mapping in config {
        if let Some(kafka_field) = kafka_payload.get(&mapping.kafka_field) {
            fields.push(mapping.database_field.to_string());
            values.push(kafka_field.to_owned());
        }
    }

    if fields.len() == config.len() && values.len() == config.len() {
        println!("Validated mapping. Ready to insert");
        match insert_record(fields, values.to_owned()) {
            Ok(()) => {}
            Err(err) => {
                eprintln!("Failed to insert data: {}", err);
            }
        }
    }

    Ok(())
}

fn consume(consumer: &mut Consumer, configuration: Vec<Mapping>) {
    loop {
        for ms in consumer.poll().unwrap().iter() {
            for m in ms.messages() {
                let str = String::from_utf8_lossy(m.value);
                if let Err(err) = map_value(&str.to_string(), &configuration) {
                    eprintln!("ERR: {}", err);
                }
            }
            let _ = consumer.consume_messageset(ms);
        }
        consumer.commit_consumed().unwrap();
    }
}

fn main() -> Result<()> {
    let cli_args = Cli::parse();
    let mapping_configuration = load_mapping_file(&cli_args.mapping_file_path)?;
    let mut consumer = Consumer::from_hosts(vec![cli_args
        .kafka_broker
        .unwrap_or("localhost:9092".to_owned())])
    .with_topic(cli_args.kafka_topic)
    .with_fallback_offset(FetchOffset::Earliest)
    .create()
    .unwrap();
    consume(&mut consumer, mapping_configuration);
    Ok(())
}
