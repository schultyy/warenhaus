use kafka::consumer::{Consumer, FetchOffset};
use clap::Parser;

#[derive(Debug, Parser)]
struct Cli {
    ///Defaults to localhost:9092
    #[arg(short, long)]
    kafka_broker: Option<String>,
    #[arg(short='t', long)]
    kafka_topic: String
}

fn main() {
    let cli_args = Cli::parse();
    let mut consumer =
       Consumer::from_hosts(vec!(cli_args.kafka_broker.unwrap_or("localhost:9092".to_owned())))
          .with_topic(cli_args.kafka_topic)
          .with_fallback_offset(FetchOffset::Earliest)
          .create()
          .unwrap();
    loop {
      for ms in consumer.poll().unwrap().iter() {
        for m in ms.messages() {
          let str = String::from_utf8_lossy(m.value);
          println!("{:?}",str);
        }
        let _ = consumer.consume_messageset(ms);
      }
      consumer.commit_consumed().unwrap();
    }
}

