use crate::{storage::Container, query::code_runner::CodeRunner, command::Command};
use config::Configurator;

use tokio::sync::mpsc;
use tracing::{error, debug};

mod storage;
mod web;
mod config;
mod query;
mod command;

fn database_storage_root_path() -> &'static str {
    "db"
}

fn compiled_map_fn_path() -> &'static str {
    "queries"
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>{
    tracing_subscriber::fmt::init();

    let (manager_tx, mut rx) = mpsc::channel(8192);
    let web_tx = manager_tx.clone();
    let mut all_workers = vec![];
    let configurator = Configurator::new();
    let config = configurator.load()?;
    let url_manager = tokio::spawn(async move {
        let mut storage_manager = Container::new(database_storage_root_path(), config).expect("failed to load container");
        while let Some(command) = rx.recv().await {
            debug!("Received Command: {:?}", command);
            match command {
                Command::Index { params, responder } => {
                    if let Err(err) = storage_manager.index(params) {
                        error!("{}", err);
                        if let Err(_) = responder.send(Err(err)) {
                            error!("Error while sending storage response");
                        }
                    } else {
                        if responder.send(Ok(())).is_err() {
                            error!("Error while sending storage response");
                        }
                    }
                },
                Command::AddMapFn {fn_name, source_code, responder } => {
                    debug!("Adding new Map Function: {}", fn_name);
    
                    let code_runner = CodeRunner::new(compiled_map_fn_path().into()).expect("Failed to instatiate Code pipeline");

                    match code_runner.compile_and_store(&source_code, &fn_name) {
                        Ok(()) => {
                            if responder.send(Ok(())).is_err() {
                                error!("Error while sending wasm response");
                            }
                        },
                        Err(err) => {
                            if responder.send(Err(err)).is_err() {
                                error!("Error while sending wasm response");
                            }
                        }
                    }
                },
                Command::InvokeMap { fn_name, responder } => {
                    debug!("Execute Map function: {}", fn_name);
                    let fn_name = fn_name.clone();

                    let code_runner = CodeRunner::new(compiled_map_fn_path().into()).expect("Failed to instatiate Code pipeline");

                    let (tx, mut rx) = mpsc::channel(8);

                    storage_manager.query(tx).await;
                    debug!("Queried Storage Manager");

                    let mut rows = vec!();

                    while let Some(payload) = rx.recv().await {
                        debug!("Received Storage Manager Callback");
                        match payload {
                            Command::QueryRow { row } => {
                                debug!("Running Code for {:?}", row);
                                match code_runner.execute_map(&fn_name, row.clone()) {
                                    Ok(result) => if result != 0 {
                                        rows.push(row);
                                    },
                                    Err(err) => {
                                        error!("Error while trying to index row: {}", err);
                                    }
                                }
                            },
                            Command::EndOfQuery => {
                                debug!("Received End of Query");
                                break
                            },
                            _ => {
                                panic!("Unexpected Code Reached");
                            }
                        }
                    }
                    match responder.send(Ok(rows)) {
                        Ok(()) => {},
                        Err(err) => {
                            error!("Failed to send rows: {:?}", err);
                        }
                    }
                },
                Command::EndOfQuery => panic!("Unexpected Code Reached: Command::EndOfQuery"),
                Command::QueryRow { row: _row } => panic!("Unexpected Code Reached: Command::QueryRow"),
            }
        }
    });
    all_workers.push(url_manager);

    web::web_handler(web_tx).await;
    futures::future::join_all(all_workers).await;
    Ok(())
}
