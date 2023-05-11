use std::{path::{Path, PathBuf}, fs};

use crate::{storage::Container, query::code_runner::CodeRunner, command::Command};
use anyhow::Context;
use config::Configurator;

use tokio::sync::mpsc;
use tracing::{error, debug, instrument, info};

mod storage;
mod web;
mod config;
mod query;
mod command;

fn database_storage_root_path() -> PathBuf {
    let db_storage_base_path_str = std::env::var("DB_STORAGE_PATH").context("Missing DB_STORAGE_PATH environment variable").unwrap();
    let db_storage_path = Path::new(&db_storage_base_path_str).join("db");
    db_storage_path
}

fn compiled_map_fn_path() -> &'static str {
    "queries"
}

fn config_file_root_path() -> String {
    std::env::var("CONFIG_FILE_ROOT_PATH").context("Missing CONFIG_FILE_ROOT_PATH environment variable").unwrap()
}

#[instrument]
fn ensure_folders(root_path: &str) -> Result<(), std::io::Error> {
    let db_path = Path::new(root_path).join("db");
    if db_path.exists() {
        info!("{:?} exists - moving on", db_path);
        return Ok(());
    }

    info!("{:?} does not exist - creating directory", db_path);
    fs::create_dir(db_path)?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()>{
    tracing_subscriber::fmt::init();
    ctrlc::set_handler(move || {
        std::process::exit(0)
    })
        .expect("Error setting Ctrl-C handler");

    let database_storage_path = database_storage_root_path();

    let (manager_tx, mut rx) = mpsc::channel(8192);
    let web_tx = manager_tx.clone();
    let mut all_workers = vec![];

    ensure_folders(&config_file_root_path())?;

    let configurator = Configurator::new(&config_file_root_path());
    let config = configurator.load().context("Failed to load ./schema.json")?;
    let url_manager = tokio::spawn(async move {
        let mut storage_manager = Container::new(&database_storage_path, config).expect("failed to load container");
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

                    let (tx, mut rx) = mpsc::channel(10000);

                    storage_manager.query(tx).await;
                    debug!("Queried Storage Manager");

                    let mut rows = vec!();

                    while let Some(payload) = rx.recv().await {
                        debug!("Received Storage Manager Callback");
                        match payload {
                            Command::QueryRow { row } => {
                                debug!("Running Code for {:?}", row);
                                match code_runner.execute_map(&fn_name, row.clone()) {
                                    Ok(should_include_row) => if should_include_row {
                                        rows.push(row);
                                    },
                                    Err(err) => {
                                        error!("Error while trying to index row: {}", err);
                                    }
                                }
                            },
                            _ => {
                                panic!("Unexpected Code Reached: {:?}", payload);
                            }
                        }
                    }
                    debug!("Received all rows");
                    match responder.send(Ok(rows)) {
                        Ok(()) => {},
                        Err(err) => {
                            error!("Failed to send rows: {:?}", err);
                        }
                    }
                },
                Command::QueryRow { row: _row } => panic!("Unexpected Code Reached: Command::QueryRow"),
            }
        }
    });
    all_workers.push(url_manager);

    web::web_handler(web_tx).await;
    futures::future::join_all(all_workers).await;
    Ok(())
}
