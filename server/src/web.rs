use crate::storage::column::Cell;
use crate::storage::data_type::DataType;
use crate::storage::ContainerError;
use lang::WasmError;
use reqwest::StatusCode;
use serde::Deserialize;
use std::convert::Infallible;
use tracing::error;

use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use warp::Filter;

fn with_tx(
    tx: Sender<Command>,
) -> impl Filter<Extract = (Sender<Command>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || tx.clone())
}

#[derive(Debug, Deserialize, Clone)]
pub enum Value {
    Int(i64),
    Float(f64),
    String(String),
    Boolean(bool),
}

impl Into<Cell> for Value {
    fn into(self) -> Cell {
        match self {
            Value::Int(value) => Cell::Int(value),
            Value::Float(value) => Cell::Float(value),
            Value::String(value) => Cell::String(value),
            Value::Boolean(value) => Cell::Boolean(value),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(val) => write!(f, "i64 {}", val),
            Value::Float(val) => write!(f, "f64 {}", val),
            Value::String(val) => write!(f, "Str {}", val),
            Value::Boolean(val) => write!(f, "bool {}", val),
        }
    }
}

impl Into<DataType> for &Value {
    fn into(self) -> DataType {
        match self {
            Value::Int(_) => DataType::Int,
            Value::Float(_) => DataType::Float,
            Value::String(_) => DataType::String,
            Value::Boolean(_) => DataType::Boolean,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct IndexParams {
    pub fields: Vec<String>,
    pub values: Vec<Value>,
}

#[derive(Debug, Deserialize)]
pub struct MapFnParams {
    ///Map Fn Name
    pub name: String,
    ///AssemblyScript Source Code
    pub source_code: String,
}

type InsertResponder = oneshot::Sender<Result<(), ContainerError>>;
type InsertMapFnResponder = oneshot::Sender<Result<(), WasmError>>;

#[derive(Debug)]
pub enum Command {
    Index {
        params: IndexParams,
        responder: InsertResponder,
    },
    AddMapFn {
        fn_name: String,
        source_code: String,
        responder: InsertMapFnResponder,
    },
}

#[tracing::instrument]
async fn index_handler(
    tx: Sender<Command>,
    index_params: IndexParams,
) -> Result<impl warp::Reply, Infallible> {
    let (resp_tx, resp_rx) = oneshot::channel();

    if let Err(err) = tx
        .send(Command::Index {
            params: index_params,
            responder: resp_tx,
        })
        .await
    {
        error!("Error while trying to index data: {}", err);
        let json = warp::reply::json(&"Internal Server Error".to_string());
        return Ok(warp::reply::with_status(
            json,
            StatusCode::INTERNAL_SERVER_ERROR,
        ));
    }

    match resp_rx.await {
        Ok(result) => match result {
            Ok(()) => {
                let json = warp::reply::json(&"ok");
                Ok(warp::reply::with_status(json, StatusCode::OK))
            }
            Err(err) => {
                let json = warp::reply::json(&format!("{}", err));
                Ok(warp::reply::with_status(
                    json,
                    StatusCode::UNPROCESSABLE_ENTITY,
                ))
            }
        },
        Err(err) => {
            error!(
                "Failed to receive answer from storage layer after save: {}",
                err
            );
            let json = warp::reply::json(&"Internal Server Error".to_string());
            return Ok(warp::reply::with_status(
                json,
                StatusCode::INTERNAL_SERVER_ERROR,
            ));
        }
    }
}

#[tracing::instrument]
async fn add_map_function(
    tx: Sender<Command>,
    map_fn_params: MapFnParams,
) -> Result<impl warp::Reply, Infallible> {
    let (resp_tx, resp_rx) = oneshot::channel();

    if let Err(err) = tx
        .send(Command::AddMapFn {
            fn_name: map_fn_params.name.to_string(),
            source_code: map_fn_params.source_code.to_string(),
            responder: resp_tx,
        })
        .await
    {
        error!(
            "Error while trying to add map function {}: {}",
            map_fn_params.name, err
        );
        let json = warp::reply::json(&"Internal Server Error".to_string());
        return Ok(warp::reply::with_status(
            json,
            StatusCode::INTERNAL_SERVER_ERROR,
        ));
    }

    match resp_rx.await {
        Ok(code_result) => match code_result {
            Ok(()) => {
                let json = warp::reply::json(&"ok");
                Ok(warp::reply::with_status(json, StatusCode::OK))
            }
            Err(err) => {
                error!("Tried to save and compile code. Received {}", err);
                if let WasmError::CompilerNotFound = err {
                    let json = warp::reply::json(&"Internal Server Error");
                    Ok(warp::reply::with_status(
                        json,
                        StatusCode::INTERNAL_SERVER_ERROR,
                    ))
                } else {
                    let json = warp::reply::json(&format!("{}", err));
                    Ok(warp::reply::with_status(
                        json,
                        StatusCode::UNPROCESSABLE_ENTITY,
                    ))
                }
            }
        },
        Err(err) => {
            error!(
                "Failed to receive answer from storage layer after save: {}",
                err
            );
            let json = warp::reply::json(&"internal server error".to_string());
            return Ok(warp::reply::with_status(
                json,
                StatusCode::INTERNAL_SERVER_ERROR,
            ));
        }
    }
}

pub async fn web_handler(tx: Sender<Command>) {
    let root = warp::path::end().map(|| "root");

    let index_data = warp::path!("index")
        .and(with_tx(tx.clone()))
        .and(warp::post())
        .and(warp::body::json())
        .and_then(index_handler);

    let add_map_fn = warp::path!("add_map")
        .and(with_tx(tx.clone()))
        .and(warp::post())
        .and(warp::body::json())
        .and_then(add_map_function);

    let endpoints = warp::any().and(root.or(index_data).or(add_map_fn));

    warp::serve(endpoints).run(([127, 0, 0, 1], 3030)).await;
}
