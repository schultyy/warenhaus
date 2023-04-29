use crate::{command::Command, storage::cell::Cell};
use crate::query::wasm_error::WasmError;
use bytes::BufMut;
use futures::TryStreamExt;
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::sync::oneshot;
use std::{convert::Infallible, collections::HashMap};
use tracing::error;
use warp::multipart::{FormData, Part};

use tokio::sync::mpsc::Sender;
use warp::{Filter, Rejection};

fn with_tx(
    tx: Sender<Command>,
) -> impl Filter<Extract = (Sender<Command>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || tx.clone())
}

#[derive(Debug, Deserialize)]
pub struct IndexParams {
    pub fields: Vec<String>,
    pub values: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct MapFnParams {
    ///Map Fn Name
    pub name: String,
    ///AssemblyScript Source Code
    pub source_code: String,
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
    fn_name: String,
    form: FormData,
    tx: Sender<Command>,
) -> Result<impl warp::Reply, Rejection> {
    let (resp_tx, resp_rx) = oneshot::channel();

    let parts: Vec<Part> = form.try_collect().await.map_err(|e| {
        error!("form error: {}", e);
        warp::reject::reject()
    })?;

    let file_part = parts.into_iter().find(|p| p.name() == "data").unwrap();

    let content_type = file_part.content_type().unwrap_or("N/A");

    if content_type != "application/octet-stream" {
        error!("invalid file type found: {}", content_type);
        return Err(warp::reject::reject());
    }

    let value = file_part
        .stream()
        .try_fold(Vec::new(), |mut vec, data| {
            vec.put(data);
            async move { Ok(vec) }
        })
        .await
        .map_err(|e| {
            error!("reading file error: {}", e);
            warp::reject::reject()
        })?;

    let file_content = String::from_utf8_lossy(&value);

    if let Err(err) = tx
        .send(Command::AddMapFn {
            fn_name: fn_name.to_string(),
            source_code: file_content.to_string(),
            responder: resp_tx,
        })
        .await
    {
        error!(
            "Error while trying to add map function {}: {}",
            fn_name, err
        );
        let json = warp::reply::json(&"Internal Server Error".to_string());
        return Ok(warp::reply::with_status(
            json,
            StatusCode::INTERNAL_SERVER_ERROR,
        ));
    }

    match resp_rx.await {
        Ok(add_map_fn_result) => match add_map_fn_result {
            Ok(()) => {
                let json = warp::reply::json(&"Created");
                Ok(warp::reply::with_status(json, StatusCode::CREATED))
            }
            Err(err) => {
                error!(
                    "Error while trying to compile and save new map function {}: {}",
                    fn_name, err
                );

                match err {
                    WasmError::InvalidCode => {
                        let json = warp::reply::json(&"Invalid Code".to_string());
                        return Ok(warp::reply::with_status(
                            json,
                            StatusCode::UNPROCESSABLE_ENTITY,
                        ));
                    }
                    WasmError::CompilerError(err) => {
                        let err_message = format!("Failed to compile code:\n{}", err);
                        let json = warp::reply::json(&err_message);
                        return Ok(warp::reply::with_status(
                            json,
                            StatusCode::UNPROCESSABLE_ENTITY,
                        ));
                    }
                    _ => {
                        let json = warp::reply::json(&"Internal Server Error".to_string());
                        return Ok(warp::reply::with_status(
                            json,
                            StatusCode::INTERNAL_SERVER_ERROR,
                        ));
                    }
                }
            }
        },
        Err(err) => {
            error!(
                "Error while trying to receive map function result {}: {}",
                fn_name, err
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
async fn execute_map_fn(
    fn_name: String,
    tx: Sender<Command>,
) -> Result<impl warp::Reply, Infallible> {
    let (resp_tx, resp_rx) = oneshot::channel();

    if let Err(err) = tx
        .send(Command::InvokeMap {
            fn_name: fn_name.to_string(),
            responder: resp_tx,
        })
        .await
    {
        error!(
            "Error while trying to execute map function {}: {}",
            fn_name, err
        );
        let json = warp::reply::json(&"Internal Server Error".to_string());
        return Ok(warp::reply::with_status(
            json,
            StatusCode::INTERNAL_SERVER_ERROR,
        ));
    }

    match resp_rx.await {
        Ok(execution_result) => match execution_result {
            Ok(rows) => {
                // TODO: Convert column frames into something that's easy to print
                // and readable
                let rows : Vec<HashMap<String, Cell>> = rows.iter().map(|r| r.to_view_object()).collect();
                let json = warp::reply::json(&rows);
                return Ok(warp::reply::with_status(json, StatusCode::OK));
            }
            Err(wasm_err) => {
                error!("Failed to execute query: {}", wasm_err);
                let json = warp::reply::json(&"Internal Server Error".to_string());
                return Ok(warp::reply::with_status(
                    json,
                    StatusCode::INTERNAL_SERVER_ERROR,
                ));
            }
        },
        Err(recv_err) => {
            error!("Failed to receive execution result: {}", recv_err);
            let json = warp::reply::json(&"Internal Server Error".to_string());
            return Ok(warp::reply::with_status(
                json,
                StatusCode::INTERNAL_SERVER_ERROR,
            ));
        }
    }
}

#[tracing::instrument]
pub async fn web_handler(tx: Sender<Command>) {
    let root = warp::path::end().map(|| "root");
    let log = warp::log("warenhaus");
    let index_data = warp::path!("index")
        .and(with_tx(tx.clone()))
        .and(warp::post())
        .and(warp::body::json())
        .and_then(index_handler);

    let add_map_fn = warp::path!("add_map" / String)
        .and(warp::multipart::form().max_length(5_000_000))
        .and(with_tx(tx.clone()))
        .and(warp::post())
        .and_then(add_map_function);

    let execute_map_fn_handler = warp::path!("query" / String)
        .and(warp::get())
        .and(with_tx(tx.clone()))
        .and_then(execute_map_fn);

    let endpoints = warp::any()
        .and(
            root.or(add_map_fn)
                .or(index_data)
                .or(execute_map_fn_handler),
        )
        .with(log);

    warp::serve(endpoints).run(([0, 0, 0, 0], 3030)).await;
}
