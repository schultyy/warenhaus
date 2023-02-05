use std::convert::Infallible;
use reqwest::StatusCode;
use warp::Filter;
use serde::{Serialize, Deserialize};
use thiserror::Error;
use tracing::{instrument, Level};
use tracing::debug;

#[derive(Debug, Deserialize)]
pub enum Value {
    Int(i64),
    Float(f64),
    String(String),
    Boolean(bool)
}

#[derive(Debug, Deserialize)]
struct IndexParams {
    fields: Vec<String>,
    values: Vec<Value>
}

#[derive(Debug, Serialize, Error)]
enum IndexParamError {
    #[error("Number of fields ({0}) does not match number of provided values ({1}).")]
    FieldValueLenMismatch(usize, usize)
}

impl IndexParams {
    #[instrument]
    pub fn validate(&self) -> Result<(), IndexParamError> {
        debug!("self.fields {} | self.values {}", self.fields.len(), self.values.len());
        if self.fields.len() != self.values.len() {
            return Err(IndexParamError::FieldValueLenMismatch(self.fields.len(), self.values.len()))
        }
        Ok(())
    }
}


#[tracing::instrument]
async fn index_handler(index_params: IndexParams) -> Result<impl warp::Reply, Infallible> {
    if let Err(err) = index_params.validate() {
        let json = warp::reply::json(&format!("{}", err));
        return Ok(warp::reply::with_status(json, StatusCode::UNPROCESSABLE_ENTITY))
    }
    let json = warp::reply::json(&"OK");
    Ok(warp::reply::with_status(json, StatusCode::OK))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();    

    let root = warp::path::end().map(|| "root");

    let index_data = warp::path!("index")
        .and(warp::post())
        .and(warp::body::json())
        .and_then(index_handler);

    let endpoints = warp::any().and(
        root.or(index_data)
    );

    warp::serve(endpoints).run(([127, 0, 0, 1], 3030)).await;
}
