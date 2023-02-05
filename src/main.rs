use std::convert::Infallible;
use reqwest::StatusCode;
use warp::Filter;
use serde::Deserialize;

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


async fn index_handler(index_params: IndexParams) -> Result<impl warp::Reply, Infallible> {
    println!("{:?}", index_params);
    let json = warp::reply::json(&"OK");
    Ok(warp::reply::with_status(json, StatusCode::OK))
}

#[tokio::main]
async fn main() {
    // GET /hello/warp => 200 OK with body "Hello, warp!"
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
