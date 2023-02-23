use tokio::sync::oneshot;

use crate::{
    query::wasm_error::WasmError,
    storage::{column::Cell, ContainerError},
    web::IndexParams,
};

pub type InsertResponder = oneshot::Sender<Result<(), ContainerError>>;
pub type InsertMapFnResponder = oneshot::Sender<Result<(), WasmError>>;
pub type ExecuteMapResponder = oneshot::Sender<Result<Vec<Vec<Cell>>, WasmError>>;

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
    InvokeMap {
        fn_name: String,
        responder: ExecuteMapResponder,
    },
    QueryRow { row: Vec<Cell> },
    EndOfQuery
}
