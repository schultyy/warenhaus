use crate::storage::Storage;
use tracing::error;
use tokio::sync::mpsc;

mod storage;
mod web;


#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let (manager_tx, mut rx) = mpsc::channel(8192);
    let web_tx = manager_tx.clone();
    let mut all_workers = vec![];
    let url_manager = tokio::spawn(async move {
        let mut storage_manager = Storage::new();
        while let Some(command) = rx.recv().await {
            println!("Received Index Payload: {:?}", command);
            match command {
                web::Command::Index { params, responder } => {
                    if let Err(err) = storage_manager.index(params) {
                        error!("{}", err);
                        if let Err(_) = responder.send(Err(err)) {
                            error!("Error while sending storage response");
                        }
                    }
                    else {
                        if responder.send(Ok(())).is_err() {
                            error!("Error while sending storage response");
                        }
                    }
                }
            }
        }
    });
    all_workers.push(url_manager);

    web::web_handler(web_tx).await;
    futures::future::join_all(all_workers).await;
}
