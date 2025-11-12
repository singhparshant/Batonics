use std::{net::SocketAddr, sync::Arc, thread};

use anyhow::{Context, Result};
use arc_swap::ArcSwapOption;
use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};

use crate::snapshot::SnapshotRecord;

#[derive(Clone)]
pub struct ServerConfig {
    pub addr: SocketAddr,
}

#[derive(Clone)]
struct AppState {
    latest: Arc<ArcSwapOption<SnapshotRecord>>,
}

pub fn spawn_http_server(
    state: Arc<ArcSwapOption<SnapshotRecord>>,
    config: ServerConfig,
) -> thread::JoinHandle<Result<()>> {
    thread::spawn(move || blocking_server(state, config))
}

fn blocking_server(latest: Arc<ArcSwapOption<SnapshotRecord>>, config: ServerConfig) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime for http server")?;
    runtime.block_on(async move {
        let app_state = AppState { latest };
        let router = Router::new()
            .route("/healthz", get(health))
            .route("/snapshot", get(snapshot))
            .with_state(app_state);

        let listener = tokio::net::TcpListener::bind(config.addr)
            .await
            .with_context(|| format!("failed to bind server to {}", config.addr))?;

        println!("server_ready addr={}", config.addr);

        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await
            .context("http server terminated unexpectedly")
    })
}

async fn health() -> impl IntoResponse {
    StatusCode::OK
}

async fn snapshot(State(state): State<AppState>) -> impl IntoResponse {
    match state.latest.load_full() {
        Some(snapshot) => match snapshot.to_json() {
            Ok(json) => Json(json).into_response(),
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        },
        None => StatusCode::NO_CONTENT.into_response(),
    }
}
