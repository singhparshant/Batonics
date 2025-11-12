use std::env;

use anyhow::{Context, Result};

fn main() -> Result<()> {
    let db_url = env::var("DATABASE_URL")
        .context("DATABASE_URL env var must be set to initialize schema")?;
    batonics::storage::init_database(&db_url)?;
    println!("schema ensured for {}", db_url);
    Ok(())
}
