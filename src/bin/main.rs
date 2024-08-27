use std::env;
use std::error::Error;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use dotenvy::dotenv;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();

    let url = env::var("LIBSQL_CLIENT_URL").unwrap();
    let token = env::var("LIBSQL_CLIENT_TOKEN").unwrap();
    let extension_dir = env::var("EXTENSION_DIR").unwrap();

    let manager = bb8_libsql::LibsqlConnectionManager::new_remote_replica(&PathBuf::from_str("sync.db")?, &url, &token)
        .sync_interval(&Duration::from_secs(60))
        .extensions(&vec![
            PathBuf::from_str(&format!("{}/crypto.dylib", extension_dir))?,
            PathBuf::from_str(&format!("{}/uuid.dylib", extension_dir))?,
        ])
        .clone();

    let pool = bb8::Pool::builder()
        .max_size(15)
        .build(manager)
        .await
        .unwrap();

    let conn = pool.get().await?;
    let value = conn.query("SELECT uuid4();", ()).await?
        .next().await?
        .expect("Row not found.")
        .get::<String>(0)?;

    dbg!(value);

    Ok(())
}
