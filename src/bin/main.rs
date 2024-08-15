use std::{env, error::Error};
use bb8_libsql::LibsqlConnectionManager;

use dotenvy::dotenv;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();

    let url = env::var("LIBSQL_CLIENT_URL").unwrap();
    let token = env::var("LIBSQL_CLIENT_TOKEN").unwrap();

    let manager = LibsqlConnectionManager::remote(&url, &token);
    let pool = bb8::Pool::builder()
        .max_size(15)
        .build(manager)
        .await
        .unwrap();

    let conn = pool.get().await?;
    let mut rows = conn.query("SELECT 1;", ()).await?;

    let value_found = rows.next().await?
        .map(|row| row.get::<u64>(0))
        .transpose()?;

    dbg!(value_found);

    Ok(())
}
