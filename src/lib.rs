#![deny(warnings)]
//! # Sqlite support for the `bb8` connection pool.
//!
//! Library crate: [bb8-libsql]()
//!
//! Integrated with: [bb8](https://crates.io/crates/bb8)
//! and [libsql](https://crates.io/crates/libsql)
//!
//! ## Example
//!
//! ```rust,no_run
//! use std::{env, error::Error};
//! use r2d2_libsql::LibsqlConnectionManager;
//!  
//! use dotenvy::dotenv;
//!  
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn Error>> {
//!     dotenv().ok();
//!  
//!     let url = env::var("LIBSQL_CLIENT_URL").unwrap();
//!     let token = env::var("LIBSQL_CLIENT_TOKEN").unwrap();
//!  
//!     let manager = LibsqlConnectionManager::remote(&url, &token);
//!     let pool = bb8::Pool::builder()
//!         .max_size(15)
//!         .build(manager)
//!         .await
//!         .unwrap();
//!  
//!     let conn = pool.get().await?;
//!     let mut rows = conn.query("SELECT 1;", ()).await?;
//!  
//!     let value_found = rows.next().await?
//!         .map(|row| row.get::<u64>(0))
//!         .transpose()?;
//!  
//!     dbg!(value_found);
//!  
//!     Ok(())
//! }
//! ```
pub use libsql;
use async_trait::async_trait;
use libsql::Connection;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub mod errors;

#[derive(Debug, Clone)]
enum Source {
    Local(PathBuf),
    Remote(String, String),
    LocalReplica(PathBuf),
    RemoteReplica(PathBuf, String, String, Duration),
}

/// An `bb8::ManageConnection` for `libsql::Connection`s.
pub struct LibsqlConnectionManager { source: Source }

impl fmt::Debug for LibsqlConnectionManager {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut builder = f.debug_struct("LibsqlConnectionManager");
        let _ = builder.field("source", &self.source);
        builder.finish()
    }
}

impl LibsqlConnectionManager {
    /// Creates a new `LibsqlConnectionManager` from local file.
    /// See `libsql::Builder::new_local`
    pub fn local<P: AsRef<Path>>(path: P) -> Self {
        Self {
            source: Source::Local(
                path.as_ref().to_path_buf()
            ),
        }
    }

    /// Creates a new `LibsqlConnectionManager` from remote.
    /// See `libsql::Builder::new_remote`
    pub fn remote(url: &str, token: &str) -> Self {
        Self {
            source: Source::Remote(
                url.to_string(), 
                token.to_string()
            ),
        }
    }

    /// Creates a new `LibsqlConnectionManager` from local replica.
    /// See `libsql::Builder::new_local_replica`
    pub fn local_replica<P: AsRef<Path>>(path: P) -> Self {
        Self {
            source: Source::LocalReplica(
                path.as_ref().to_path_buf(),
            ),
        }
    }

    /// Creates a new `LibsqlConnectionManager` from remote replica.
    /// See `libsql::Builder::new_remote_replica`
    pub fn remote_replica<P: AsRef<Path>>(path: P, url: &str, token: &str, sync_interval: Duration) -> Self {
        Self {
            source: Source::RemoteReplica(
                path.as_ref().to_path_buf(),
                url.to_string(),
                token.to_string(),
                sync_interval
            ),
        }
    }
}

#[async_trait]
impl bb8::ManageConnection for LibsqlConnectionManager {
    type Connection = Connection;
    type Error = errors::ConnectionManagerError;

    async fn connect(&self) -> Result<Connection, errors::ConnectionManagerError> {
        Ok(match &self.source {
            Source::Local(ref path) => {
                libsql::Builder::new_local(path)
                    .build().await
                    .and_then(|builder| builder.connect())
            },
            Source::Remote(url, token) => {
                libsql::Builder::new_remote(url.to_string(), token.to_string())
                    .build().await
                    .and_then(|builder| builder.connect())
            },
            Source::LocalReplica(path) => {
                libsql::Builder::new_local_replica(path)
                    .build().await
                    .and_then(|builder| builder.connect())
            },
            Source::RemoteReplica(path, url, token, sync_interval) => {
                libsql::Builder::new_remote_replica(path, url.to_string(), token.to_string())
                    .sync_interval(sync_interval.clone())
                    .build().await
                    .and_then(|builder| builder.connect())
            },
        }?)
    }

    async fn is_valid(&self, conn: &mut Connection) -> Result<(), errors::ConnectionManagerError> {
        Ok(conn.execute_batch("SELECT 1;").await.map(|_| ())?)
    }

    fn has_broken(&self, _: &mut Connection) -> bool { false }
}
