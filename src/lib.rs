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
//! use std::env;
//! use std::error::Error;
//! use std::path::PathBuf;
//! use std::str::FromStr;
//! use std::time::Duration;
//! 
//! use dotenvy::dotenv;
//! 
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn Error>> {
//!     dotenv().ok();
//! 
//!     let url = env::var("LIBSQL_CLIENT_URL").unwrap();
//!     let token = env::var("LIBSQL_CLIENT_TOKEN").unwrap();
//!     let extension_dir = env::var("EXTENSION_DIR").unwrap();
//! 
//!     let manager = bb8_libsql::LibsqlConnectionManager::new_remote_replica(&PathBuf::from_str("sync.db")?, &url, &token)
//!         .sync_interval(&Duration::from_secs(60))
//!         .extensions(&vec![
//!             PathBuf::from_str(&format!("{}/crypto.dylib", extension_dir))?,
//!             PathBuf::from_str(&format!("{}/uuid.dylib", extension_dir))?,
//!         ])
//!         .clone();
//! 
//!     let pool = bb8::Pool::builder()
//!         .max_size(15)
//!         .build(manager)
//!         .await
//!         .unwrap();
//! 
//!     let conn = pool.get().await?;
//!     let value = conn.query("SELECT uuid4();", ()).await?
//!         .next().await?
//!         .expect("Row not found.")
//!         .get::<String>(0)?;
//! 
//!     dbg!(value);
//! 
//!     Ok(())
//! }
//! ```
use std::{path::PathBuf, time::Duration};

use async_trait::async_trait;
use libsql::Connection;

mod errors;

#[derive(Clone)]
pub struct Local { 
    path: PathBuf,
    extensions: Option<Vec<PathBuf>>,
}

#[derive(Clone)]
pub struct Remote { 
    url: String,
    token: String,
}

#[derive(Clone)]
pub struct LocalReplica { 
    path: PathBuf,
    extensions: Option<Vec<PathBuf>>,
}

#[derive(Clone)]
pub struct RemoteReplica {
    path: PathBuf,
    url: String,
    token: String,
    sync_interval: Option<Duration>,
    extensions: Option<Vec<PathBuf>>,
}

#[derive(Clone)]
/// A `bb8::ManageConnection` for `libsql::Connection`s.
pub struct LibsqlConnectionManager<T> {
    inner: T,
}

impl LibsqlConnectionManager<()> {
    /// Creates a new `LibsqlConnectionManager` from local file.
    /// See `libsql::Builder::new_local`
    pub fn new_local(path: &PathBuf) -> LibsqlConnectionManager<Local> {
        LibsqlConnectionManager {
            inner: Local { 
                path: path.clone(), 
                extensions: None
            }
        }
    }

    /// Creates a new `LibsqlConnectionManager` from remote connection.
    /// See `libsql::Builder::new_remote`
    pub fn new_remote(url: &str, token: &str) -> LibsqlConnectionManager<Remote> {
        LibsqlConnectionManager {
            inner: Remote { 
                url: url.to_string(), 
                token: token.to_string(),
            }
        }
    }

    /// Creates a new `LibsqlConnectionManager` from local replica.
    /// See `libsql::Builder::new_local_replica`
    pub fn new_local_replica(path: &PathBuf) -> LibsqlConnectionManager<LocalReplica> {
        LibsqlConnectionManager {
            inner: LocalReplica { 
                path: path.clone(),
                extensions: None,
            }
        }
    }


    /// Creates a new `LibsqlConnectionManager` from remote replica.
    /// See `libsql::Builder::new_remote_replica`
    pub fn new_remote_replica(path: &PathBuf, url: &str, token: &str) -> LibsqlConnectionManager<RemoteReplica> {
        LibsqlConnectionManager {
            inner: RemoteReplica {
                path: path.clone(), 
                url: url.to_string(), 
                token: token.to_string(),
                sync_interval: None,
                extensions: None,
            },
        }
    }
}

impl LibsqlConnectionManager<Local> {
    pub fn extensions(&mut self, extensions: &Vec<PathBuf>) -> &mut Self {
        self.inner.extensions = Some(extensions.clone());

        self
    }
}

impl LibsqlConnectionManager<LocalReplica> {
    pub fn extensions(&mut self, extensions: &Vec<PathBuf>) -> &mut Self {
        self.inner.extensions = Some(extensions.clone());

        self
    }
}

impl LibsqlConnectionManager<RemoteReplica> {
    pub fn sync_interval(&mut self, interval: &Duration) -> &mut Self {
        self.inner.sync_interval = Some(interval.clone());

        self
    }

    pub fn extensions(&mut self, extensions: &Vec<PathBuf>) -> &mut Self {
        self.inner.extensions = Some(extensions.clone());

        self
    }
}

#[async_trait]
impl bb8::ManageConnection for LibsqlConnectionManager<Local> {
    type Connection = Connection;
    type Error = errors::ConnectionManagerError;

    async fn connect(&self) -> Result<Connection, errors::ConnectionManagerError> {
        let builder = libsql::Builder::new_local(self.inner.path.clone());

        Ok(builder.build().await
            .and_then(|db| db.connect())
            .and_then(|conn| {
                let Some(ext) = self.inner.extensions.clone() else { return Ok(conn) };
                conn.load_extension_enable()?;
                for path in ext { conn.load_extension(path, None)?; }
                conn.load_extension_disable()?;

                Ok(conn)
            })?)
    }

    async fn is_valid(&self, conn: &mut Connection) -> Result<(), errors::ConnectionManagerError> {
        Ok(conn.execute_batch("SELECT 1;").await.map(|_| ())?)
    }

    fn has_broken(&self, _: &mut Connection) -> bool { false }
}

#[async_trait]
impl bb8::ManageConnection for LibsqlConnectionManager<Remote> {
    type Connection = Connection;
    type Error = errors::ConnectionManagerError;

    async fn connect(&self) -> Result<Connection, errors::ConnectionManagerError> {
        let builder = libsql::Builder::new_remote(self.inner.url.clone(), self.inner.token.clone());

        Ok(builder.build().await
            .and_then(|db| db.connect())?)
    }

    async fn is_valid(&self, conn: &mut Connection) -> Result<(), errors::ConnectionManagerError> {
        Ok(conn.execute_batch("SELECT 1;").await.map(|_| ())?)
    }

    fn has_broken(&self, _: &mut Connection) -> bool { false }
}

#[async_trait]
impl bb8::ManageConnection for LibsqlConnectionManager<LocalReplica> {
    type Connection = Connection;
    type Error = errors::ConnectionManagerError;

    async fn connect(&self) -> Result<Connection, errors::ConnectionManagerError> {
        let builder = libsql::Builder::new_local_replica(self.inner.path.clone());

        Ok(builder.build().await
            .and_then(|db| db.connect())
            .and_then(|conn| {
                let Some(ext) = self.inner.extensions.clone() else { return Ok(conn) };
                conn.load_extension_enable()?;
                for path in ext { conn.load_extension(path, None)?; }
                conn.load_extension_disable()?;

                Ok(conn)
            })?)
    }

    async fn is_valid(&self, conn: &mut Connection) -> Result<(), errors::ConnectionManagerError> {
        Ok(conn.execute_batch("SELECT 1;").await.map(|_| ())?)
    }

    fn has_broken(&self, _: &mut Connection) -> bool { false }
}

#[async_trait]
impl bb8::ManageConnection for LibsqlConnectionManager<RemoteReplica> {
    type Connection = Connection;
    type Error = errors::ConnectionManagerError;

    async fn connect(&self) -> Result<Connection, errors::ConnectionManagerError> {
        let mut builder = libsql::Builder::new_remote_replica(self.inner.path.clone(), self.inner.url.clone(), self.inner.token.clone());
        if let Some(interval) = self.inner.sync_interval {
            builder = builder.sync_interval(interval);
        }

        Ok(builder.build().await
            .and_then(|db| db.connect())
            .and_then(|conn| {
                let Some(ext) = self.inner.extensions.clone() else { return Ok(conn) };
                conn.load_extension_enable()?;
                for path in ext { conn.load_extension(path, None)?; }
                conn.load_extension_disable()?;

                Ok(conn)
            })?)
    }

    async fn is_valid(&self, conn: &mut Connection) -> Result<(), errors::ConnectionManagerError> {
        Ok(conn.execute_batch("SELECT 1;").await.map(|_| ())?)
    }

    fn has_broken(&self, _: &mut Connection) -> bool { false }
}
