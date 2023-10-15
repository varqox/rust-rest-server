use async_trait::async_trait;
use axum::{
    extract, extract::State, http::StatusCode, response, response::IntoResponse, routing, Router,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;

#[derive(Parser)]
struct CmdArgs {
    #[arg(long, default_value = "127.0.0.1:8080")]
    address: String,
    #[arg(long)]
    cache_dir: Option<String>,
}

#[tokio::main]
async fn main() {
    let cmd_args = CmdArgs::parse();

    let app_state = AppState {
        cache: match cmd_args.cache_dir {
            Some(path) => {
                tokio::fs::create_dir_all(&path).await.unwrap();
                Box::new(DiskCache::new(PathBuf::from(path)))
            }
            None => Box::new(MemCache::new()),
        },
    };

    println!("Starting to listen on http://{}", cmd_args.address);
    axum::Server::bind(&cmd_args.address.parse().unwrap())
        .serve(app(app_state))
        .await
        .unwrap();
}

struct AppState {
    cache: Box<dyn Cache + Send + Sync>,
}

// As a function to facilitate testing
fn app(app_state: AppState) -> axum::routing::IntoMakeService<Router> {
    Router::new()
        .route("/add", routing::put(add))
        .route("/delete", routing::delete(delete))
        .route("/get", routing::get(get))
        .route("/list", routing::get(list))
        .route("/modify", routing::patch(modify))
        .with_state(Arc::new(RwLock::new(app_state)))
        .into_make_service()
}

// Allow more than one implementation of the Cache
#[async_trait]
trait Cache {
    async fn list(&self) -> Value;

    async fn add(&mut self, key: String, value: String);

    // Returns true if the entry was deleted, false if there is no entry
    async fn delete(&mut self, key: &String) -> bool;

    // Returns true if the entry was modified, false if there is no entry
    async fn modify(&mut self, key: String, value: String) -> bool;

    async fn get(&self, key: &String) -> Option<String>;
}

struct MemCache {
    cache: HashMap<String, String>,
}

// In memory cache - the simplest
impl MemCache {
    fn new() -> Self {
        MemCache {
            cache: HashMap::new(),
        }
    }
}

#[async_trait]
impl Cache for MemCache {
    async fn list(&self) -> Value {
        let map = serde_json::Map::from_iter(
            self.cache
                .iter()
                .map(|(k, v)| (k.clone(), Value::String(v.clone()))),
        );
        Value::Object(map)
    }

    async fn add(&mut self, key: String, value: String) {
        self.cache.insert(key, value);
    }

    async fn delete(&mut self, key: &String) -> bool {
        self.cache.remove(key).is_some()
    }

    async fn modify(&mut self, key: String, value: String) -> bool {
        let entry = self.cache.entry(key);
        match entry {
            std::collections::hash_map::Entry::Occupied(mut o) => {
                o.insert(value);
                true
            }
            std::collections::hash_map::Entry::Vacant(_) => false,
        }
    }

    async fn get(&self, key: &String) -> Option<String> {
        self.cache.get(key).cloned()
    }
}

// On disk cache - a little trickier than in memory cache
struct DiskCache {
    cache_dir: PathBuf,
}

impl DiskCache {
    fn new(cache_dir: PathBuf) -> Self {
        DiskCache { cache_dir }
    }

    fn key_to_filename(key: &String) -> String {
        blake3::hash(key.as_bytes()).to_hex().as_str().to_string()
    }

    fn key_to_path(&self, key: &String) -> PathBuf {
        self.cache_dir.join(Self::key_to_filename(&key))
    }

    fn serialize(entry: &DiskCacheEntry) -> String {
        serde_json::to_string(entry).unwrap()
    }

    fn deserialize(entry: &[u8]) -> DiskCacheEntry {
        serde_json::from_slice(entry).unwrap()
    }
}

#[derive(Serialize, Deserialize)]
struct DiskCacheEntry {
    key: String,
    value: String,
}

#[async_trait]
impl Cache for DiskCache {
    async fn list(&self) -> Value {
        let mut entries = tokio::fs::read_dir(&self.cache_dir).await.unwrap();
        let mut vec = vec![];
        while let Some(entry) = entries.next_entry().await.unwrap() {
            let file_name = entry.file_name();
            if file_name.len() == blake3::OUT_LEN * 2 {
                let mut contents = vec![];
                File::open(self.cache_dir.join(file_name))
                    .await
                    .unwrap()
                    .read_to_end(&mut contents)
                    .await
                    .unwrap();
                let entry = Self::deserialize(&contents);
                vec.push((entry.key, Value::String(entry.value)));
            }
        }
        let map = serde_json::Map::from_iter(vec.into_iter());
        Value::Object(map)
    }

    async fn add(&mut self, key: String, value: String) {
        let filename = Self::key_to_filename(&key);
        let file_path = self.cache_dir.join(&filename);
        let tmp_filename = filename + ".new";
        let tmp_file_path = self.cache_dir.join(tmp_filename);
        let contents = Self::serialize(&DiskCacheEntry { key, value });
        // Save data
        let mut file = File::create(&tmp_file_path).await.unwrap();
        file.write_all(contents.as_bytes()).await.unwrap();
        // Make changes to disk durable
        file.sync_all().await.unwrap();
        tokio::fs::rename(tmp_file_path, file_path).await.unwrap();
        File::open(&self.cache_dir)
            .await
            .unwrap()
            .sync_data() // make rename durable
            .await
            .unwrap();
    }

    async fn delete(&mut self, key: &String) -> bool {
        match tokio::fs::remove_file(self.key_to_path(key)).await {
            Ok(()) => {
                File::open(&self.cache_dir)
                    .await
                    .unwrap()
                    .sync_data() // make deletion durable
                    .await
                    .unwrap();
                true
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
            Err(err) => panic!("{:?}", err),
        }
    }

    async fn modify(&mut self, key: String, value: String) -> bool {
        if tokio::fs::try_exists(self.key_to_path(&key)).await.unwrap() {
            self.add(key, value).await;
            true
        } else {
            false
        }
    }

    async fn get(&self, key: &String) -> Option<String> {
        match File::open(self.key_to_path(key)).await {
            Ok(mut file) => {
                let mut contents = vec![];
                file.read_to_end(&mut contents).await.unwrap();
                let entry = Self::deserialize(&contents);
                Some(entry.value)
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => panic!("{:?}", err),
        }
    }
}

async fn list(State(state): State<Arc<RwLock<AppState>>>) -> response::Json<Value> {
    response::Json(state.read().await.cache.list().await)
}

#[derive(Debug, Serialize, Deserialize)]
struct AddPayload {
    key: String,
    value: String,
}

async fn add(
    State(state): State<Arc<RwLock<AppState>>>,
    extract::Json(payload): extract::Json<AddPayload>,
) -> impl IntoResponse {
    state
        .write()
        .await
        .cache
        .add(payload.key, payload.value)
        .await;
    StatusCode::CREATED
}

#[derive(Debug, Serialize, Deserialize)]
struct DeletePayload {
    key: String,
}

async fn delete(
    State(state): State<Arc<RwLock<AppState>>>,
    extract::Json(payload): extract::Json<DeletePayload>,
) -> impl IntoResponse {
    if state.write().await.cache.delete(&payload.key).await {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ModifyPayload {
    key: String,
    value: String,
}

async fn modify(
    State(state): State<Arc<RwLock<AppState>>>,
    extract::Json(payload): extract::Json<ModifyPayload>,
) -> impl IntoResponse {
    if state
        .write()
        .await
        .cache
        .modify(payload.key, payload.value)
        .await
    {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct GetPayload {
    key: String,
}

async fn get(
    State(state): State<Arc<RwLock<AppState>>>,
    extract::Json(payload): extract::Json<GetPayload>,
) -> impl IntoResponse {
    match state.read().await.cache.get(&payload.key).await {
        Some(val) => (StatusCode::OK, val.clone()),
        None => (StatusCode::NOT_FOUND, String::new()),
    }
}

#[cfg(test)]
mod app_tests {
    use super::*;
    use axum::http::StatusCode;
    use axum_test::TestServer;
    use tmpdir::TmpDir;

    struct Apps {
        _tmp_dir: TmpDir, // guards temporary directory and removes it after testing
        apps: [axum::routing::IntoMakeService<Router>; 2],
    }

    impl Apps {
        async fn new() -> Self {
            let tmp_dir = TmpDir::new("rest_server").await.unwrap();
            let tmp_dir_path = tmp_dir.to_path_buf();
            Self {
                _tmp_dir: tmp_dir,
                apps: [
                    app(AppState {
                        cache: Box::new(MemCache::new()),
                    }),
                    app(AppState {
                        cache: Box::new(DiskCache::new(tmp_dir_path)),
                    }),
                ],
            }
        }
    }

    #[tokio::test]
    async fn empty_list() {
        for app in Apps::new().await.apps {
            let server = TestServer::new(app).unwrap();

            let response = server.get("/list").await;
            assert_eq!(response.status_code(), StatusCode::OK);
            assert_eq!(response.text(), "{}");
        }
    }

    #[tokio::test]
    async fn add() {
        for app in Apps::new().await.apps {
            let server = TestServer::new(app).unwrap();

            let request = server.put("/add").json(&AddPayload {
                key: "some key".to_string(),
                value: "a value".to_string(),
            });
            assert_eq!(request.await.status_code(), StatusCode::CREATED);

            let response = server.get("/list").await;
            assert_eq!(response.status_code(), StatusCode::OK);
            assert_eq!(response.text(), r#"{"some key":"a value"}"#);
        }
    }

    #[tokio::test]
    async fn add_two_values() {
        for app in Apps::new().await.apps {
            let server = TestServer::new(app).unwrap();

            let request = server.put("/add").json(&AddPayload {
                key: "a".to_string(),
                value: "x".to_string(),
            });
            assert_eq!(request.await.status_code(), StatusCode::CREATED);

            let request = server.put("/add").json(&AddPayload {
                key: "b".to_string(),
                value: "y".to_string(),
            });
            assert_eq!(request.await.status_code(), StatusCode::CREATED);

            let response = server.get("/list").await;
            assert_eq!(response.status_code(), StatusCode::OK);
            assert_eq!(response.text(), r#"{"a":"x","b":"y"}"#);
        }
    }

    #[tokio::test]
    async fn add_overrides_previous_entry() {
        for app in Apps::new().await.apps {
            let server = TestServer::new(app).unwrap();

            let request = server.put("/add").json(&AddPayload {
                key: "some key".to_string(),
                value: "a value".to_string(),
            });
            assert_eq!(request.await.status_code(), StatusCode::CREATED);

            let request = server.put("/add").json(&AddPayload {
                key: "some key".to_string(),
                value: "another value".to_string(),
            });
            assert_eq!(request.await.status_code(), StatusCode::CREATED);

            let response = server.get("/list").await;
            assert_eq!(response.status_code(), StatusCode::OK);
            assert_eq!(response.text(), r#"{"some key":"another value"}"#);
        }
    }

    #[tokio::test]
    async fn deleting_nonexistent_entry() {
        for app in Apps::new().await.apps {
            let server = TestServer::new(app).unwrap();

            let request = server.delete("/delete").json(&DeletePayload {
                key: "some key".to_string(),
            });
            assert_eq!(request.await.status_code(), StatusCode::NOT_FOUND);
        }
    }

    #[tokio::test]
    async fn delete() {
        for app in Apps::new().await.apps {
            let server = TestServer::new(app).unwrap();

            let request = server.put("/add").json(&AddPayload {
                key: "some key".to_string(),
                value: "a value".to_string(),
            });
            assert_eq!(request.await.status_code(), StatusCode::CREATED);

            let request = server.delete("/delete").json(&DeletePayload {
                key: "some key".to_string(),
            });
            assert_eq!(request.await.status_code(), StatusCode::NO_CONTENT);

            let response = server.get("/list").await;
            assert_eq!(response.status_code(), StatusCode::OK);
            assert_eq!(response.text(), "{}");
        }
    }

    #[tokio::test]
    async fn modifying_nonexistent_entry() {
        for app in Apps::new().await.apps {
            let server = TestServer::new(app).unwrap();

            let request = server.patch("/modify").json(&ModifyPayload {
                key: "some key".to_string(),
                value: "a value".to_string(),
            });
            assert_eq!(request.await.status_code(), StatusCode::NOT_FOUND);
        }
    }

    #[tokio::test]
    async fn modify() {
        for app in Apps::new().await.apps {
            let server = TestServer::new(app).unwrap();

            let request = server.put("/add").json(&AddPayload {
                key: "some key".to_string(),
                value: "a value".to_string(),
            });
            assert_eq!(request.await.status_code(), StatusCode::CREATED);

            let request = server.patch("/modify").json(&ModifyPayload {
                key: "some key".to_string(),
                value: "another value".to_string(),
            });
            assert_eq!(request.await.status_code(), StatusCode::NO_CONTENT);

            let response = server.get("/list").await;
            assert_eq!(response.status_code(), StatusCode::OK);
            assert_eq!(response.text(), r#"{"some key":"another value"}"#);
        }
    }

    #[tokio::test]
    async fn get_nonexistent_entry() {
        for app in Apps::new().await.apps {
            let server = TestServer::new(app).unwrap();

            let request = server.get("/get").json(&GetPayload {
                key: "some key".to_string(),
            });
            assert_eq!(request.await.status_code(), StatusCode::NOT_FOUND);
        }
    }

    #[tokio::test]
    async fn get() {
        for app in Apps::new().await.apps {
            let server = TestServer::new(app).unwrap();

            let request = server.put("/add").json(&AddPayload {
                key: "some key".to_string(),
                value: "a value".to_string(),
            });
            assert_eq!(request.await.status_code(), StatusCode::CREATED);

            let request = server.get("/get").json(&GetPayload {
                key: "some key".to_string(),
            });
            let response = request.await;
            assert_eq!(response.status_code(), StatusCode::OK);
            assert_eq!(response.text(), "a value");
        }
    }
}
