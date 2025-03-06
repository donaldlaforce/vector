use redis::{aio::ConnectionManager, AsyncCommands, ErrorKind, RedisError, RedisResult};
use snafu::{ResultExt, Snafu};
use std::time::Duration;
use std::num::NonZeroUsize;

use super::{InputHandler, Method};
use crate::{internal_events::RedisReceiveEventError, sources::Source};

#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("Failed to create connection: {}", source))]
    Connection { source: RedisError },
}

impl InputHandler {
    pub(super) async fn watch(mut self, method: Method, batch_size: usize) -> crate::Result<Source> {
        let mut conn = self
            .client
            .get_connection_manager()
            .await
            .context(ConnectionSnafu {})?;

        Ok(Box::pin(async move {
            let mut shutdown = self.cx.shutdown.clone();
            let mut retry: u32 = 0;
            loop {
                let res = match method {
                    Method::Rpop => tokio::select! {
                        res = brpop(&mut conn, &self.key, batch_size) => res,
                        _ = &mut shutdown => break
                    },
                    Method::Lpop => tokio::select! {
                        res = blpop(&mut conn, &self.key, batch_size) => res,
                        _ = &mut shutdown => break
                    },
                };

                match res {
                    Err(error) => {
                        let err: RedisError = error;
                        let kind = err.kind();

                        emit!(RedisReceiveEventError::from(err));

                        if kind == ErrorKind::IoError {
                            retry += 1;
                            backoff_exponential(retry).await
                        }
                    }
                    Ok(lines) => {
                        if retry > 0 {
                            retry = 0
                        }
                        
                        // Process each line in the batch
                        for line in lines {
                            if let Err(()) = self.handle_line(line).await {
                                return Ok(());
                            }
                        }
                    }
                }
            }
            Ok(())
        }))
    }
}

async fn backoff_exponential(exp: u32) {
    let ms = if exp <= 4 { 2_u64.pow(exp + 5) } else { 1000 };
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

async fn brpop(conn: &mut ConnectionManager, key: &str, count: usize) -> RedisResult<Vec<String>> {
    if count <= 1 {
        // Use blocking operation for single item
        conn.brpop(key, 0.0)
            .await
            .map(|(_, value): (String, String)| vec![value])
    } else {
        // Use non-blocking operation with count for batching
        let non_zero_count = NonZeroUsize::new(count).unwrap();
        let result: RedisResult<Vec<String>> = conn.rpop(key, Some(non_zero_count)).await;
        match result {
            Ok(values) if !values.is_empty() => Ok(values),
            Ok(_) => {
                // If no values were returned, use blocking operation to wait for new items
                conn.brpop(key, 0.0)
                    .await
                    .map(|(_, value): (String, String)| vec![value])
            }
            Err(err) => Err(err),
        }
    }
}

async fn blpop(conn: &mut ConnectionManager, key: &str, count: usize) -> RedisResult<Vec<String>> {
    if count <= 1 {
        // Use blocking operation for single item
        conn.blpop(key, 0.0)
            .await
            .map(|(_, value): (String, String)| vec![value])
    } else {
        // Use non-blocking operation with count for batching
        let non_zero_count = NonZeroUsize::new(count).unwrap();
        let result: RedisResult<Vec<String>> = conn.lpop(key, Some(non_zero_count)).await;
        match result {
            Ok(values) if !values.is_empty() => Ok(values),
            Ok(_) => {
                // If no values were returned, use blocking operation to wait for new items
                conn.blpop(key, 0.0)
                    .await
                    .map(|(_, value): (String, String)| vec![value])
            }
            Err(err) => Err(err),
        }
    }
}
