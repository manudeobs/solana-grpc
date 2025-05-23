

use {
    futures::{sink::SinkExt, stream::StreamExt}, std::{str::FromStr, time::Duration}, tonic::{metadata::AsciiMetadataValue, transport::Endpoint}, tonic_health::pb::health_client::HealthClient, yellowstone_grpc_client::{GeyserGrpcClient, InterceptorXToken}, yellowstone_grpc_proto::{
        geyser::{
            geyser_client::GeyserClient, subscribe_update::UpdateOneof, SubscribeRequest, SubscribeUpdateTransaction
        },
        prelude::SubscribeRequestPing,
    }
};

pub mod proto {
    pub use yellowstone_grpc_proto::geyser;
}

pub struct GrpcStreamManager {
    endpoint: String,
    client: GeyserGrpcClient<InterceptorXToken>,
    is_connected: bool,
    reconnect_attempts: u32,
    max_reconnect_attempts: u32,
    reconnect_interval: Duration,
    tx_handler: Box<dyn Fn(SubscribeUpdateTransaction, &str) + Send + Sync>
}

impl GrpcStreamManager {
    
    pub async fn new(endpoint: &str, x_token: Option<String>, tx_handler: Box<dyn Fn(SubscribeUpdateTransaction, &str) + Send + Sync>) -> Result<GrpcStreamManager, anyhow::Error> {
        let x_token = if let Some(token) = x_token {
            Some(AsciiMetadataValue::from_str(token.as_str())?)
        } else {
            None
        };

        let interceptor = InterceptorXToken {
            x_token: x_token,
            x_request_snapshot: true,
        };

        let channel = Endpoint::from_shared(endpoint.to_string())?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(10))
            .connect()
            .await
            .map_err(|e| anyhow::Error::from(e))?;

        let client = GeyserGrpcClient::new(
            HealthClient::with_interceptor(channel.clone(), interceptor.clone()),
            GeyserClient::with_interceptor(channel, interceptor),
        );

        Ok(GrpcStreamManager {
            endpoint: endpoint.to_string(),
            client,
            is_connected: false,
            reconnect_attempts: 0,
            max_reconnect_attempts: 10,
            reconnect_interval: Duration::from_secs(5),
            tx_handler: tx_handler
        })
    }

    /// Establishes connection and handles the subscription stream
    /// 
    /// # Arguments
    /// * `request` - The subscription request containing account filters and other parameters
    pub async fn connect(&mut self, request: SubscribeRequest) -> Result<(), anyhow::Error> {
        let request = request.clone();
        
        loop {
            let (mut subscribe_tx, mut stream) = self.client.subscribe_with_request(Some(request.clone())).await?;

            self.is_connected = true;
            self.reconnect_attempts = 0;

            while let Some(message) = stream.next().await {
            match message {
                Ok(msg) => {
                    match msg.update_oneof {
                        Some(UpdateOneof::Transaction(tx)) => {
                            self.tx_handler.as_ref()(tx, &self.endpoint);
                        }
                        Some(UpdateOneof::Ping(_)) => {
                            subscribe_tx
                                .send(SubscribeRequest {
                                    ping: Some(SubscribeRequestPing { id: 1 }),
                                    ..Default::default()
                                })
                                .await?;
                        }
                        Some(UpdateOneof::Pong(_)) => {} // Ignore pong responses
                        _ => {}
                    }
                },
                Err(err) => {
                    log::error!("Error: {:?}", err);
                    drop(subscribe_tx);
                    drop(stream);
                    self.is_connected = false;
                    self.reconnect(request.clone()).await?;
                    break;
                }
            }
        }}

    }

    /// Attempts to reconnect when the connection is lost
    /// 
    /// # Arguments
    /// * `request` - The original subscription request to reestablish the connection
    async fn reconnect(&mut self, request: SubscribeRequest) -> Result<(), anyhow::Error> {
        if self.reconnect_attempts >= self.max_reconnect_attempts {
            return Err(anyhow::anyhow!("Max reconnection attempts reached"));
        }

        self.reconnect_attempts += 1;

        let backoff = self.reconnect_interval * std::cmp::min(self.reconnect_attempts, 5);
        tokio::time::sleep(backoff).await;

        Box::pin(self.connect(request)).await
    }
}
