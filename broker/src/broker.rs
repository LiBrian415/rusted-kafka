#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Void {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TopicPartition {
    #[prost(string, tag = "1")]
    pub topic: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub partition: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaderAndIsr {
    #[prost(uint32, tag = "1")]
    pub leader: u32,
    #[prost(uint32, repeated, tag = "2")]
    pub isr: ::prost::alloc::vec::Vec<u32>,
    #[prost(uint64, tag = "3")]
    pub leader_epoch: u64,
    #[prost(uint64, tag = "4")]
    pub controller_epoch: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TopicPartitionLeaderInput {
    #[prost(message, optional, tag = "1")]
    pub topic_partition: ::core::option::Option<TopicPartition>,
    #[prost(message, optional, tag = "2")]
    pub leader_and_isr: ::core::option::Option<LeaderAndIsr>,
}
/// Data structure for create topic/partitions
/// topic - topic
/// partitions - number of partitions
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TopicPartitions {
    #[prost(string, tag = "1")]
    pub topic: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub partitions: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateInput {
    #[prost(message, repeated, tag = "1")]
    pub topic_partitions: ::prost::alloc::vec::Vec<TopicPartitions>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProducerInput {
    #[prost(string, tag = "1")]
    pub topic: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub partition: u32,
    #[prost(bytes = "vec", tag = "3")]
    pub messages: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConsumerInput {
    #[prost(string, tag = "1")]
    pub topic: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub partition: u32,
    #[prost(uint64, tag = "3")]
    pub offset: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConsumerOutput {
    #[prost(bytes = "vec", tag = "1")]
    pub messages: ::prost::alloc::vec::Vec<u8>,
    #[prost(bool, tag = "2")]
    pub end: bool,
}
#[doc = r" Generated client implementations."]
pub mod broker_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct BrokerClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl BrokerClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> BrokerClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> BrokerClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            BrokerClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn topic_partition_leader(
            &mut self,
            request: impl tonic::IntoRequest<super::TopicPartitionLeaderInput>,
        ) -> Result<tonic::Response<super::Void>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/broker.Broker/topic_partition_leader");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn create(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateInput>,
        ) -> Result<tonic::Response<super::Void>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/broker.Broker/create");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn produce(
            &mut self,
            request: impl tonic::IntoRequest<super::ProducerInput>,
        ) -> Result<tonic::Response<super::Void>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/broker.Broker/produce");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn consume(
            &mut self,
            request: impl tonic::IntoRequest<super::ConsumerInput>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<super::ConsumerOutput>>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/broker.Broker/consume");
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod broker_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with BrokerServer."]
    #[async_trait]
    pub trait Broker: Send + Sync + 'static {
        async fn topic_partition_leader(
            &self,
            request: tonic::Request<super::TopicPartitionLeaderInput>,
        ) -> Result<tonic::Response<super::Void>, tonic::Status>;
        async fn create(
            &self,
            request: tonic::Request<super::CreateInput>,
        ) -> Result<tonic::Response<super::Void>, tonic::Status>;
        async fn produce(
            &self,
            request: tonic::Request<super::ProducerInput>,
        ) -> Result<tonic::Response<super::Void>, tonic::Status>;
        #[doc = "Server streaming response type for the consume method."]
        type consumeStream: futures_core::Stream<Item = Result<super::ConsumerOutput, tonic::Status>>
            + Send
            + 'static;
        async fn consume(
            &self,
            request: tonic::Request<super::ConsumerInput>,
        ) -> Result<tonic::Response<Self::consumeStream>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct BrokerServer<T: Broker> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Broker> BrokerServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for BrokerServer<T>
    where
        T: Broker,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/broker.Broker/topic_partition_leader" => {
                    #[allow(non_camel_case_types)]
                    struct topic_partition_leaderSvc<T: Broker>(pub Arc<T>);
                    impl<T: Broker> tonic::server::UnaryService<super::TopicPartitionLeaderInput>
                        for topic_partition_leaderSvc<T>
                    {
                        type Response = super::Void;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::TopicPartitionLeaderInput>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).topic_partition_leader(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = topic_partition_leaderSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/broker.Broker/create" => {
                    #[allow(non_camel_case_types)]
                    struct createSvc<T: Broker>(pub Arc<T>);
                    impl<T: Broker> tonic::server::UnaryService<super::CreateInput> for createSvc<T> {
                        type Response = super::Void;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateInput>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).create(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = createSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/broker.Broker/produce" => {
                    #[allow(non_camel_case_types)]
                    struct produceSvc<T: Broker>(pub Arc<T>);
                    impl<T: Broker> tonic::server::UnaryService<super::ProducerInput> for produceSvc<T> {
                        type Response = super::Void;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ProducerInput>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).produce(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = produceSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/broker.Broker/consume" => {
                    #[allow(non_camel_case_types)]
                    struct consumeSvc<T: Broker>(pub Arc<T>);
                    impl<T: Broker> tonic::server::ServerStreamingService<super::ConsumerInput> for consumeSvc<T> {
                        type Response = super::ConsumerOutput;
                        type ResponseStream = T::consumeStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ConsumerInput>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).consume(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = consumeSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Broker> Clone for BrokerServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Broker> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Broker> tonic::transport::NamedService for BrokerServer<T> {
        const NAME: &'static str = "broker.Broker";
    }
}
