use std::{pin::Pin, sync::Arc};

use futures_util::Stream;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::protocol::oll::{self, plugin_runtime_server::PluginRuntime};

use super::super::OUTBOUND_ENVELOPE_CAPACITY;

type OutboundStream =
    Pin<Box<dyn Stream<Item = Result<oll::PluginEnvelope, Status>> + Send + 'static>>;

pub(in crate::plugin::runtime) struct ConnectedSession {
    pub(in crate::plugin::runtime) incoming: Streaming<oll::PluginEnvelope>,
    pub(in crate::plugin::runtime) outgoing: mpsc::Sender<Result<oll::PluginEnvelope, Status>>,
}

pub(in crate::plugin::runtime) struct InstanceService {
    connection: Arc<Mutex<Option<oneshot::Sender<ConnectedSession>>>>,
}

impl InstanceService {
    pub(in crate::plugin::runtime) fn new() -> (Self, oneshot::Receiver<ConnectedSession>) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                connection: Arc::new(Mutex::new(Some(sender))),
            },
            receiver,
        )
    }
}

#[tonic::async_trait]
impl PluginRuntime for InstanceService {
    type ConnectStream = OutboundStream;

    async fn connect(
        &self,
        request: Request<Streaming<oll::PluginEnvelope>>,
    ) -> Result<Response<Self::ConnectStream>, Status> {
        let sender = self
            .connection
            .lock()
            .await
            .take()
            .ok_or_else(|| Status::already_exists("plugin instance already connected"))?;
        let (outgoing, receiver) = mpsc::channel(OUTBOUND_ENVELOPE_CAPACITY);
        sender
            .send(ConnectedSession {
                incoming: request.into_inner(),
                outgoing,
            })
            .map_err(|_| Status::unavailable("plugin instance is no longer accepting Connect"))?;
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}
