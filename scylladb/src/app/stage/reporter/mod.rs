use async_trait::async_trait;

use super::{
    sender::SenderEvent,
    Payloads,
};
use crate::{
    app::worker::{
        Worker,
        WorkerError,
    },
    cql::{
        CqlError,
        Decoder,
    },
};
use std::convert::TryFrom;

use overclock::core::{
    AbortableUnboundedHandle,
    Actor,
    ActorError,
    ActorResult,
    Rt,
    ShutdownEvent,
    StreamExt,
    SupHandle,
    UnboundedChannel,
    UnboundedHandle,
};
use std::collections::HashMap;
/// Workers Map holds all the workers_ids
type Workers = HashMap<i16, Box<dyn Worker>>;
/// Reporter's handle, used to push cql request
pub type ReporterHandle = UnboundedHandle<ReporterEvent>;

/// Reporter event enum.
#[derive(Debug)]
pub enum ReporterEvent {
    /// The request Cql query.
    Request {
        /// The worker which is used to process the request.
        worker: Box<dyn Worker>,
        /// The request payload.
        payload: Vec<u8>,
    },
    /// The response Cql query.
    Response {
        /// The reponse stream ID.
        stream_id: i16,
    },
    /// The stream error.
    Err(anyhow::Error, i16),
    /// Shutdown signal
    Shutdown,
}
impl ShutdownEvent for ReporterEvent {
    fn shutdown_event() -> Self {
        Self::Shutdown
    }
}
/// Reporter state
pub struct Reporter {
    streams: Vec<i16>,
    workers: Workers,
}

impl Reporter {
    /// Create new reporter
    pub(super) fn new(streams: Vec<i16>) -> Self {
        Self {
            streams,
            workers: HashMap::new(),
        }
    }
}

/// The Reporter actor lifecycle implementation
#[async_trait]
impl<S> Actor<S> for Reporter
where
    S: SupHandle<Self>,
{
    type Data = (Payloads, AbortableUnboundedHandle<SenderEvent>);
    type Channel = UnboundedChannel<ReporterEvent>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        let parent_id = rt
            .parent_id()
            .ok_or_else(|| ActorError::exit_msg("reporter without stage supervisor"))?;
        let sender_scope_id = rt
            .sibling("sender")
            .scope_id()
            .await
            .ok_or_else(|| ActorError::exit_msg("reporter without sender sibling"))?;
        let payloads = rt
            .lookup(parent_id)
            .await
            .ok_or_else(|| ActorError::exit_msg("reporter unables to lookup for payloads"))?;
        let sender_handle = rt.link(sender_scope_id, false).await.map_err(ActorError::exit)?;
        Ok((payloads, sender_handle))
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, (mut payloads, sender): Self::Data) -> ActorResult<()> {
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                ReporterEvent::Request { worker, mut payload } => {
                    if let Some(stream) = self.streams.pop() {
                        // Assign stream_id to the payload
                        assign_stream_to_payload(stream, &mut payload);
                        // store payload as reusable at payloads[stream]
                        payloads[stream as usize].as_mut().replace(payload);
                        self.workers.insert(stream, worker);
                        sender.send(stream).unwrap_or_else(|e| log::error!("{}", e));
                    } else {
                        // Send overload to the worker in-case we don't have anymore streams
                        worker
                            .handle_error(WorkerError::Overload, Some(rt.handle()))
                            .unwrap_or_else(|e| log::error!("{}", e));
                    }
                }
                ReporterEvent::Response { stream_id } => {
                    self.handle_response(rt.handle(), stream_id, &mut payloads)
                        .unwrap_or_else(|e| log::error!("{}", e));
                }
                ReporterEvent::Err(io_error, stream_id) => {
                    self.handle_error(rt.handle(), stream_id, &mut payloads, WorkerError::Other(io_error))
                        .unwrap_or_else(|e| log::error!("{}", e));
                }
                ReporterEvent::Shutdown => {
                    rt.stop().await;
                    if rt.microservices_stopped() {
                        rt.inbox_mut().close();
                    }
                }
            }
        }
        self.force_consistency(rt.handle());
        Ok(())
    }
}
impl Reporter {
    fn handle_response(&mut self, handle: &ReporterHandle, stream: i16, payloads: &mut Payloads) -> anyhow::Result<()> {
        // push the stream_id back to streams vector.
        self.streams.push(stream);
        // remove the worker from workers.
        if let Some(worker) = self.workers.remove(&stream) {
            if let Some(payload) = payloads[stream as usize].as_mut().take() {
                if is_cql_error(&payload) {
                    let error = Decoder::try_from(payload)
                        .and_then(|mut decoder| CqlError::new(&mut decoder).map(|e| WorkerError::Cql(e)))
                        .unwrap_or_else(|e| WorkerError::Other(e));
                    worker.handle_error(error, Some(handle))?;
                } else {
                    worker.handle_response(payload)?;
                }
            } else {
                log::error!("No payload found while handling response for stream {}!", stream);
            }
        } else {
            log::error!("No worker found while handling response for stream {}!", stream);
        }
        Ok(())
    }
    fn handle_error(
        &mut self,
        handle: &ReporterHandle,
        stream: i16,
        payloads: &mut Payloads,
        error: WorkerError,
    ) -> anyhow::Result<()> {
        // push the stream_id back to streams vector.
        self.streams.push(stream);
        // remove the worker from workers and send error.
        if let Some(worker) = self.workers.remove(&stream) {
            // drop payload.
            if let Some(_payload) = payloads[stream as usize].as_mut().take() {
                worker.handle_error(error, Some(handle))?;
            } else {
                log::error!("No payload found while handling error for stream {}!", stream);
            }
        } else {
            log::error!("No worker found while handling error for stream {}!", stream);
        }
        Ok(())
    }
    fn force_consistency(&mut self, handle: &ReporterHandle) {
        for (stream_id, worker_id) in self.workers.drain() {
            // push the stream_id back into the streams vector
            self.streams.push(stream_id);
            // tell worker_id that we lost the response for his request, because we lost scylla connection in
            // middle of request cycle, still this is a rare case.
            worker_id
                .handle_error(WorkerError::Lost, Some(handle))
                .unwrap_or_else(|e| log::error!("{}", e));
        }
    }
}

// private functions
fn assign_stream_to_payload(stream: i16, payload: &mut Vec<u8>) {
    payload[2] = (stream >> 8) as u8; // payload[2] is where the first byte of the stream_id should be,
    payload[3] = stream as u8; // payload[3] is the second byte of the stream_id. please refer to cql specs
}

fn is_cql_error(buffer: &[u8]) -> bool {
    buffer[4] == 0
}
