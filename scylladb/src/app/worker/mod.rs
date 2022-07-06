pub use crate::app::stage::reporter::{
    ReporterEvent,
    ReporterHandle,
};
use crate::{
    app::{
        access::*,
        ring::RingSendError,
    },
    cql::{
        CqlError,
        Decoder,
    },
};
pub use any::AnyWorker;
use anyhow::anyhow;
pub use basic::{
    BasicRetryWorker,
    BasicWorker,
    SpawnableRespondWorker,
};
pub use handle::{
    AtomicHandle,
    Refer,
};
use log::*;
pub use prepare::PrepareWorker;
use std::convert::TryFrom;
use thiserror::Error;
use tokio::{
    runtime::Handle,
    sync::mpsc::UnboundedSender,
    task,
};

mod any;
mod basic;
mod handle;
mod prepare;

/// WorkerId trait type which will be implemented by worker in order to send their channel_tx.
pub trait Worker: Send + Sync + std::fmt::Debug + 'static {
    /// Reporter will invoke this method to Send the cql response to worker
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()>;
    /// Reporter will invoke this method to Send the worker error to worker
    fn handle_error(self: Box<Self>, error: WorkerError, reporter: Option<&ReporterHandle>) -> anyhow::Result<()>;
}

#[derive(Error, Debug)]
/// The CQL worker error.
pub enum WorkerError {
    /// The CQL Error reported from ScyllaDB.
    #[error("Worker CqlError: {0}")]
    Cql(CqlError),
    /// The IO Error.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    /// The overload when we do not have any more streams.
    #[error("Worker Overload")]
    Overload,
    /// We lost the worker due to the abortion of ScyllaDB connection.
    #[error("Worker Lost")]
    Lost,
    /// There is no ring initialized.
    #[error("Worker NoRing")]
    NoRing,
}

/// should be implemented on the handle of the worker
pub trait HandleResult<O, E> {
    /// Handle response for worker of type W
    fn handle_response(self, ok: O) -> anyhow::Result<()>;
    /// Handle error for worker of type W
    fn handle_error(self, err: E) -> anyhow::Result<()>;
}

/// Defines a worker which contains enough information to retry on a failure
#[async_trait::async_trait]
pub trait RetryableWorker<R>: Worker {
    /// Get the number of retries remaining
    fn retries(&self) -> usize;
    /// Mutably access the number of retries remaining
    fn retries_mut(&mut self) -> &mut usize;
    /// Get the request
    fn request(&self) -> &R;
    /// Update the retry count
    fn with_retries(mut self, retries: usize) -> Self
    where
        Self: Sized + Into<Box<Self>>,
    {
        *self.retries_mut() = retries;
        self
    }

    /// Retry the worker
    fn retry(mut self) -> anyhow::Result<Option<Box<Self>>>
    where
        Self: 'static + Sized + Into<Box<Self>> + Worker,
        R: Request,
    {
        if self.retries() > 0 {
            *self.retries_mut() -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            if let Err(ring_send_error) = send_global(
                self.request().keyspace().as_ref().map(|s| s.as_str()),
                self.request().token(),
                self.request().payload(),
                self.into(),
            ) {
                if let ReporterEvent::Request { worker, .. } = ring_send_error.into() {
                    worker.handle_error(WorkerError::Other(anyhow::Error::msg("Retrying on the fly")), None)?;
                } else {
                    unreachable!("Unexpected report event variant while sending retry request using send global");
                };
            }
            Ok(None)
        } else {
            Ok(Some(self.into()))
        }
    }

    /// Send the worker to a specific reporter, without waiting for a response
    fn send_to_reporter(self: Box<Self>, reporter: &ReporterHandle) -> Result<DecodeResult<R::Marker>, RequestError>
    where
        Self: 'static + Sized + Worker,
        R: SendRequestExt,
    {
        let request = ReporterEvent::Request {
            payload: self.request().payload(),
            worker: self,
        };
        reporter.send(request).map_err(|e| RingSendError::SendError(e))?;
        Ok(DecodeResult::new(R::Marker::new(), R::TYPE))
    }

    /// Send the worker to the local datacenter, without waiting for a response
    fn send_local(self) -> Result<DecodeResult<R::Marker>, RequestError>
    where
        Self: 'static + Sized + Into<Box<Self>> + Worker,
        R: SendRequestExt,
    {
        if let Err(ring_send_error) = send_local(
            self.request().keyspace().as_ref().map(|s| s.as_str()),
            self.request().token(),
            self.request().payload(),
            self.into(),
        ) {
            if let ReporterEvent::Request { worker, .. } = ring_send_error.into() {
                worker
                    .handle_error(WorkerError::Other(anyhow::Error::msg("Retrying on the fly")), None)
                    .map_err(|e| RequestError::Other(e))?
            } else {
                unreachable!("Unexpected report event variant while sending retry request using send local");
            }
        };
        Ok(DecodeResult::new(R::Marker::new(), R::TYPE))
    }

    /// Send the worker to a global datacenter, without waiting for a response
    fn send_global(self) -> Result<DecodeResult<R::Marker>, RequestError>
    where
        Self: 'static + Sized + Into<Box<Self>> + Worker,
        R: SendRequestExt,
    {
        if let Err(ring_send_error) = send_global(
            self.request().keyspace().as_ref().map(|s| s.as_str()),
            self.request().token(),
            self.request().payload(),
            self.into(),
        ) {
            if let ReporterEvent::Request { worker, .. } = ring_send_error.into() {
                worker
                    .handle_error(WorkerError::Other(anyhow::Error::msg("Retrying on the fly")), None)
                    .map_err(|e| RequestError::Other(e))?
            } else {
                unreachable!("Unexpected report event variant while sending retry request using send global");
            }
        };
        Ok(DecodeResult::new(R::Marker::new(), R::TYPE))
    }

    /// Send the worker to the local datacenter and await a response asynchronously
    async fn get_local(self) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        R: SendRequestExt,
        Self: 'static
            + IntoRespondingWorker<R, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>
            + Into<Box<Self>>,
        Self::Output: RetryableWorker<R> + Worker,
        R::Marker: Send + Sync,
    {
        let (handle, inbox) = tokio::sync::oneshot::channel();
        let marker = self.with_handle(handle).send_local()?;
        Ok(marker.try_decode(
            inbox
                .await
                .map_err(|e| anyhow::anyhow!("No response from worker: {}", e))??,
        )?)
    }

    /// Send the worker to the local datacenter and await a response synchronously
    fn get_local_blocking(self) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        R: SendRequestExt,
        Self: 'static
            + IntoRespondingWorker<R, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>
            + Into<Box<Self>>,
        Self::Output: RetryableWorker<R> + Worker,
    {
        let (handle, inbox) = tokio::sync::oneshot::channel();
        let marker = self.with_handle(handle).send_local()?;

        Ok(marker.try_decode(
            task::block_in_place(move || Handle::current().block_on(async move { inbox.await }))
                .map_err(|e| anyhow::anyhow!("No response from worker: {}", e))??,
        )?)
    }

    /// Send the worker to a global datacenter and await a response asynchronously
    async fn get_global(self) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        R: SendRequestExt,
        Self: 'static
            + IntoRespondingWorker<R, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>
            + Into<Box<Self>>,
        Self::Output: RetryableWorker<R> + Worker,
        R::Marker: Send + Sync,
    {
        let (handle, inbox) = tokio::sync::oneshot::channel();
        let marker = self.with_handle(handle).send_global()?;

        Ok(marker.try_decode(
            inbox
                .await
                .map_err(|e| anyhow::anyhow!("No response from worker: {}", e))??,
        )?)
    }

    /// Send the worker to a global datacenter and await a response synchronously
    fn get_global_blocking(self) -> Result<<R::Marker as Marker>::Output, RequestError>
    where
        R: SendRequestExt,
        Self: 'static
            + IntoRespondingWorker<R, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>
            + Into<Box<Self>>,
        Self::Output: RetryableWorker<R> + Worker,
    {
        let (handle, inbox) = tokio::sync::oneshot::channel();
        let marker = self.with_handle(handle).send_global()?;

        Ok(marker.try_decode(
            task::block_in_place(move || Handle::current().block_on(async move { inbox.await }))
                .map_err(|e| anyhow::anyhow!("No response from worker: {}", e))??,
        )?)
    }
}

/// Defines a worker which can be given a handle to be capable of responding to the sender
pub trait IntoReferencingWorker<Ref> {
    /// The type of worker which will be created
    type Output: Send;
    /// Give the worker a reference
    fn with_ref(self, reference: Ref) -> Self::Output;
}

/// Defines a worker which can be given a handle to be capable of responding to the sender
pub trait IntoDecodingWorker {
    /// The type of worker which will be created
    type Output: Send;
    /// Give the worker a reference
    fn with_decoder(self) -> Self::Output;
}

/// Defines a worker which can be given a handle to be capable of responding to the sender
pub trait IntoRespondingWorker<R, H, O = Decoder>
where
    R: SendRequestExt,
    Self: Sized,
{
    /// The type of worker which will be created
    type Output: Send;
    /// Give the worker a handle
    fn with_handle(self, handle: H) -> Self::Output;
}

/// A worker which can respond to a sender
#[async_trait::async_trait]
pub trait RespondingWorker<R, H, O>: Worker {
    /// Get the handle this worker will use to respond
    fn handle(&self) -> &H;
}

impl<O, E, T> HandleResult<O, E> for UnboundedSender<T>
where
    T: From<Result<O, E>> + Send,
{
    fn handle_response(self, response: O) -> anyhow::Result<()> {
        self.send(T::from(Ok(response))).map_err(|e| anyhow!(e.to_string()))
    }
    fn handle_error(self, err: E) -> anyhow::Result<()> {
        self.send(T::from(Err(err))).map_err(|e| anyhow!(e.to_string()))
    }
}

impl<O, E, T> HandleResult<O, E> for tokio::sync::oneshot::Sender<T>
where
    T: From<Result<O, E>> + Send,
{
    fn handle_response(self, response: O) -> anyhow::Result<()> {
        self.send(T::from(Ok(response)))
            .map_err(|_| anyhow!("Failed to send response!"))
    }
    fn handle_error(self, err: E) -> anyhow::Result<()> {
        self.send(T::from(Err(err)))
            .map_err(|_| anyhow!("Failed to send error response!"))
    }
}

impl<O, E, T> HandleResult<O, E> for overclock::core::UnboundedHandle<T>
where
    T: From<Result<O, E>> + Send,
{
    fn handle_response(self, response: O) -> anyhow::Result<()> {
        self.send(T::from(Ok(response))).map_err(|e| anyhow!(e.to_string()))
    }
    fn handle_error(self, err: E) -> anyhow::Result<()> {
        self.send(T::from(Err(err))).map_err(|e| anyhow!(e.to_string()))
    }
}

/// Handle an unprepared CQL error by sending a prepare
/// request and resubmitting the original query as an
/// unprepared statement
pub fn handle_unprepared_error<W, R>(worker: Box<W>, id: [u8; 16], reporter: &ReporterHandle) -> Result<(), Box<W>>
where
    W: 'static + Worker + RetryableWorker<R> + Send + Sync,
    R: Request,
{
    if let Some(statement) = worker.request().statement_by_id(&id) {
        info!("Attempting to prepare statement '{}', id: '{:?}'", statement, id);
        PrepareWorker::new(id, statement.try_into().unwrap())
            .send_to_reporter(reporter)
            .ok();
        let payload = worker.request().payload();
        let retry_request = ReporterEvent::Request { worker, payload };
        reporter.send(retry_request).ok();
    } else {
        log::error!("Unable to handle unprepared error, due to missing statement by id")
    };
    Ok(())
}

/// retry to send a request
pub fn retry_send(keyspace: Option<&str>, mut r: RingSendError, mut retries: u8) -> Result<(), Box<dyn Worker>> {
    loop {
        if let ReporterEvent::Request { worker, payload } = r.into() {
            if retries > 0 {
                if let Err(still_error) = send_global(keyspace, rand::random(), payload, worker) {
                    r = still_error;
                    retries -= 1;
                } else {
                    break;
                };
            } else {
                return Err(worker);
            }
        } else {
            unreachable!("the reporter variant must be request");
        }
    }
    Ok(())
}
