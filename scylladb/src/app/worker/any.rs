use super::{
    handle::*,
    *,
};
use std::fmt::Debug;

/// An Any worker
#[derive(Clone, Debug)]
pub struct AnyWorker<H, R> {
    /// The worker's request
    pub request: R,
    /// A handle which can be used to return the queried value
    pub handle: H,
    /// The number of times this worker will retry on failure
    pub retries: usize,
}

impl<H, R> AnyWorker<H, R>
where
    H: 'static,
{
    /// Create a new value selecting worker with a number of retries and a response handle
    pub fn new(request: R, handle: H, retries: usize) -> Self {
        Self {
            request,
            handle,
            retries,
        }
    }
    /// Set retries to the worker
    pub fn with_retries(mut self, retries: usize) -> Self {
        self.retries = retries;
        self
    }

    /// Send the worker to the local datacenter, without waiting for a response
    pub fn send_local(self) -> Result<DecodeResult<R::Marker>, RequestError>
    where
        Self: 'static + Sized + Into<Box<Self>> + Worker + RetryableWorker<R>,
        R: SendRequestExt,
    {
        RetryableWorker::<R>::send_local(self)
    }

    /// Send the worker to the local datacenter, without waiting for a response
    pub fn send_global(self) -> Result<DecodeResult<R::Marker>, RequestError>
    where
        Self: 'static + Sized + Into<Box<Self>> + Worker + RetryableWorker<R>,
        R: SendRequestExt,
    {
        RetryableWorker::<R>::send_global(self)
    }

    pub(crate) fn from(BasicRetryWorker { request, retries }: BasicRetryWorker<R>, handle: H) -> Self
    where
        R: 'static + SendRequestExt + Debug + Send + Sync,
    {
        Self::new(request, handle, retries)
    }
}

impl<H, R> Worker for AnyWorker<H, R>
where
    H: 'static + HandleResult<Decoder, WorkerError> + Debug + Send + Sync,
    R: 'static + Send + Debug + SendRequestExt + Sync,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        match Decoder::try_from(giveload) {
            Ok(decoder) => self.handle.handle_response(decoder),
            Err(e) => self.handle.handle_error(WorkerError::Other(e)),
        }
    }

    fn handle_error(self: Box<Self>, mut error: WorkerError, reporter: Option<&ReporterHandle>) -> anyhow::Result<()> {
        error!("{}", error);
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                handle_unprepared_error(self, id, reporter).or_else(|worker| {
                    error!(
                        "Error trying to reprepare query: {:?}",
                        worker.request().statement_by_id(&id)
                    );
                    worker.handle.handle_error(error)
                })
            } else {
                self.retry()?.map_or_else(|| Ok(()), |w| w.handle.handle_error(error))
            }
        } else {
            self.retry()?.map_or_else(|| Ok(()), |w| w.handle.handle_error(error))
        }
    }
}

impl<H, R> RetryableWorker<R> for AnyWorker<H, R>
where
    H: 'static + HandleResult<Decoder, WorkerError> + Debug + Send + Sync,
    R: 'static + Send + Debug + SendRequestExt + Sync,
{
    fn retries(&self) -> usize {
        self.retries
    }

    fn request(&self) -> &R {
        &self.request
    }

    fn retries_mut(&mut self) -> &mut usize {
        &mut self.retries
    }
}

impl<Ref, H, R> IntoReferencingWorker<Ref> for AnyWorker<H, R>
where
    H: 'static + Debug + Send + Sync,
    Ref: 'static + Debug + Send + Sync,
    R: 'static + Send + Debug + Request + Sync + SendRequestExt,
{
    type Output = AnyWorker<RefHandle<H, Ref>, R>;
    fn with_ref(self, reference: Ref) -> AnyWorker<RefHandle<H, Ref>, R> {
        let handle = RefHandle::new(self.handle, reference);
        AnyWorker::new(self.request, handle, self.retries)
    }
}

impl<H, R: SendRequestExt> IntoDecodingWorker for AnyWorker<H, R>
where
    H: 'static + Debug + Send + Sync,
    R: 'static + Send + Debug + Request + Sync + SendRequestExt,
{
    type Output = AnyWorker<DecHandle<H, R>, R>;
    fn with_decoder(self) -> Self::Output {
        let handle = DecHandle::<H, R>::new(self.handle);
        AnyWorker::new(self.request, handle, self.retries)
    }
}

impl<R, H> RespondingWorker<R, H, Decoder> for AnyWorker<H, R>
where
    H: 'static + HandleResult<Decoder, WorkerError> + Debug + Send + Sync,
    R: 'static + Send + Debug + SendRequestExt + Sync,
{
    fn handle(&self) -> &H {
        &self.handle
    }
}
