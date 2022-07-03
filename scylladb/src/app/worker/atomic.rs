use super::*;
/// Atomic handle enables sending multiple queries and get one response once all dropped.
#[derive(Debug)]
pub struct AtomicHandle<H, O, E>
where
    H: DropResult<O, E>,
    O: Default,
    E: Default,
{
    handle: Option<H>,
    ok: O,
    err: E,
    any_error: std::sync::atomic::AtomicBool,
}

/// Drop Result from atomic handle
pub trait DropResult<O, E> {
    /// Drop the result from an atomic handle into the self/handle channel
    fn drop_result(&mut self, result: Result<O, E>) -> Option<()>;
}

impl<T, O, E> DropResult<O, E> for tokio::sync::mpsc::UnboundedSender<T>
where
    T: From<Result<O, E>>,
{
    fn drop_result(&mut self, result: Result<O, E>) -> Option<()> {
        let message = T::from(result);
        self.send(message).ok()
    }
}

impl<T, O, E> DropResult<O, E> for overclock::core::UnboundedHandle<T>
where
    T: From<Result<O, E>>,
{
    fn drop_result(&mut self, result: Result<O, E>) -> Option<()> {
        let message = T::from(result);
        self.send(message).ok()
    }
}

impl<T, O, E> DropResult<O, E> for overclock::core::AbortableUnboundedHandle<T>
where
    T: From<Result<O, E>>,
{
    fn drop_result(&mut self, result: Result<O, E>) -> Option<()> {
        let message = T::from(result);
        self.send(message).ok()
    }
}

impl<H, O: Default, E: Default> AtomicHandle<H, O, E>
where
    H: DropResult<O, E>,
{
    /// Create new atomic handle
    pub fn new(handle: H, ok: O, err: E) -> std::sync::Arc<Self> {
        Self {
            handle: Some(handle),
            ok,
            err,
            any_error: std::sync::atomic::AtomicBool::new(false),
        }
        .into()
    }
    /// Set the atomic error
    pub fn set_error(&self) {
        self.any_error.store(true, std::sync::atomic::Ordering::SeqCst);
    }
    /// Try to take the inner handle to prevent drop impl from dropping a result into the handle
    pub fn take(self: &mut std::sync::Arc<Self>) -> Option<H> {
        std::sync::Arc::get_mut(self).and_then(|this| this.handle.take())
    }
}

impl<H, O, E> Drop for AtomicHandle<H, O, E>
where
    H: DropResult<O, E>,
    O: Default,
    E: Default,
{
    fn drop(&mut self) {
        if self.any_error.load(std::sync::atomic::Ordering::Relaxed) {
            self.handle
                .take()
                .and_then(|mut h| h.drop_result(Err(std::mem::take(&mut self.err))));
        } else {
            self.handle
                .take()
                .and_then(|mut h| h.drop_result(Ok(std::mem::take(&mut self.ok))));
        }
    }
}

/// An atomic worker
#[derive(Clone)]
pub struct AtomicWorker<R, H> {
    /// The worker's request
    pub request: R,
    /// The number of times this worker will retry on failure
    pub retries: usize,
    /// the atomic handle
    pub handle: H,
}

impl<R, H, O, E> AtomicWorker<R, AtomicHandle<H, O, E>>
where
    R: std::fmt::Debug + 'static + Send + Sync + Request,
    H: std::fmt::Debug + 'static + Send + Sync + DropResult<O, E>,
    O: std::fmt::Debug + 'static + Send + Sync + Default,
    E: std::fmt::Debug + 'static + Send + Sync + Default,
{
    /// Create atomic worker
    pub fn new(request: R, handle: AtomicHandle<H, O, E>) -> Box<Self> {
        Self {
            request,
            retries: 0,
            handle,
        }
        .into()
    }

    pub(crate) fn from(
        BasicRetryWorker { request, retries }: BasicRetryWorker<R>,
        handle: AtomicHandle<H, O, E>,
    ) -> Box<Self>
    where
        R: 'static + Request + std::fmt::Debug + Send + Sync,
    {
        Self::new(request, handle).with_retries(retries)
    }
}

impl<R, H> std::fmt::Debug for AtomicWorker<R, H>
where
    R: std::fmt::Debug,
    H: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtomicWorker")
            .field("request", &self.request)
            .field("retries", &self.retries)
            .field("handle", &self.handle)
            .finish()
    }
}

impl<R, H, O, E> super::Worker for AtomicWorker<R, AtomicHandle<H, O, E>>
where
    R: std::fmt::Debug + 'static + Send + Sync + Request,
    H: std::fmt::Debug + 'static + Send + Sync + DropResult<O, E>,
    O: std::fmt::Debug + 'static + Send + Sync + Default,
    E: std::fmt::Debug + 'static + Send + Sync + Default,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::try_from(giveload)?;
        Ok(())
    }

    fn handle_error(
        self: Box<Self>,
        mut error: WorkerError,
        reporter_opt: Option<&ReporterHandle>,
    ) -> anyhow::Result<()> {
        error!("{}", error);
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter_opt) {
                let statement = self
                    .request
                    .statement_by_id(&id)
                    .ok_or_else(|| {
                        self.handle.set_error();
                        anyhow::anyhow!("Unable to get statement for a request {:?}", self.request)
                    })?
                    .clone()
                    .into();
                PrepareWorker::new(id, statement).send_to_reporter(reporter).ok();
            }
        }
        if let Some(worker) = self.retry()? {
            worker.handle.set_error();
            anyhow::bail!("AtomicWorker Consumed all retries")
        } else {
            Ok(())
        }
    }
}

impl<R, H, O, E> RetryableWorker<R> for AtomicWorker<R, AtomicHandle<H, O, E>>
where
    R: std::fmt::Debug + 'static + Send + Sync + Request,
    H: std::fmt::Debug + 'static + Send + Sync + DropResult<O, E>,
    O: std::fmt::Debug + 'static + Send + Sync + Default,
    E: std::fmt::Debug + 'static + Send + Sync + Default,
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

impl<R, H, O, E> RespondingWorker<R, AtomicHandle<H, O, E>, Decoder> for AtomicWorker<R, AtomicHandle<H, O, E>>
where
    H: 'static + DropResult<O, E> + std::fmt::Debug + Send + Sync,
    R: 'static + Send + std::fmt::Debug + Request + Sync,
    O: Default + 'static + Send + std::fmt::Debug + Sync,
    E: Default + 'static + Send + std::fmt::Debug + Sync,
{
    fn handle(&self) -> &AtomicHandle<H, O, E> {
        &self.handle
    }
}
