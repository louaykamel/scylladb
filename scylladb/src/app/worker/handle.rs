use super::*;
use std::sync::{
    atomic::Ordering,
    Arc,
};

/// Create DecHandle
#[derive(Debug)]
pub struct DecHandle<H, R: SendRequestExt> {
    /// The handle used to send the response or error
    pub(super) handle: H,
    /// Request marker
    pub(super) _marker: std::marker::PhantomData<R>,
}

impl<H, R: SendRequestExt> DecHandle<H, R> {
    pub(super) fn new(handle: H) -> Self {
        Self {
            handle,
            _marker: std::marker::PhantomData,
        }
    }
}

/// Create RefHandle
#[derive(Debug)]
pub struct RefHandle<H, R> {
    /// The handle used to send the response or error
    pub(super) handle: H,
    /// The cql query reference
    pub(super) reference: R,
}

impl<H, R> RefHandle<H, R> {
    pub(super) fn new(handle: H, reference: R) -> Self {
        Self { handle, reference }
    }
}

#[derive(Debug)]
/// Cql Reference wrapper struct
pub struct Refer<R, T> {
    reference: R,
    inner: T,
}

impl<R, T> Refer<R, T> {
    /// Converts the refer into inner tuple
    pub fn into_inner(self) -> (R, T) {
        (self.reference, self.inner)
    }
}
impl<H, R, O, E> HandleResult<O, E> for RefHandle<H, R>
where
    H: HandleResult<Refer<R, O>, Refer<R, E>>,
{
    fn handle_response(self, ok: O) -> anyhow::Result<()> {
        let ok = Refer {
            reference: self.reference,
            inner: ok,
        };
        self.handle.handle_response(ok)
    }
    fn handle_error(self, err: E) -> anyhow::Result<()> {
        let err = Refer {
            reference: self.reference,
            inner: err,
        };
        self.handle.handle_error(err)
    }
}

impl<H, Req> HandleResult<Decoder, WorkerError> for DecHandle<H, Req>
where
    H: HandleResult<<<Req as SendRequestExt>::Marker as Marker>::Output, WorkerError>,
    Req: SendRequestExt,
{
    fn handle_response(self, decoder: Decoder) -> anyhow::Result<()> {
        let marker = DecodeResult::new(Req::Marker::new(), Req::TYPE);
        match marker.try_decode(decoder) {
            Ok(res) => self.handle.handle_response(res),
            Err(e) => self.handle.handle_error(WorkerError::Other(e)),
        }
    }
    fn handle_error(self, err: WorkerError) -> anyhow::Result<()> {
        self.handle.handle_error(err)
    }
}

impl<H, Req, R> HandleResult<Refer<R, Decoder>, Refer<R, WorkerError>> for DecHandle<H, Req>
where
    H: HandleResult<Refer<R, <<Req as SendRequestExt>::Marker as Marker>::Output>, Refer<R, WorkerError>>,
    Req: SendRequestExt,
{
    fn handle_response(self, r: Refer<R, Decoder>) -> anyhow::Result<()> {
        let reference = r.reference;
        let decoder = r.inner;
        let marker = DecodeResult::new(Req::Marker::new(), Req::TYPE);
        match marker.try_decode(decoder) {
            Ok(inner) => self.handle.handle_response(Refer { reference, inner }),
            Err(e) => self.handle.handle_error(Refer {
                reference,
                inner: WorkerError::Other(e),
            }),
        }
    }
    fn handle_error(self, r: Refer<R, WorkerError>) -> anyhow::Result<()> {
        let reference = r.reference;
        let err = r.inner;
        self.handle.handle_error(Refer { reference, inner: err })
    }
}

/// Atomic handle enables sending multiple queries and get one response once all dropped.
#[derive(Debug)]
pub struct AtomicHandle<H, R>
where
    H: HandleResult<R, R>,
    R: Default,
{
    handle: Option<H>,
    reference: R,
    any_error: std::sync::atomic::AtomicI8,
}

impl<H, R> AtomicHandle<H, R>
where
    H: HandleResult<R, R>,
    R: Default,
{
    /// Create new atomic handle
    pub fn new(handle: H, reference: R) -> Arc<Self> {
        Self {
            handle: Some(handle),
            reference,
            any_error: std::sync::atomic::AtomicI8::new(0),
        }
        .into()
    }
    /// Set the atomic error
    pub fn set_error(&self) -> Result<i8, i8> {
        self.any_error
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::Acquire)
    }
    /// Set any error to any value except (0, 1) to disable it from dropping a result
    pub fn disable(&self) {
        self.any_error.store(-1, Ordering::SeqCst)
    }
    /// Try to take the inner handle to prevent drop impl from dropping a result into the handle
    pub fn take(self: &mut Arc<Self>) -> Option<H> {
        std::sync::Arc::get_mut(self).and_then(|this| this.handle.take())
    }
}

impl<H, R> Drop for AtomicHandle<H, R>
where
    H: HandleResult<R, R>,
    R: Default,
{
    fn drop(&mut self) {
        match self.any_error.load(std::sync::atomic::Ordering::Relaxed) {
            0 => {
                self.handle
                    .take()
                    .and_then(|h| h.handle_response(std::mem::take(&mut self.reference)).ok());
            }
            1 => {
                self.handle
                    .take()
                    .and_then(|h| h.handle_error(std::mem::take(&mut self.reference)).ok());
            }
            _ => (),
        }
    }
}

impl<H, R> HandleResult<Decoder, WorkerError> for Arc<AtomicHandle<H, R>>
where
    H: HandleResult<R, R>,
    R: Default,
{
    fn handle_response(self, _: Decoder) -> anyhow::Result<()> {
        Ok(())
    }
    fn handle_error(self, _: WorkerError) -> anyhow::Result<()> {
        self.set_error().ok();
        Ok(())
    }
}
