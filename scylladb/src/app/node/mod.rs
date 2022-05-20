use super::stage::Stage;
use async_trait::async_trait;
use overclock::core::{
    Actor,
    ActorError,
    ActorResult,
    Rt,
    ScopeId,
    Service,
    ServiceEvent,
    ServiceStatus,
    ShutdownEvent,
    StreamExt,
    SupHandle,
    UnboundedChannel,
};
use std::net::SocketAddr;

/// Node event enum.
pub enum NodeEvent {
    /// To keep the node with up to date stage(s) service
    Microservice(ScopeId, Service),
    /// Shutdown signal.
    Shutdown,
}
impl ShutdownEvent for NodeEvent {
    fn shutdown_event() -> Self {
        Self::Shutdown
    }
}
impl ServiceEvent<Stage> for NodeEvent {
    fn report_event(scope: ScopeId, service: Service) -> Self {
        Self::Microservice(scope, service)
    }
    fn eol_event(scope: ScopeId, service: Service, _actor: Stage, _r: ActorResult<()>) -> Self {
        Self::Microservice(scope, service)
    }
}

/// Node state
pub struct Node {
    address: SocketAddr,
    shard_count: usize,
}

impl Node {
    /// Create new Node with the provided socket address
    pub fn new(address: SocketAddr, shard_count: usize) -> Self {
        Self { address, shard_count }
    }
}

/// The Node actor lifecycle implementation
#[async_trait]
impl<S> Actor<S> for Node
where
    S: SupHandle<Self>,
{
    type Data = ();
    type Channel = UnboundedChannel<NodeEvent>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        // start stages in sync
        for shard_id in 0..self.shard_count {
            let stage = Stage::new(self.address, shard_id, self.shard_count);
            rt.start(format!("stage_{}", shard_id), stage).await?;
        }
        Ok(())
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, _: Self::Data) -> ActorResult<Self::Data> {
        log::info!("{} Node is {}", self.address, rt.service().status());
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                NodeEvent::Microservice(scope_id, service) => {
                    if service.is_stopped() {
                        rt.remove_microservice(scope_id);
                        rt.shutdown_children().await;
                        if !rt.service().is_stopping() {
                            if rt.microservices_stopped() {
                                log::warn!("Node {:?} disconnected", rt.service().directory().as_ref());
                                return Err(ActorError::restart_msg("disconnected", None));
                            } else {
                                rt.update_status(ServiceStatus::Degraded).await;
                            }
                        } else {
                            rt.update_status(ServiceStatus::Stopping).await;
                            if rt.microservices_stopped() {
                                rt.inbox_mut().close();
                            }
                        }
                    } else {
                        rt.upsert_microservice(scope_id, service);
                    }
                }
                NodeEvent::Shutdown => {
                    log::warn!("{} node is Stopping", self.address);
                    rt.stop().await;
                    if rt.microservices_stopped() {
                        rt.inbox_mut().close();
                    }
                }
            }
        }
        log::info!("{} node gracefully shutdown", self.address);
        Ok(())
    }
}
