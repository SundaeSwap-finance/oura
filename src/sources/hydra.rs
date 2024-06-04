use std::path::PathBuf;

use gasket::framework::*;
use serde::Deserialize;
use tracing::{debug, info};

use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::miniprotocols::chainsync::{BlockContent, NextResponse};
use pallas::network::miniprotocols::Point;

use crate::framework::*;

pub struct HydraSession {
    // we can probably use the udp sink as a source here
    udp_url: UdpSocket,
}

pub struct Worker {
    session: HydraSession,
}
pub struct Stage {
    config: Config,

    chain: GenesisValues,
    intersect: IntersectConfig, // only Origin supported for now, we can maybe do tip later
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        debug!("connecting");

        let mut peer_session = UdpSocket::bind("127.0.0.1:5678");

        // if stage.breadcrumbs.is_empty() {
        //     intersect_from_config(&mut peer_session, &stage.intersect).await?;
        // } else {
        //     intersect_from_breadcrumbs(&mut peer_session, &stage.breadcrumbs).await?;
        // }

        let worker = Self { peer_session };

        Ok(worker)
    }

    async fn schedule(
        &mut self,
        _stage: &mut Stage,
    ) -> Result<WorkSchedule<NextResponse<BlockContent>>, WorkerError> {
        //TODO: we don't need to schedule anything since we're just gonna get sent stuff over UDP
        // unless we want to like buffer it or something
        let client = self.peer_session.chainsync();

        let next = match client.has_agency() {
            true => {
                info!("requesting next block");
                client.request_next().await.or_restart()?
            }
            false => {
                info!("awaiting next block (blocking)");
                client.recv_while_must_reply().await.or_restart()?
            }
        };

        Ok(WorkSchedule::Unit(next))
    }

    async fn execute(
        &mut self,
        unit: &NextResponse<BlockContent>,
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        let mut buf = [0; 1024*1024];
        self.peer_session.recv_from(&mut buf).await?;
        //parse as cbor. this will be a statechanged event. then match on SnapshotConfirmed -> snapshot -> utxo, turn that into a multierablock (singleera is more accurate)
        // then output to stage like https://github.com/SundaeSwap-finance/oura/blob/d7838ea984e774399ab1790b97692847a6a7752e/src/sources/n2c.rs#L112
        // 
    
        self.process_next(stage, todo!()).await
    }
}

async fn intersect_from_config(
    peer: &mut NodeClient,
    intersect: &IntersectConfig,
) -> Result<(), WorkerError> {
    match intersect {
        IntersectConfig::Origin => {
            peer.intersect_origin().await.or_retry()?;
        }
    }

    Ok(())
}