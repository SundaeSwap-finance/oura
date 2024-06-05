use std::path::PathBuf;

use gasket::framework::*;
use pallas::codec::minicbor::Decoder;
use serde::Deserialize;
use tokio::net::UdpSocket;
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
#[derive(Stage)]
#[stage(
    name = "source",
    unit = "NextResponse<BlockContent>",
    worker = "Worker"
)] //TODO: double check unit type
pub struct Stage {
    config: Config,

    chain: GenesisValues,
    intersect: IntersectConfig, // only Origin supported for now, we can maybe do tip later
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {


    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        debug!("connecting");

        let mut peer_session = UdpSocket::bind("127.0.0.1:5678").await.map_err(|_| WorkerError::Panic)?;
        // if stage.breadcrumbs.is_empty() {
        //     intersect_from_config(&mut peer_session, &stage.intersect).await?;
        // } else {
        //     intersect_from_breadcrumbs(&mut peer_session, &stage.breadcrumbs).await?;
        // }

        let worker = Self { session: HydraSession { udp_url: peer_session } };

        Ok(worker)
    }

    async fn schedule(
        &mut self,
        _stage: &mut Stage,
    ) -> Result<WorkSchedule<NextResponse<BlockContent>>, WorkerError> {
        //TODO: we don't need to schedule anything since we're just gonna get sent stuff over UDP
        // if we want to buffer, here's where we'd do it

        Ok(WorkSchedule::Idle)
    }

    async fn execute(
        &mut self,
        unit: &NextResponse<BlockContent>,
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        let mut buf = [0; 1024*1024];
        self.session.udp_url.recv_from(&mut buf).await.map_err(|_| WorkerError::Recv)?;
        //parse as cbor. this will be a statechanged event. then match on SnapshotConfirmed -> snapshot -> utxo, turn that into a multierablock (singleera is more accurate)
        // then output to stage like https://github.com/SundaeSwap-finance/oura/blob/d7838ea984e774399ab1790b97692847a6a7752e/src/sources/n2c.rs#L112
        
        // parse buf as cbor
        let mut decoder = Decoder::new(&buf);
        // i think transaction confirmed is the 10th (1 indexed) in the event enum. double check though
        
        decoder.map().map_err(|_| WorkerError::Panic)?;
        if decoder.str().map_err(|_| WorkerError::Panic)? != "Snapshot" {
            return Err(WorkerError::Panic);
        }
        decoder.map().map_err(|_| WorkerError::Panic)?;
        if decoder.str().map_err(|_| WorkerError::Panic)? != "confirmed" {
            return Err(WorkerError::Panic);
        }
        // TODO: double check indefinite etc. parse actual transactions
        let snapshot = decoder.array().map_err(|_| WorkerError::Panic)?;





    
        self.process_next(stage, todo!()).await
    }

    // async fn shutdown(&mut self) -> Result<()> {
    //     Ok(())
    // }
}

impl Worker {
    async fn process_next(&mut self, stage: &mut Stage, unit: NextResponse<BlockContent>) -> Result<(), WorkerError> {
        todo!()
    }
}

#[derive(Deserialize)]
pub struct Config {
    udp_address: PathBuf,
}

impl Config {
    pub fn bootstrapper(self, ctx: &Context) -> Result<Stage, Error> {
        let stage = Stage {
            config: self,
            chain: ctx.chain.clone().into(),
            intersect: ctx.intersect.clone(),
        };

        Ok(stage)
    }
}