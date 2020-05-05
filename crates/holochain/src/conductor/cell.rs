use super::{api::error::SerializationError, ConductorHandle};
use crate::{
    conductor::{
        api::{error::ConductorApiResult, CellConductorApi},
        cell::error::CellResult,
    },
    core::{
        ribosome::WasmRibosome,
        workflow::{runner::WorkflowRunner, WorkflowCall},
    },
};
use holo_hash::*;
use holochain_serialized_bytes::SerializedBytes;
use holochain_state::env::{Environment, EnvironmentKind};
use holochain_types::{
    autonomic::AutonomicProcess,
    cell::CellId,
    nucleus::{ZomeInvocation, ZomeInvocationResponse},
    shims::*,
};
use holochain_keystore::KeystoreSender;

use std::{
    convert::TryInto,
    hash::{Hash, Hasher},
    path::Path,
};

pub mod error;

impl Hash for Cell {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.id.hash(state);
    }
}

impl PartialEq for Cell {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

/// A Cell is a grouping of the resources necessary to run workflows
/// on behalf of an agent. It does not have a lifetime of its own aside
/// from the lifetimes of the resources which it holds references to.
/// Any work it does is through running a workflow, passing references to
/// the resources needed to complete that workflow.
///
/// The [Conductor] manages a collection of Cells, and will call functions
/// on the Cell when a Conductor API method is called (either a
/// [CellConductorApi] or an [AppInterfaceApi])
pub struct Cell {
    id: CellId,
    conductor_api: CellConductorApi,
    state_env: Environment,
}

impl Cell {
    pub fn create<P: AsRef<Path>>(
        id: CellId,
        conductor_handle: ConductorHandle,
        env_path: P,
        keystore: KeystoreSender,
    ) -> CellResult<Self> {
        let conductor_api = CellConductorApi::new(conductor_handle, id.clone());
        let state_env = Environment::new(
            env_path.as_ref(),
            EnvironmentKind::Cell(id.clone()),
            keystore,
        )?;
        Ok(Self {
            id,
            conductor_api,
            state_env,
        })
    }
    #[allow(dead_code)]
    fn dna_hash(&self) -> &DnaHash {
        &self.id.dna_hash()
    }

    #[allow(dead_code)]
    fn agent_pubkey(&self) -> &AgentPubKey {
        &self.id.agent_pubkey()
    }

    /// Entry point for incoming messages from the network that need to be handled
    pub async fn handle_network_message(
        &self,
        _msg: Lib3hToClient,
    ) -> CellResult<Option<Lib3hToClientResponse>> {
        unimplemented!()
    }

    /// When the Conductor determines that it's time to execute some [AutonomicProcess],
    /// whether scheduled or through an [AutonomicCue], this function gets called
    pub async fn handle_autonomic_process(&self, process: AutonomicProcess) -> CellResult<()> {
        match process {
            AutonomicProcess::SlowHeal => unimplemented!(),
            AutonomicProcess::HealthCheck => unimplemented!(),
        }
    }

    /// Function called by the Conductor
    pub async fn invoke_zome(
        &self,
        invocation: ZomeInvocation,
    ) -> ConductorApiResult<ZomeInvocationResponse> {
        // create the workflow runner
        let runner = WorkflowRunner::new(&self);
        // create the work flow call
        let call = WorkflowCall::InvokeZome(invocation.into());
        // call the workflow
        // FIXME this result isn't actualy returned
        let result = runner.run_workflow(call).await?;
        let result: SerializedBytes = result.try_into().map_err(|e| SerializationError::from(e))?;
        Ok(ZomeInvocationResponse::ZomeApiFn(
            result.try_into().map_err(|e| SerializationError::from(e))?,
        ))
    }

    // TODO: tighten up visibility: only WorkflowRunner needs to access this
    pub(crate) fn get_ribosome(&self) -> WasmRibosome {
        unimplemented!()
    }

    // TODO: tighten up visibility: only WorkflowRunner needs to access this
    pub(crate) fn state_env(&self) -> Environment {
        self.state_env.clone()
    }

    // TODO: tighten up visibility: only WorkflowRunner needs to access this
    pub(crate) fn get_conductor_api(&self) -> CellConductorApi {
        self.conductor_api.clone()
    }
}

////////////////////////////////////////////////////////////////////////////////////
// The following is a sketch from the skunkworx phase, and can probably be removed

// These are possibly composable traits that describe how to get a resource,
// so instead of explicitly building resources, we can downcast a Cell to exactly
// the right set of resource getter traits
trait NetSend {
    fn network_send(&self, msg: Lib3hClientProtocol) -> Result<(), NetError>;
}

#[allow(dead_code)]
/// TODO - this is a shim until we need a real NetError
enum NetError {}
