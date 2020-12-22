//! A wrapper around ConductorHandle with more convenient methods for testing
// TODO [ B-03669 ] move to own crate

use super::{CoolAgents, CoolApp, CoolAppBatch, CoolCell, CoolZome};
use crate::{
    conductor::{
        api::ZomeCall, config::ConductorConfig, dna_store::DnaStore, handle::ConductorHandle,
        Conductor, ConductorBuilder,
    },
    core::ribosome::ZomeCallInvocation,
};
use futures::future;
use hdk3::prelude::*;
use holochain_keystore::KeystoreSender;
use holochain_state::test_utils::{test_environments, TestEnvironments};
use holochain_types::app::InstalledCell;
use holochain_types::dna::zome::Zome;
use holochain_types::dna::DnaFile;
use kitsune_p2p::KitsuneP2pConfig;
use std::sync::Arc;
use unwrap_to::unwrap_to;

/// A collection of CoolConductors, with methods for operating on the entire collection
#[derive(Clone, derive_more::From, derive_more::Into, derive_more::IntoIterator)]
pub struct CoolConductorBatch(Vec<CoolConductor>);

impl CoolConductorBatch {
    /// Map the given ConductorConfigs into CoolConductors, each with its own new TestEnvironments
    pub async fn from_configs<I: IntoIterator<Item = ConductorConfig>>(
        configs: I,
    ) -> CoolConductorBatch {
        future::join_all(configs.into_iter().map(CoolConductor::from_config))
            .await
            .into()
    }

    /// Create the given number of new CoolConductors, each with its own new TestEnvironments
    pub async fn from_config(num: usize, config: ConductorConfig) -> CoolConductorBatch {
        Self::from_configs(std::iter::repeat(config).take(num)).await
    }

    /// Create the given number of new CoolConductors, each with its own new TestEnvironments
    pub async fn from_standard_config(num: usize) -> CoolConductorBatch {
        Self::from_configs(std::iter::repeat_with(standard_config).take(num)).await
    }

    /// Get the underlying data
    pub fn iter(&self) -> impl Iterator<Item = &CoolConductor> {
        self.0.iter()
    }

    /// Get the underlying data
    pub fn into_inner(self) -> Vec<CoolConductor> {
        self.0
    }

    /// Opinionated app setup.
    /// Creates one app on each Conductor in this batch, creating a new AgentPubKey for each.
    /// The created AgentPubKeys can be retrieved via each CoolApp.
    pub async fn setup_app(&self, installed_app_id: &str, dna_files: &[DnaFile]) -> CoolAppBatch {
        let apps = self
            .0
            .iter()
            .map(|conductor| async move {
                let agent = CoolAgents::one(conductor.keystore()).await;
                conductor
                    .setup_app_for_agent(installed_app_id, agent, dna_files)
                    .await
            })
            .collect::<Vec<_>>();

        future::join_all(apps).await.into()
    }

    /// Opinionated app setup. Creates one app on each Conductor in this batch,
    /// using the given agents and DnaFiles.
    ///
    /// The number of Agents passed in must be the same as the number of Conductors
    /// in this batch. Each Agent will be used to create one app on one Conductor,
    /// hence the "zipped" in the function name
    ///
    /// Returns a batch of CoolApps, sorted in the same order as the Conductors in
    /// this batch.
    pub async fn setup_app_for_zipped_agents(
        &self,
        installed_app_id: &str,
        agents: &[AgentPubKey],
        dna_files: &[DnaFile],
    ) -> CoolAppBatch {
        if agents.len() != self.0.len() {
            panic!("setup_app_for_zipped_agents must take as many Agents as there are Conductors in this batch.")
        }

        let apps = self
            .0
            .iter()
            .zip(agents.iter())
            .map(|(conductor, agent)| {
                conductor.setup_app_for_agent(installed_app_id, agent.clone(), dna_files)
            })
            .collect::<Vec<_>>();

        future::join_all(apps).await.into()
    }

    /// Let each conductor know about each others' agents so they can do networking
    pub async fn exchange_peer_info(&self) {
        let envs = self.0.iter().map(|c| c.envs().p2p()).collect();
        crate::conductor::p2p_store::exchange_peer_info(envs);
    }
}

/// A useful Conductor abstraction for testing, allowing startup and shutdown as well
/// as easy installation of apps across multiple Conductors and Agents.
#[derive(Clone, derive_more::From)]
pub struct CoolConductor {
    handle: Option<Arc<CoolConductorHandle>>,
    envs: TestEnvironments,
    config: ConductorConfig,
    dnas: Arc<std::sync::Mutex<Vec<DnaFile>>>,
}

/// A wrapper around ConductorHandle with more convenient methods for testing
/// and a cleanup drop
#[derive(shrinkwraprs::Shrinkwrap, derive_more::From)]
pub struct CoolConductorHandle(pub(crate) ConductorHandle);

fn standard_config() -> ConductorConfig {
    let mut network = KitsuneP2pConfig::default();
    network.transport_pool = vec![kitsune_p2p::TransportConfig::Quic {
        bind_to: None,
        override_host: None,
        override_port: None,
    }];
    ConductorConfig {
        network: Some(network),
        ..Default::default()
    }
}

impl CoolConductor {
    /// Create a CoolConductor from an already-build ConductorHandle and environments
    pub fn new(
        handle: ConductorHandle,
        envs: TestEnvironments,
        config: ConductorConfig,
    ) -> CoolConductor {
        let handle = Arc::new(CoolConductorHandle(handle));
        Self {
            handle: Some(handle),
            envs,
            config,
            dnas: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    /// Create a CoolConductor with a new set of TestEnvironments from the given config
    pub async fn from_config(config: ConductorConfig) -> CoolConductor {
        let envs = test_environments();
        let handle = Self::from_existing(&envs, &config).await;
        Self::new(handle, envs, config)
    }

    /// Create a CoolConductor from a partially-configured ConductorBuilder
    pub async fn from_builder<DS: DnaStore + 'static>(
        builder: ConductorBuilder<DS>,
    ) -> CoolConductor {
        let envs = test_environments();
        let config = builder.config.clone();
        let handle = builder.test(&envs).await.unwrap();
        Self::new(handle, envs, config)
    }

    /// Create a handle from an existing environment and config
    async fn from_existing(envs: &TestEnvironments, config: &ConductorConfig) -> ConductorHandle {
        Conductor::builder()
            .config(config.clone())
            .test(envs)
            .await
            .unwrap()
    }

    /// Create a CoolConductor with a new set of TestEnvironments from the given config
    pub async fn from_standard_config() -> CoolConductor {
        Self::from_config(standard_config()).await
    }

    /// Access the TestEnvironments for this conductor
    pub fn envs(&self) -> &TestEnvironments {
        &self.envs
    }

    /// Access the KeystoreSender for this conductor
    pub fn keystore(&self) -> KeystoreSender {
        self.envs.keystore()
    }

    /// TODO: make this take a more flexible config for specifying things like
    /// membrane proofs
    async fn setup_app_inner(
        &self,
        installed_app_id: &str,
        agent: AgentPubKey,
        dna_files: &[DnaFile],
    ) -> CoolApp {
        let installed_app_id = installed_app_id.to_string();

        for dna_file in dna_files {
            self.install_dna(dna_file.clone())
                .await
                .expect("Could not install DNA");
            self.dnas.lock().unwrap().push(dna_file.clone());
        }

        let mut cool_cells = Vec::new();
        for dna in dna_files.iter() {
            let cell_id = CellId::new(dna.dna_hash().clone(), agent.clone());
            let cell_env = self
                .handle()
                .0
                .get_cell_env(&cell_id)
                .await
                .expect("Couldn't get cell environment");
            let cell = CoolCell { cell_id, cell_env };
            cool_cells.push(cell);
        }
        let installed_cells = cool_cells
            .iter()
            .map(|cell| {
                (
                    InstalledCell::new(
                        cell.cell_id().clone(),
                        format!("{}", cell.cell_id().dna_hash()),
                    ),
                    None,
                )
            })
            .collect();
        self.handle()
            .0
            .clone()
            .install_app(installed_app_id.clone(), installed_cells)
            .await
            .expect("Could not install app");
        CoolApp::new(installed_app_id, cool_cells)
    }

    /// Opinionated app setup.
    /// Creates an app for the given agent, using the given DnaFiles, with no extra configuration.
    pub async fn setup_app_for_agent(
        &self,
        installed_app_id: &str,
        agent: AgentPubKey,
        dna_files: &[DnaFile],
    ) -> CoolApp {
        let app = self
            .setup_app_inner(installed_app_id, agent, dna_files)
            .await;

        self.activate_app(app.installed_app_id().clone())
            .await
            .expect("Could not activate app");

        self.handle()
            .0
            .clone()
            .setup_cells()
            .await
            .expect("Could not setup cells");

        app
    }

    /// Opinionated app setup.
    /// Creates an app using the given DnaFiles, with no extra configuration.
    /// An AgentPubKey will be generated, and is accessible via the returned CoolApp.
    pub async fn setup_app(&self, installed_app_id: &str, dna_files: &[DnaFile]) -> CoolApp {
        let agent = CoolAgents::one(self.keystore()).await;
        self.setup_app_for_agent(installed_app_id, agent, dna_files)
            .await
    }

    /// Opinionated app setup. Creates one app per agent, using the given DnaFiles.
    ///
    /// All InstalledAppIds and CellNicks are auto-generated. In tests driven directly
    /// by Rust, you typically won't care what these values are set to, but in case you
    /// do, they are set as so:
    /// - InstalledAppId: {app_id_prefix}-{agent_pub_key}
    /// - CellNick: {dna_hash}
    ///
    /// Returns a batch of CoolApps, sorted in the same order as Agents passed in.
    pub async fn setup_app_for_agents(
        &self,
        app_id_prefix: &str,
        agents: &[AgentPubKey],
        dna_files: &[DnaFile],
    ) -> CoolAppBatch {
        let mut apps = Vec::new();
        for agent in agents.to_vec() {
            let installed_app_id = format!("{}{}", app_id_prefix, agent);
            let app = self
                .setup_app_inner(&installed_app_id, agent, dna_files)
                .await;
            apps.push(app);
        }

        for app in apps.iter() {
            self.activate_app(app.installed_app_id().clone())
                .await
                .expect("Could not activate app");
        }

        self.handle()
            .0
            .clone()
            .setup_cells()
            .await
            .expect("Could not setup cells");

        CoolAppBatch(apps)
    }

    /// Shutdown this conductor.
    /// This will wait for the conductor to shutdown but
    /// keep the inner state to restart it.
    ///
    /// Using this conductor without starting it up again will panic.
    pub async fn shutdown(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.wait_for_shutdown().await;
        } else {
            panic!("Attempted to shutdown conductor which was already shutdown");
        }
    }

    /// Start up this conductor if it's not already running.
    pub async fn startup(&mut self) {
        if self.handle.is_none() {
            self.handle = Some(Arc::new(CoolConductorHandle(
                Self::from_existing(&self.envs, &self.config).await,
            )));

            // MD: this feels wrong, why should we have to reinstall DNAs on restart?

            for dna_file in self.dnas.lock().unwrap().iter() {
                self.install_dna(dna_file.clone())
                    .await
                    .expect("Could not install DNA");
            }
        } else {
            panic!("Attempted to start conductor which was already started");
        }
    }

    /// Check if this conductor is running
    pub fn is_running(&self) -> bool {
        self.handle.is_some()
    }

    // NB: keep this private to prevent leaking out owned references
    fn handle(&self) -> Arc<CoolConductorHandle> {
        self.handle
            .clone()
            .expect("Tried to use a conductor that is offline")
    }
}

impl CoolConductorHandle {
    /// Call a zome function with automatic de/serialization of input and output
    /// and unwrapping of nested errors.
    async fn call_inner<I, O, F, E>(&self, invocation: ZomeCall) -> O
    where
        E: std::fmt::Debug,
        FunctionName: From<F>,
        SerializedBytes: TryFrom<I, Error = E>,
        O: TryFrom<SerializedBytes, Error = E> + std::fmt::Debug,
    {
        let response = self.0.call_zome(invocation).await.unwrap().unwrap();
        unwrap_to!(response => ZomeCallResponse::Ok)
            .clone()
            .into_inner()
            .try_into()
            .expect("Couldn't deserialize zome call output")
    }

    /// Make a zome call to a Cell, as if that Cell were the caller. Most common case.
    /// No capability is necessary, since the authorship capability is automatically granted.
    pub async fn call<I, O, F, E>(&self, zome: &CoolZome, fn_name: F, payload: I) -> O
    where
        E: std::fmt::Debug,
        FunctionName: From<F>,
        SerializedBytes: TryFrom<I, Error = E>,
        O: TryFrom<SerializedBytes, Error = E> + std::fmt::Debug,
    {
        self.call_from(zome.cell_id().agent_pubkey(), None, zome, fn_name, payload)
            .await
    }

    /// Make a zome call to a Cell, as if some other Cell were the caller. More general case.
    /// Can optionally provide a capability.
    pub async fn call_from<I, O, F, E>(
        &self,
        provenance: &AgentPubKey,
        cap: Option<CapSecret>,
        zome: &CoolZome,
        fn_name: F,
        payload: I,
    ) -> O
    where
        E: std::fmt::Debug,
        FunctionName: From<F>,
        SerializedBytes: TryFrom<I, Error = E>,
        O: TryFrom<SerializedBytes, Error = E> + std::fmt::Debug,
    {
        let payload = ExternInput::new(payload.try_into().expect("Couldn't serialize payload"));
        let call = ZomeCall {
            cell_id: zome.cell_id().clone(),
            zome_name: zome.name().clone(),
            fn_name: fn_name.into(),
            cap,
            provenance: provenance.clone(),
            payload,
        };
        let response = self.0.call_zome(call).await.unwrap().unwrap();
        unwrap_to!(response => ZomeCallResponse::Ok)
            .clone()
            .into_inner()
            .try_into()
            .expect("Couldn't deserialize zome call output")
    }

    /// Manually await shutting down the conductor.
    /// Conductors are already cleaned up on drop but this
    /// is useful if you need to know when it's finished cleaning up.
    pub async fn wait_for_shutdown(&self) {
        let c = &self.0;
        if let Some(shutdown) = c.take_shutdown_handle().await {
            c.shutdown().await;
            shutdown.await.expect("Failed to await shutdown handle");
        }
    }
}

// /// A top-level call into a zome function,
// /// i.e. coming from outside the Cell from an external Interface
// #[derive(Clone, Debug)]
// pub struct CoolZomeCall<'a, P, F, E>
// where
//     SerializedBytes: TryFrom<P, Error = E>,
//     E: std::fmt::Debug,
//     FunctionName: From<F>,
// {
//     /// The Id of the `Cell` in which this Zome-call would be invoked
//     pub cell_id: &'a CellId,
//     /// The Zome containing the function that would be invoked
//     pub zome: &'a Zome,
//     /// The capability request authorization.
//     /// This can be `None` and still succeed in the case where the function
//     /// in the zome being called has been given an Unrestricted status
//     /// via a `CapGrant`. Otherwise, it will be necessary to provide a `CapSecret` for every call.
//     pub cap: Option<CapSecret>,
//     /// The name of the Zome function to call
//     pub fn_name: F,
//     /// The data to be serialized and passed as an argument to the Zome call
//     pub payload: P,
//     /// If None, the AgentPubKey from the CellId is used (a common case)
//     pub provenance: Option<AgentPubKey>,
// }

// impl<'a, P, F, E> From<CoolZomeCall<'a, P, F, E>> for ZomeCallInvocation
// where
//     SerializedBytes: TryFrom<P, Error = E>,
//     E: std::fmt::Debug,
//     FunctionName: From<F>,
// {
//     fn from(czc: CoolZomeCall<'a, P, F, E>) -> Self {
//         let CoolZomeCall {
//             cell_id,
//             zome,
//             fn_name,
//             cap,
//             provenance,
//             payload,
//         } = czc;
//         let payload = ExternInput::new(payload.try_into().expect("Couldn't serialize payload"));
//         let provenance = provenance.unwrap_or_else(|| cell_id.agent_pubkey().clone());
//         ZomeCallInvocation {
//             cell_id: cell_id.clone(),
//             zome: zome.clone(),
//             fn_name: fn_name.into(),
//             cap,
//             provenance,
//             payload,
//         }
//     }
// }

// impl<'a, P, F, E> From<CoolZomeCall<'a, P, F, E>> for ZomeCall
// where
//     SerializedBytes: TryFrom<P, Error = E>,
//     E: std::fmt::Debug,
//     FunctionName: From<F>,
// {
//     fn from(czc: CoolZomeCall<'a, P, F, E>) -> Self {
//         ZomeCallInvocation::from(czc).into()
//     }
// }

impl Drop for CoolConductorHandle {
    fn drop(&mut self) {
        let c = self.0.clone();
        tokio::task::spawn(async move {
            // Shutdown the conductor
            if let Some(shutdown) = c.take_shutdown_handle().await {
                c.shutdown().await;
                shutdown.await.expect("Failed to await shutdown handle");
            }
        });
    }
}

// impl From<ConductorHandle> for CoolConductor {
//     fn from(h: ConductorHandle) -> Self {
//         CoolConductor(Arc::new(CoolConductorHandle(h)))
//     }
// }

impl AsRef<Arc<CoolConductorHandle>> for CoolConductor {
    fn as_ref(&self) -> &Arc<CoolConductorHandle> {
        self.handle
            .as_ref()
            .expect("Tried to use a conductor that is offline")
    }
}
impl std::ops::Deref for CoolConductor {
    type Target = Arc<CoolConductorHandle>;

    fn deref(&self) -> &Self::Target {
        self.handle
            .as_ref()
            .expect("Tried to use a conductor that is offline")
    }
}
impl std::borrow::Borrow<Arc<CoolConductorHandle>> for CoolConductor {
    fn borrow(&self) -> &Arc<CoolConductorHandle> {
        self.handle
            .as_ref()
            .expect("Tried to use a conductor that is offline")
    }
}

impl std::ops::Index<usize> for CoolConductorBatch {
    type Output = CoolConductor;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}
impl std::ops::IndexMut<usize> for CoolConductorBatch {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.0[index]
    }
}