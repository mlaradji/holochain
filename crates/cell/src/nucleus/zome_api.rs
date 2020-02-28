use super::{ZomeInvocationResult, ZomeInvocation, error::ZomeApiResult};
use crate::{agent::SourceChainCommitBundle, net::NetRequester, cell::ZomeId};
use sx_types::prelude::*;

pub trait ZomeApi {
    fn call(&self, invocation: ZomeInvocation) -> ZomeApiResult<ZomeInvocationResult>;
    // fn commit_capability_claim();
    // fn commit_capability_grant();
    // fn commit_entry();
    // fn commit_entry_result();
    // fn debug();
    // fn decrypt();
    // fn emit_signal();
    // fn encrypt();
    // fn entry_address();
    // // fn entry_type_properties();
    // fn get_entry();
    // // fn get_entry_history();
    // // fn get_entry_initial();
    // fn get_entry_results();

    // fn get_links();
    // // et al...

    // fn link_entries();
    // fn property(); // --> get_property ?
    // fn query();
    // fn query_result();
    // fn remove_link();
    // fn send();
    // fn sign();
    // fn sign_one_time();
    // fn sleep();
    // fn verify_signature();
    // fn remove_entry();
    // // fn update_agent();
    // fn update_entry();
    // fn version();
    // fn version_hash();
}

pub struct ZomeEnvironment<N: NetRequester> {
    bundle: SourceChainCommitBundle,
    net_requester: N,
    zome_id: ZomeId,
}

impl<N: NetRequester> ZomeEnvironment<N> {
    pub fn new(bundle: SourceChainCommitBundle, net_requester: N, zome_id: ZomeId) -> Self {
        Self {
            bundle,
            net_requester,
            zome_id,
        }
    }
}

impl<N: NetRequester> ZomeApi for ZomeEnvironment<N> {
    fn call(&self, invocation: ZomeInvocation) -> ZomeApiResult<ZomeInvocationResult> {
        unimplemented!()
    }
}