// Copyright 2020 Parity Technologies (UK) Ltd.
// This file is part of Polkadot.

// Polkadot is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Polkadot is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Polkadot.  If not, see <http://www.gnu.org/licenses/>.

//! Availability Recovery Subsystem of Polkadot.

#![warn(missing_docs)]

use std::{
	collections::{HashMap, VecDeque},
	pin::Pin,
    time::Duration,
};

use futures::{
	channel::oneshot,
	future::{BoxFuture, FutureExt, RemoteHandle},
	pin_mut,
	prelude::*,
	stream::FuturesUnordered,
	task::{Context, Poll},
};
use lru::LruCache;
use rand::seq::SliceRandom;

use polkadot_erasure_coding::{branch_hash, branches, obtain_chunks_v1, recovery_threshold};
use polkadot_node_network_protocol::{
	request_response::{
		self as req_res, incoming, outgoing::RequestError, v1 as request_v1,
		IncomingRequestReceiver, OutgoingRequest, Recipient, Requests,
	},
	IfDisconnected, UnifiedReputationChange as Rep,
};
use polkadot_node_primitives::{AvailableData, ErasureChunk};
use polkadot_node_subsystem_util::{request_session_info, TimeoutExt};
use polkadot_primitives::v1::{
	AuthorityDiscoveryId, BlakeTwo256, BlockNumber, CandidateHash, CandidateReceipt, GroupIndex,
	Hash, HashT, SessionIndex, SessionInfo, ValidatorId, ValidatorIndex,
};
use polkadot_subsystem::{
	errors::RecoveryError,
	jaeger,
	messages::{AvailabilityRecoveryMessage, AvailabilityStoreMessage, NetworkBridgeMessage},
	overseer::{self, Subsystem},
	ActiveLeavesUpdate, FromOverseer, OverseerSignal, SpawnedSubsystem, SubsystemContext,
	SubsystemError, SubsystemResult, SubsystemSender,
};

mod error;

#[cfg(test)]
mod tests;

const LOG_TARGET: &str = "parachain::availability-recovery";

// How many parallel requests interaction should have going at once.
const N_PARALLEL: usize = 50;

// Size of the LRU cache where we keep recovered data.
const LRU_SIZE: usize = 16;

const COST_INVALID_REQUEST: Rep = Rep::CostMajor("Peer sent unparsable request");

/// Max time we want to wait for responses, before calling `launch_parallel_requests` again to fill
/// up slots.
const MAX_CHUNK_WAIT: Duration = Duration::from_secs(1);

/// The Availability Recovery Subsystem.
pub struct AvailabilityRecoverySubsystem {
	fast_path: bool,
	/// Receiver for available data requests.
	req_receiver: IncomingRequestReceiver<request_v1::AvailableDataFetchingRequest>,
}

struct RequestFromBackersPhase {
	// a random shuffling of the validators from the backing group which indicates the order
	// in which we connect to them and request the chunk.
	shuffled_backers: Vec<ValidatorIndex>,
}

struct RequestChunksPhase {
	// a random shuffling of the validators which indicates the order in which we connect to the validators and
	// request the chunk from them.
	shuffling: VecDeque<ValidatorIndex>,
	received_chunks: HashMap<ValidatorIndex, ErasureChunk>,
	requesting_chunks: FuturesUnordered<
		BoxFuture<'static, Result<Option<ErasureChunk>, (ValidatorIndex, RequestError)>>,
	>,
}

struct InteractionParams {
	/// Discovery ids of `validators`.
	validator_authority_keys: Vec<AuthorityDiscoveryId>,

	/// Validators relevant to this `Interaction`.
	validators: Vec<ValidatorId>,

	/// The number of pieces needed.
	threshold: usize,

	/// A hash of the relevant candidate.
	candidate_hash: CandidateHash,

	/// The root of the erasure encoding of the para block.
	erasure_root: Hash,
}

enum InteractionPhase {
	RequestFromBackers(RequestFromBackersPhase),
	RequestChunks(RequestChunksPhase),
}

/// A state of a single interaction reconstructing an available data.
struct Interaction<S> {
	sender: S,

	/// The parameters of the interaction.
	params: InteractionParams,

	/// The phase of the interaction.
	phase: InteractionPhase,
}

impl RequestFromBackersPhase {
	fn new(mut backers: Vec<ValidatorIndex>) -> Self {
		backers.shuffle(&mut rand::thread_rng());

		RequestFromBackersPhase { shuffled_backers: backers }
	}

	// Run this phase to completion.
	async fn run(
		&mut self,
		params: &InteractionParams,
		sender: &mut impl SubsystemSender,
	) -> Result<AvailableData, RecoveryError> {
		tracing::debug!(
			target: LOG_TARGET,
			candidate_hash = ?params.candidate_hash,
			erasure_root = ?params.erasure_root,
			"Requesting from backers",
		);
		loop {
            tracing::debug!(
                target: LOG_TARGET,
                candidate_hash = ?params.candidate_hash,
                erasure_root = ?params.erasure_root,
                "Entering from_backers loop.",
            );
			// Pop the next backer, and proceed to next phase if we're out.
			let validator_index =
				self.shuffled_backers.pop().ok_or_else(|| RecoveryError::Unavailable)?;
            tracing::debug!(
                target: LOG_TARGET,
                candidate_hash = ?params.candidate_hash,
                erasure_root = ?params.erasure_root,
                "Succeeded to get past Unavailable error.",
            );

			// Request data.
			let (req, res) = OutgoingRequest::new(
				Recipient::Authority(
					params.validator_authority_keys[validator_index.0 as usize].clone(),
				),
				req_res::v1::AvailableDataFetchingRequest { candidate_hash: params.candidate_hash },
			);

			sender.send_message(NetworkBridgeMessage::SendRequests(
				vec![Requests::AvailableDataFetching(req)],
				IfDisconnected::TryConnect,
			).into()).await;
            tracing::debug!(
                target: LOG_TARGET,
                candidate_hash = ?params.candidate_hash,
                erasure_root = ?params.erasure_root,
                "Succeeded in sending Available Data Fetching message.",
            );

			match res.await {
				Ok(req_res::v1::AvailableDataFetchingResponse::AvailableData(data)) => {
                    tracing::debug!(
                        target: LOG_TARGET,
                        candidate_hash = ?params.candidate_hash,
                        erasure_root = ?params.erasure_root,
                        "Data is available.",
                    );
					if reconstructed_data_matches_root(params.validators.len(), &params.erasure_root, &data) {
						tracing::debug!(
							target: LOG_TARGET,
							candidate_hash = ?params.candidate_hash,
							"Received full data",
						);

						return Ok(data)
					} else {
						tracing::debug!(
							target: LOG_TARGET,
							candidate_hash = ?params.candidate_hash,
							?validator_index,
							"Invalid data response",
						);

						// it doesn't help to report the peer with req/res.
					}
				}
				Ok(req_res::v1::AvailableDataFetchingResponse::NoSuchData) => {
                    tracing::debug!(
                        target: LOG_TARGET,
                        candidate_hash = ?params.candidate_hash,
                        erasure_root = ?params.erasure_root,
                        "DataFetching Response NoSuchData",
                    );
                }
				Err(e) => tracing::debug!(
					target: LOG_TARGET,
					candidate_hash = ?params.candidate_hash,
					?validator_index,
					err = ?e,
					"Error fetching full available data."
				),
			}
		}
	}
}

impl RequestChunksPhase {
	fn new(n_validators: u32) -> Self {
		let mut shuffling: Vec<_> = (0..n_validators).map(ValidatorIndex).collect();
		shuffling.shuffle(&mut rand::thread_rng());

		RequestChunksPhase {
			shuffling: shuffling.into(),
			received_chunks: HashMap::new(),
			requesting_chunks: FuturesUnordered::new(),
		}
	}

	fn is_unavailable(&self, params: &InteractionParams) -> bool {
		is_unavailable(
			self.received_chunks.len(),
			self.requesting_chunks.len(),
			self.shuffling.len(),
			params.threshold,
		)
	}

	fn can_conclude(&self, params: &InteractionParams) -> bool {
		self.received_chunks.len() >= params.threshold || self.is_unavailable(params)
	}

	async fn launch_parallel_requests(
		&mut self,
		params: &InteractionParams,
		sender: &mut impl SubsystemSender,
	) {
		let max_requests = std::cmp::min(N_PARALLEL, params.threshold);
        tracing::debug!(
            target: LOG_TARGET,
            candidate_hash = ?params.candidate_hash,
            "WE HAVE {:?} ongoing requests",
            self.requesting_chunks.len(),
        );
		while self.requesting_chunks.len() < max_requests {
            tracing::debug!(
                target: LOG_TARGET,
                candidate_hash = ?params.candidate_hash,
                "INNER WE HAVE {:?} ongoing requests",
                self.requesting_chunks.len(),
            );
			if let Some(validator_index) = self.shuffling.pop_back() {
                let now = std::time::Instant::now();
				let validator = params.validator_authority_keys[validator_index.0 as usize].clone();
				tracing::debug!(
					target: LOG_TARGET,
					?validator,
					?validator_index,
					candidate_hash = ?params.candidate_hash,
					"Requesting chunk",
				);

				// Request data.
				let raw_request = req_res::v1::ChunkFetchingRequest {
					candidate_hash: params.candidate_hash,
					index: validator_index,
				};

				let (req, res) = OutgoingRequest::new(
					Recipient::Authority(validator.clone()),
					raw_request.clone(),
				);

				sender.send_message(NetworkBridgeMessage::SendRequests(
					vec![Requests::ChunkFetching(req)],
					IfDisconnected::TryConnect,
				).into()).await;
                
                let candidate_hash = params.candidate_hash.clone();
				self.requesting_chunks.push(Box::pin(async move {
                    let output = res.await;

                    let after = std::time::Instant::now();
                    let elapsed = after.duration_since(now).as_millis();
                    if elapsed > std::time::Duration::from_secs(3).as_millis() {
                        tracing::debug!(
                            target: LOG_TARGET,
                            ?validator,
                            ?validator_index,
                            ?candidate_hash,
                            "TIMEOUT REACHED. CHUNK TIMED OUT.",
                        );
                    } else {
                        tracing::debug!(
                            target: LOG_TARGET,
                            ?validator,
                            ?validator_index,
                            ?candidate_hash,
                            "Response received after {:?} millis",
                            elapsed,
                        );
                    }
					match output {
						Ok(req_res::v1::ChunkFetchingResponse::Chunk(chunk))
							=> Ok(Some(chunk.recombine_into_chunk(&raw_request))),
						Ok(req_res::v1::ChunkFetchingResponse::NoSuchChunk) => Ok(None),
						Err(e) => Err((validator_index, e)),
					}
				}));
			} else {
				tracing::debug!(
					target: LOG_TARGET,
					candidate_hash = ?params.candidate_hash,
					"BREAKING BECAUSE WE HAVE {:?} ongoing requests",
                    self.requesting_chunks.len(),
				);
				break;
			}
		}
	}

	async fn wait_for_chunks(&mut self, params: &InteractionParams) {
        // We will also stop, if there has not been a response for `MAX_CHUNK_WAIT`, so
		// `launch_parallel_requests` cann fill up slots again.
		while let Some(request_result) =
			self.requesting_chunks.next().timeout(MAX_CHUNK_WAIT).await.flatten()
		{
            tracing::debug!(
                target: LOG_TARGET,
                candidate_hash = ?params.candidate_hash,
                "Looping for request_result",
            );
			match request_result {
				Ok(Some(chunk)) => {
                    tracing::debug!(
                        target: LOG_TARGET,
                        candidate_hash = ?params.candidate_hash,
                        "Got valid chunk",
                    );
					// Check merkle proofs of any received chunks.

					let validator_index = chunk.index;

					if let Ok(anticipated_hash) =
						branch_hash(&params.erasure_root, &chunk.proof, chunk.index.0 as usize)
					{
						let erasure_chunk_hash = BlakeTwo256::hash(&chunk.chunk);

						if erasure_chunk_hash != anticipated_hash {
							tracing::debug!(
								target: LOG_TARGET,
								candidate_hash = ?params.candidate_hash,
								?validator_index,
                                candidate_hash = ?params.candidate_hash,
								"Merkle proof mismatch",
							);
						} else {
							tracing::debug!(
								target: LOG_TARGET,
								candidate_hash = ?params.candidate_hash,
								?validator_index,
                                candidate_hash = ?params.candidate_hash,
								"Received valid chunk.",
							);
							self.received_chunks.insert(validator_index, chunk);
						}
					} else {
						tracing::debug!(
							target: LOG_TARGET,
							candidate_hash = ?params.candidate_hash,
							?validator_index,
                            candidate_hash = ?params.candidate_hash,
							"Invalid Merkle proof",
						);
					}
				}
				Ok(None) => {
					tracing::debug!(
						target: LOG_TARGET,
                        candidate_hash = ?params.candidate_hash,
						"Reached Ok(None)",
					);
                }
				Err((validator_index, e)) => {
					tracing::debug!(
						target: LOG_TARGET,
						candidate_hash= ?params.candidate_hash,
						err = ?e,
						?validator_index,
                        candidate_hash = ?params.candidate_hash,
						"Failure requesting chunk",
					);

					match e {
						RequestError::InvalidResponse(_) => {},
						RequestError::NetworkError(_) | RequestError::Canceled(_) => {
							self.shuffling.push_front(validator_index);
						},
					}
				},
			}

			// Stop waiting for requests when we either can already recover the data
			// or have gotten firm 'No' responses from enough validators.
			if self.can_conclude(params) {
				break
			}
		}
	}

	async fn run(
		&mut self,
		params: &InteractionParams,
		sender: &mut impl SubsystemSender,
	) -> Result<AvailableData, RecoveryError> {
		// First query the store for any chunks we've got.
		{
			let (tx, rx) = oneshot::channel();
			sender
				.send_message(
					AvailabilityStoreMessage::QueryAllChunks(params.candidate_hash, tx).into(),
				)
				.await;

			match rx.await {
				Ok(chunks) => {
                    tracing::debug!(
                        target: LOG_TARGET,
                        candidate_hash = ?params.candidate_hash,
					    erasure_root = ?params.erasure_root,
                        "Availability store respode"
                    );
					// This should either be length 1 or 0. If we had the whole data,
					// we wouldn't have reached this stage.
					let chunk_indices: Vec<_> = chunks.iter().map(|c| c.index).collect();
					self.shuffling.retain(|i| !chunk_indices.contains(i));

					for chunk in chunks {
                        tracing::debug!(
                            target: LOG_TARGET,
                            candidate_hash = ?params.candidate_hash,
					        erasure_root = ?params.erasure_root,
                            "Got chunks from the availability store"
                        );
						self.received_chunks.insert(chunk.index, chunk);
					}
				},
				Err(oneshot::Canceled) => {
					tracing::debug!(
						target: LOG_TARGET,
						candidate_hash = ?params.candidate_hash,
						"Failed to reach the availability store"
					);
				},
			}
		}

		loop {
			if self.is_unavailable(&params) {
				tracing::debug!(
					target: LOG_TARGET,
					candidate_hash = ?params.candidate_hash,
					erasure_root = ?params.erasure_root,
					received = %self.received_chunks.len(),
					requesting = %self.requesting_chunks.len(),
					n_validators = %params.validators.len(),
					"Data recovery is not possible",
				);

				return Err(RecoveryError::Unavailable)
			}
            
            tracing::debug!(
                target: LOG_TARGET,
                candidate_hash = ?params.candidate_hash,
                erasure_root = ?params.erasure_root,
                received = %self.received_chunks.len(),
                requesting = %self.requesting_chunks.len(),
                n_validators = %params.validators.len(),
                "LOOPING for launch_parallel_requests.",
            );

			self.launch_parallel_requests(params, sender).await;
			self.wait_for_chunks(params).await;

			// If received_chunks has more than threshold entries, attempt to recover the data.
			// If that fails, or a re-encoding of it doesn't match the expected erasure root,
			// return Err(RecoveryError::Invalid)
			if self.received_chunks.len() >= params.threshold {
				return match polkadot_erasure_coding::reconstruct_v1(
					params.validators.len(),
					self.received_chunks.values().map(|c| (&c.chunk[..], c.index.0 as usize)),
				) {
					Ok(data) => {
						if reconstructed_data_matches_root(params.validators.len(), &params.erasure_root, &data) {
							tracing::debug!(
								target: LOG_TARGET,
								candidate_hash = ?params.candidate_hash,
								erasure_root = ?params.erasure_root,
								"Data recovery complete",
							);

							Ok(data)
						} else {
							tracing::debug!(
								target: LOG_TARGET,
								candidate_hash = ?params.candidate_hash,
								erasure_root = ?params.erasure_root,
								"Data recovery - root mismatch",
							);

							Err(RecoveryError::Invalid)
						}
					},
					Err(err) => {
						tracing::debug!(
							target: LOG_TARGET,
							candidate_hash = ?params.candidate_hash,
							erasure_root = ?params.erasure_root,
							?err,
							"Data recovery error ",
						);

						Err(RecoveryError::Invalid)
					},
				};
			} else {
                tracing::debug!(
                    target: LOG_TARGET,
                    candidate_hash = ?params.candidate_hash,
                    erasure_root = ?params.erasure_root,
                    received = %self.received_chunks.len(),
                    requesting = %self.requesting_chunks.len(),
                    n_validators = %params.validators.len(),
                    "Have not received enough chunks.",
                );
            }
		}
	}
}

const fn is_unavailable(
	received_chunks: usize,
	requesting_chunks: usize,
	unrequested_validators: usize,
	threshold: usize,
) -> bool {
	received_chunks + requesting_chunks + unrequested_validators < threshold
}

fn reconstructed_data_matches_root(
	n_validators: usize,
	expected_root: &Hash,
	data: &AvailableData,
) -> bool {
	let chunks = match obtain_chunks_v1(n_validators, data) {
		Ok(chunks) => chunks,
		Err(e) => {
			tracing::debug!(
				target: LOG_TARGET,
				err = ?e,
				"Failed to obtain chunks",
			);
			return false
		},
	};

	let branches = branches(&chunks);

	branches.root() == *expected_root
}

impl<S: SubsystemSender> Interaction<S> {
	async fn run(mut self) -> Result<AvailableData, RecoveryError> {
		// First just see if we have the data available locally.
		{
			let (tx, rx) = oneshot::channel();
			self.sender
				.send_message(
					AvailabilityStoreMessage::QueryAvailableData(self.params.candidate_hash, tx)
						.into(),
				)
				.await;

			match rx.await {
				Ok(Some(data)) => {
					tracing::debug!(
						target: LOG_TARGET,
						candidate_hash = ?self.params.candidate_hash,
                        "Data is available. Succeeded.",
					);
                    return Ok(data);
                }
				Ok(None) => {
					tracing::debug!(
						target: LOG_TARGET,
						candidate_hash = ?self.params.candidate_hash,
                        "Data is None",
					)
                }
				Err(oneshot::Canceled) => {
					tracing::debug!(
						target: LOG_TARGET,
						candidate_hash = ?self.params.candidate_hash,
						"Failed to reach the availability store",
					)
				},
			}
		}

		loop {
			// These only fail if we cannot reach the underlying subsystem, which case there is nothing
			// meaningful we can do.
			match self.phase {
				InteractionPhase::RequestFromBackers(ref mut from_backers) => {
					tracing::debug!(
						target: LOG_TARGET,
						candidate_hash = ?self.params.candidate_hash,
                        "Requesting from Backers",
					);
					match from_backers.run(&self.params, &mut self.sender).await {
						Ok(data) => {
                            tracing::debug!(
                                target: LOG_TARGET,
                                candidate_hash = ?self.params.candidate_hash,
                                "From Backers has returned",
                            );
                            break Ok(data)
                        },
						Err(RecoveryError::Invalid) => {
                            tracing::debug!(
                                target: LOG_TARGET,
                                candidate_hash = ?self.params.candidate_hash,
                                "RecoveryError Invalid encountered",
                            );
                            break Err(RecoveryError::Invalid)
                        }
						Err(RecoveryError::Unavailable) => {
                            tracing::debug!(
                                target: LOG_TARGET,
                                candidate_hash = ?self.params.candidate_hash,
                                "RecoveryError Unavailable encountered",
                            );
							self.phase = InteractionPhase::RequestChunks(
								RequestChunksPhase::new(self.params.validators.len() as _)
							)
						}
					}
				}
				InteractionPhase::RequestChunks(ref mut from_all) => {
                    tracing::debug!(
                        target: LOG_TARGET,
                        candidate_hash = ?self.params.candidate_hash,
                        "Request Chunks encountered",
                    );
					break from_all.run(&self.params, &mut self.sender).await
				}
			}
		}
	}
}

/// Accumulate all awaiting sides for some particular `AvailableData`.
struct InteractionHandle {
	candidate_hash: CandidateHash,
	remote: RemoteHandle<Result<AvailableData, RecoveryError>>,
	awaiting: Vec<oneshot::Sender<Result<AvailableData, RecoveryError>>>,
}

impl Future for InteractionHandle {
	type Output = Option<(CandidateHash, Result<AvailableData, RecoveryError>)>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let mut indices_to_remove = Vec::new();
		for (i, awaiting) in self.awaiting.iter_mut().enumerate().rev() {
			if let Poll::Ready(()) = awaiting.poll_canceled(cx) {
				indices_to_remove.push(i);
			}
		}

		// these are reverse order, so remove is fine.
		for index in indices_to_remove {
			tracing::debug!(
				target: LOG_TARGET,
				candidate_hash = ?self.candidate_hash,
				"Receiver for available data dropped.",
			);

			self.awaiting.swap_remove(index);
		}

		if self.awaiting.is_empty() {
			tracing::debug!(
				target: LOG_TARGET,
				candidate_hash = ?self.candidate_hash,
				"All receivers for available data dropped.",
			);

			return Poll::Ready(None)
		}

		let remote = &mut self.remote;
		futures::pin_mut!(remote);
		let result = futures::ready!(remote.poll(cx));

		for awaiting in self.awaiting.drain(..) {
			let _ = awaiting.send(result.clone());
		}

		Poll::Ready(Some((self.candidate_hash, result)))
	}
}

struct State {
	/// Each interaction is implemented as its own async task,
	/// and these handles are for communicating with them.
	interactions: FuturesUnordered<InteractionHandle>,

	/// A recent block hash for which state should be available.
	live_block: (BlockNumber, Hash),

	/// An LRU cache of recently recovered data.
	availability_lru: LruCache<CandidateHash, Result<AvailableData, RecoveryError>>,
}

impl Default for State {
	fn default() -> Self {
		Self {
			interactions: FuturesUnordered::new(),
			live_block: (0, Hash::default()),
			availability_lru: LruCache::new(LRU_SIZE),
		}
	}
}

impl<Context> Subsystem<Context, SubsystemError> for AvailabilityRecoverySubsystem
where
	Context: SubsystemContext<Message = AvailabilityRecoveryMessage>,
	Context: overseer::SubsystemContext<Message = AvailabilityRecoveryMessage>,
{
	fn start(self, ctx: Context) -> SpawnedSubsystem {
		let future = self
			.run(ctx)
			.map_err(|e| SubsystemError::with_origin("availability-recovery", e))
			.boxed();
		SpawnedSubsystem { name: "availability-recovery-subsystem", future }
	}
}

/// Handles a signal from the overseer.
async fn handle_signal(state: &mut State, signal: OverseerSignal) -> SubsystemResult<bool> {
	match signal {
		OverseerSignal::Conclude => Ok(true),
		OverseerSignal::ActiveLeaves(ActiveLeavesUpdate { activated, .. }) => {
			// if activated is non-empty, set state.live_block to the highest block in `activated`
			for activated in activated {
				if activated.number > state.live_block.0 {
					state.live_block = (activated.number, activated.hash)
				}
			}

			Ok(false)
		},
		OverseerSignal::BlockFinalized(_, _) => Ok(false),
	}
}

/// Machinery around launching interactions into the background.
async fn launch_interaction<Context>(
	state: &mut State,
	ctx: &mut Context,
	session_info: SessionInfo,
	receipt: CandidateReceipt,
	backing_group: Option<GroupIndex>,
	response_sender: oneshot::Sender<Result<AvailableData, RecoveryError>>,
) -> error::Result<()>
where
	Context: SubsystemContext<Message = AvailabilityRecoveryMessage>,
	Context: overseer::SubsystemContext<Message = AvailabilityRecoveryMessage>,
{
	let candidate_hash = receipt.hash();
    tracing::debug!(
        target: LOG_TARGET,
        ?candidate_hash,
        "Interaction launched.",
    );

	let params = InteractionParams {
		validator_authority_keys: session_info.discovery_keys.clone(),
		validators: session_info.validators.clone(),
		threshold: recovery_threshold(session_info.validators.len())?,
		candidate_hash,
		erasure_root: receipt.descriptor.erasure_root,
	};

	let phase = backing_group
		.and_then(|g| session_info.validator_groups.get(g.0 as usize))
		.map(|group| {
			InteractionPhase::RequestFromBackers(RequestFromBackersPhase::new(group.clone()))
		})
		.unwrap_or_else(|| {
			InteractionPhase::RequestChunks(RequestChunksPhase::new(params.validators.len() as _))
		});

	let interaction = Interaction { sender: ctx.sender().clone(), params, phase };

	let (remote, remote_handle) = interaction.run().remote_handle();

    tracing::debug!(
        target: LOG_TARGET,
        ?candidate_hash,
        "Creating Interaction handle.",
    );

	state.interactions.push(InteractionHandle {
		candidate_hash,
		remote: remote_handle,
		awaiting: vec![response_sender],
	});

	if let Err(e) = ctx.spawn("recovery interaction", Box::pin(remote)) {
		tracing::debug!(
			target: LOG_TARGET,
			err = ?e,
			"Failed to spawn a recovery interaction task",
		);
	} else {
        tracing::debug!(
            target: LOG_TARGET,
            ?candidate_hash,
            "Recovery interaction task spawned successfully.",
        );
    }

	Ok(())
}

/// Handles an availability recovery request.
async fn handle_recover<Context>(
	state: &mut State,
	ctx: &mut Context,
	receipt: CandidateReceipt,
	session_index: SessionIndex,
	backing_group: Option<GroupIndex>,
	response_sender: oneshot::Sender<Result<AvailableData, RecoveryError>>,
) -> error::Result<()>
where
	Context: SubsystemContext<Message = AvailabilityRecoveryMessage>,
	Context: overseer::SubsystemContext<Message = AvailabilityRecoveryMessage>,
{
	let candidate_hash = receipt.hash();
    tracing::debug!(
        target: LOG_TARGET,
        ?candidate_hash,
        "Entering handle recovery function.",
    );

	let span = jaeger::Span::new(candidate_hash, "availbility-recovery")
		.with_stage(jaeger::Stage::AvailabilityRecovery);

	if let Some(result) = state.availability_lru.get(&candidate_hash) {
		if let Err(e) = response_sender.send(result.clone()) {
			tracing::warn!(
				target: LOG_TARGET,
				err = ?e,
				"Error responding with an availability recovery result",
			);
		} else {
            tracing::debug!(
                target: LOG_TARGET,
                ?candidate_hash,
                "Response of result succeeded.",
            );
        }
		return Ok(());
	} else {
        tracing::debug!(
            target: LOG_TARGET,
            ?candidate_hash,
            "Candidate not in availability LRU.",
        );
    }

	if let Some(i) = state.interactions.iter_mut().find(|i| i.candidate_hash == candidate_hash) {
        tracing::debug!(
            target: LOG_TARGET,
            ?candidate_hash,
            "Candidate hash found, pushing response sender.",
        );
		i.awaiting.push(response_sender);
		return Ok(());
	} else {
        tracing::debug!(
            target: LOG_TARGET,
            ?candidate_hash,
            "Candidate hash not found in interactions.",
        );
    }

	let _span = span.child("not-cached");
	let session_info = request_session_info(state.live_block.1, session_index, ctx.sender())
		.await
		.await
		.map_err(error::Error::CanceledSessionInfo)??;

	let _span = span.child("session-info-ctx-received");
	match session_info {
		Some(session_info) => {
            tracing::debug!(
                target: LOG_TARGET,
                ?candidate_hash,
                "Launching Interaction.",
            );
			launch_interaction(
				state,
				ctx,
				session_info,
				receipt,
				backing_group,
				response_sender,
			).await
		}
		None => {
			tracing::debug!(
				target: LOG_TARGET,
                ?candidate_hash,
				"SessionInfo is `None` at {:?}", state.live_block,
			);
			response_sender
				.send(Err(RecoveryError::Unavailable))
				.map_err(|_| error::Error::CanceledResponseSender)?;
			Ok(())
		},
	}
}

/// Queries a chunk from av-store.
async fn query_full_data<Context>(
	ctx: &mut Context,
	candidate_hash: CandidateHash,
) -> error::Result<Option<AvailableData>>
where
	Context: SubsystemContext<Message = AvailabilityRecoveryMessage>,
	Context: overseer::SubsystemContext<Message = AvailabilityRecoveryMessage>,
{
	let (tx, rx) = oneshot::channel();
	ctx.send_message(AvailabilityStoreMessage::QueryAvailableData(candidate_hash, tx))
		.await;

	Ok(rx.await.map_err(error::Error::CanceledQueryFullData)?)
}

impl AvailabilityRecoverySubsystem {
	/// Create a new instance of `AvailabilityRecoverySubsystem` which starts with a fast path to request data from backers.
	pub fn with_fast_path(
		req_receiver: IncomingRequestReceiver<request_v1::AvailableDataFetchingRequest>,
	) -> Self {
		Self { fast_path: true, req_receiver }
	}

	/// Create a new instance of `AvailabilityRecoverySubsystem` which requests only chunks
	pub fn with_chunks_only(
		req_receiver: IncomingRequestReceiver<request_v1::AvailableDataFetchingRequest>,
	) -> Self {
		Self { fast_path: false, req_receiver }
	}

	async fn run<Context>(self, mut ctx: Context) -> SubsystemResult<()>
	where
		Context: SubsystemContext<Message = AvailabilityRecoveryMessage>,
		Context: overseer::SubsystemContext<Message = AvailabilityRecoveryMessage>,
	{
		let mut state = State::default();
		let Self { fast_path, mut req_receiver } = self;

		loop {
			let recv_req = req_receiver.recv(|| vec![COST_INVALID_REQUEST]).fuse();
			pin_mut!(recv_req);
			futures::select! {
				v = ctx.recv().fuse() => {
					match v? {
						FromOverseer::Signal(signal) => if handle_signal(
							&mut state,
							signal,
						).await? {
							return Ok(());
						}
						FromOverseer::Communication { msg } => {
							match msg {
								AvailabilityRecoveryMessage::RecoverAvailableData(
									receipt,
									session_index,
									maybe_backing_group,
									response_sender,
								) => {
									if let Err(e) = handle_recover(
										&mut state,
										&mut ctx,
										receipt,
										session_index,
										maybe_backing_group.filter(|_| fast_path),
										response_sender,
									).await {
										tracing::warn!(
											target: LOG_TARGET,
											err = ?e,
											"Error handling a recovery request",
										);
									}
								}
							}
						}
					}
				}
				in_req = recv_req => {
					match in_req {
						Ok(req) => {
							match query_full_data(&mut ctx, req.payload.candidate_hash).await {
								Ok(res) => {
									let _ = req.send_response(res.into());
								}
								Err(e) => {
									tracing::debug!(
										target: LOG_TARGET,
										err = ?e,
										"Failed to query available data.",
									);

									let _ = req.send_response(None.into());
								}
							}
						}
						Err(incoming::Error::Fatal(f)) => return Err(SubsystemError::with_origin("availability-recovery", f)),
						Err(incoming::Error::NonFatal(err)) => {
							tracing::debug!(
								target: LOG_TARGET,
								?err,
								"Decoding incoming request failed"
							);
							continue
						}
					}
				}
				output = state.interactions.select_next_some() => {
					if let Some((candidate_hash, result)) = output {
						state.availability_lru.put(candidate_hash, result);
					}
				}
			}
		}
	}
}
