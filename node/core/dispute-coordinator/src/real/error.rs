// Copyright 2021 Parity Technologies (UK) Ltd.
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

use futures::channel::oneshot;
use thiserror::Error;

use polkadot_node_subsystem::{
	errors::{ChainApiError, RuntimeApiError},
	SubsystemError,
};
use polkadot_node_subsystem_util::{rolling_session_window::SessionsUnavailable, runtime};

use super::{db, participation};
use crate::real::{CodecError, LOG_TARGET};

#[fatality(splitable)]
pub enum Error {
	/// Errors coming from runtime::Runtime.
	#[fatal]
	#[error("Error while accessing runtime information {0}")]
	Runtime(#[from] runtime::Fatal),

	/// We received a legacy `SubystemError::Context` error which is considered fatal.
	#[fatal]
	#[error("SubsystemError::Context error: {0}")]
	SubsystemContext(String),

	/// `ctx.spawn` failed with an error.
	#[fatal]
	#[error("Spawning a task failed: {0}")]
	SpawnFailed(SubsystemError),

	#[fatal]
	#[error("Participation worker receiver exhausted.")]
	ParticipationWorkerReceiverExhausted,

	/// Receiving subsystem message from overseer failed.
	#[fatal]
	#[error("Receiving message from overseer failed: {0}")]
	SubsystemReceive(#[source] SubsystemError),

	#[fatal]
	#[error("Writing to database failed: {0}")]
	DbWriteFailed(std::io::Error),

	#[fatal]
	#[error("Oneshow for receiving block number from chain API got cancelled")]
	CanceledBlockNumber,

	#[fatal]
	#[error("Retrieving block number from chain API failed with error: {0}")]
	ChainApiBlockNumber(ChainApiError),

	#[error(transparent)]
	RuntimeApi(#[from] RuntimeApiError),

	#[error(transparent)]
	ChainApi(#[from] ChainApiError),

	#[error(transparent)]
	Io(#[from] std::io::Error),

	#[error(transparent)]
	Oneshot(#[from] oneshot::Canceled),

	#[error("Dispute import confirmation send failed (receiver canceled)")]
	DisputeImportOneshotSend,

	#[error(transparent)]
	Subsystem(SubsystemError),

	#[error(transparent)]
	Codec(#[from] CodecError),

	/// `RollingSessionWindow` was not able to retrieve `SessionInfo`s.
	#[error("Sessions unavailable in `RollingSessionWindow`: {0}")]
	RollingSessionWindow(#[from] SessionsUnavailable),

	/// Errors coming from runtime::Runtime.
	#[error("Error while accessing runtime information: {0}")]
	Runtime(#[from] runtime::NonFatal),

	#[error(transparent)]
	QueueError(#[from] participation::QueueError),
}

/// General `Result` type for dispute coordinator.
pub type Result<R> = std::result::Result<R, Error>;
/// Result type with only fatal errors.
pub type FatalResult<R> = std::result::Result<R, FatalError>;
/// Result type with only non fatal errors.
pub type JfyiResult<R> = std::result::Result<R, JfyiError>;

impl From<db::v1::Error> for Error {
	fn from(err: db::v1::Error) -> Self {
		match err {
			db::v1::Error::Io(io) => Self::from(JfyiError::Io(io)),
			db::v1::Error::Codec(e) => Self::from(JfyiError::Codec(e)),
		}
	}
}

/// Utility for eating top level errors and log them.
///
/// We basically always want to try and continue on error. This utility function is meant to
/// consume top-level errors by simply logging them
pub fn log_error(result: Result<()>) -> std::result::Result<(), FatalError> {
	match result.into_nested()? {
		Ok(()) => Ok(()),
		Err(jfyi) => {
			jfyi.log();
			Ok(())
		},
	}
}

impl JfyiError {
	/// Log a `NonFatal`.
	pub fn log(self) {
		match self {
			// don't spam the log with spurious errors
			Self::RuntimeApi(_) | Self::Oneshot(_) =>
				tracing::debug!(target: LOG_TARGET, error = ?self),
			// it's worth reporting otherwise
			_ => tracing::warn!(target: LOG_TARGET, error = ?self),
		}
	}
}
