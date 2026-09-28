#[cfg(feature = "trace")]
use std::num::NonZeroU64;

/// Where a [super::Reader] or [super::Writer] sits in its transport stream.
///
/// Only tracing reads it, so it is zero-sized without the `trace` feature.
#[derive(Clone, Copy, Default)]
pub(crate) struct Position {
	// The transport stream ID plus one. Stream ID 0 is valid, so the offset lets
	// `NonZeroU64` supply the niche that keeps this field one word. A plain
	// `Option<u64>` adds a word to every traced reader and writer, which pushes
	// enums that hold them, such as the lite subscriber's `Sub`, past clippy's
	// `large_enum_variant` limit.
	#[cfg(feature = "trace")]
	stream_id: Option<NonZeroU64>,
	#[cfg(feature = "trace")]
	offset: u64,
}

#[cfg(feature = "trace")]
impl Position {
	pub(crate) fn recv<S: web_transport_trait::RecvStream>(stream: &S) -> Self {
		Self::new(stream.stream_id())
	}

	pub(crate) fn send<S: web_transport_trait::SendStream>(stream: &S) -> Self {
		Self::new(stream.stream_id())
	}

	fn new(identity: Option<web_transport_trait::StreamId>) -> Self {
		Self {
			stream_id: identity
				.and_then(|identity| identity.id().checked_add(1))
				.and_then(NonZeroU64::new),
			offset: identity.map_or(0, |identity| identity.offset()),
		}
	}

	/// The transport stream ID, when the transport reports one.
	pub(crate) fn stream_id(&self) -> Option<u64> {
		self.stream_id.map(|stream_id| stream_id.get() - 1)
	}

	/// The transport stream byte offset reached so far.
	pub(crate) fn offset(&self) -> u64 {
		self.offset
	}

	#[inline]
	pub(crate) fn advance(&mut self, amount: usize) {
		self.offset += amount as u64;
	}
}

#[cfg(not(feature = "trace"))]
impl Position {
	pub(crate) fn recv<S: web_transport_trait::RecvStream>(_stream: &S) -> Self {
		Self {}
	}

	pub(crate) fn send<S: web_transport_trait::SendStream>(_stream: &S) -> Self {
		Self {}
	}

	pub(crate) fn stream_id(&self) -> Option<u64> {
		None
	}

	pub(crate) fn offset(&self) -> u64 {
		0
	}

	#[inline]
	pub(crate) fn advance(&mut self, _amount: usize) {}
}
