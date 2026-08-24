#[cfg(feature = "trace")]
use std::num::NonZeroU64;

use std::fmt::Debug;

use crate::{Error, coding::*, ietf};

/// A wrapper around a [web_transport_trait::SendStream] that will reset on Drop.
pub struct Writer<S: web_transport_trait::SendStream, V> {
	stream: Option<S>,
	buffer: bytes::BytesMut,
	version: V,
	#[cfg(feature = "trace")]
	stream_id: Option<NonZeroU64>,
	#[cfg(feature = "trace")]
	offset: u64,
}

impl<S: web_transport_trait::SendStream, V> Writer<S, V> {
	/// Create a new writer for the given stream and version.
	pub fn new(stream: S, version: V) -> Self {
		#[cfg(feature = "trace")]
		let identity = stream.stream_id();
		Self {
			stream: Some(stream),
			buffer: Default::default(),
			version,
			#[cfg(feature = "trace")]
			stream_id: identity
				.and_then(|identity| identity.id().checked_add(1))
				.and_then(NonZeroU64::new),
			#[cfg(feature = "trace")]
			offset: identity.map_or(0, |identity| identity.offset()),
		}
	}

	/// Return the underlying transport stream ID, when available.
	#[cfg(feature = "trace")]
	pub(crate) fn stream_id(&self) -> Option<u64> {
		self.stream_id.map(|stream_id| stream_id.get() - 1)
	}

	#[cfg(not(feature = "trace"))]
	pub(crate) fn stream_id(&self) -> Option<u64> {
		None
	}

	/// Return the transport stream byte offset written by this writer.
	#[cfg(feature = "trace")]
	pub(crate) fn offset(&self) -> u64 {
		self.offset
	}

	#[cfg(not(feature = "trace"))]
	pub(crate) fn offset(&self) -> u64 {
		0
	}

	#[inline]
	fn advance(&mut self, amount: usize) {
		#[cfg(feature = "trace")]
		{
			self.offset += amount as u64;
		}
		#[cfg(not(feature = "trace"))]
		let _ = amount;
	}

	/// Encode the given message to the stream.
	pub async fn encode<T: Encode<V> + Debug>(&mut self, msg: &T) -> Result<(), Error>
	where
		V: Clone,
	{
		self.buffer.clear();
		msg.encode(&mut self.buffer, self.version.clone())?;

		while !self.buffer.is_empty() {
			let n = self
				.stream
				.as_mut()
				.unwrap()
				.write_buf(&mut self.buffer)
				.await
				.map_err(Error::from_transport)?;
			self.advance(n);
		}

		Ok(())
	}

	pub(crate) async fn write<Buf: bytes::Buf + Send>(&mut self, buf: &mut Buf) -> Result<usize, Error> {
		let n = self
			.stream
			.as_mut()
			.unwrap()
			.write_buf(buf)
			.await
			.map_err(Error::from_transport)?;
		self.advance(n);
		Ok(n)
	}

	/// Write the entire `Buf` to the stream.
	///
	/// NOTE: This can avoid performing a copy when using `Bytes`.
	pub async fn write_all<Buf: bytes::Buf + Send>(&mut self, buf: &mut Buf) -> Result<(), Error> {
		while buf.has_remaining() {
			self.write(buf).await?;
		}
		Ok(())
	}

	/// Write the entire [`bytes::Bytes`] chunk to the stream.
	pub async fn write_chunk(&mut self, chunk: bytes::Bytes) -> Result<(), Error> {
		let len = chunk.len();
		self.stream
			.as_mut()
			.unwrap()
			.write_chunk(chunk)
			.await
			.map_err(Error::from_transport)?;
		self.advance(len);
		Ok(())
	}

	/// Mark the stream as finished.
	pub fn finish(&mut self) -> Result<(), Error> {
		self.stream.as_mut().unwrap().finish().map_err(Error::from_transport)
	}

	/// Abort the stream with the given error.
	pub fn abort(&mut self, err: &Error) {
		self.stream.as_mut().unwrap().reset(err.to_code());
	}

	/// Wait for the stream to be closed, or the [Self::finish] to be acknowledged by the peer.
	pub async fn closed(&mut self) -> Result<(), Error> {
		self.stream
			.as_mut()
			.unwrap()
			.closed()
			.await
			.map_err(Error::from_transport)?;
		Ok(())
	}

	/// Set the priority of the stream.
	pub fn set_priority(&mut self, priority: u8) {
		self.stream.as_mut().unwrap().set_priority(priority);
	}

	/// Cast the writer to a different version, used during version negotiation.
	pub fn with_version<O>(mut self, version: O) -> Writer<S, O> {
		Writer {
			// We need to use an Option so Drop doesn't reset the stream.
			stream: self.stream.take(),
			buffer: std::mem::take(&mut self.buffer),
			version,
			#[cfg(feature = "trace")]
			stream_id: self.stream_id,
			#[cfg(feature = "trace")]
			offset: self.offset,
		}
	}
}

impl<S: web_transport_trait::SendStream> Writer<S, ietf::Version> {
	/// Encode an IETF `Message` to the stream, writing `[type_id][size][body]`.
	pub async fn encode_message<T: ietf::Message>(&mut self, msg: &T) -> Result<(), Error> {
		self.encode(&T::ID).await?;
		self.encode(msg).await
	}
}

impl<S: web_transport_trait::SendStream, V> Drop for Writer<S, V> {
	fn drop(&mut self) {
		if let Some(mut stream) = self.stream.take() {
			// Unlike the Quinn default, we abort the stream on drop.
			stream.reset(Error::Cancel.to_code());
		}
	}
}

#[cfg(all(test, feature = "trace"))]
mod tests {
	use super::*;
	use crate::coding::test;

	#[tokio::test]
	async fn transport_identity_uses_transport_offset() {
		let mut writer = Writer::new(test::SendStream, ());

		assert_eq!(writer.stream_id(), Some(17));
		assert_eq!(writer.offset(), 3);
		writer.write_chunk(bytes::Bytes::from_static(b"hello")).await.unwrap();
		assert_eq!(writer.offset(), 8);
	}
}
