use std::{cmp, fmt::Debug, io};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{Error, coding::*};

/// A reader for decoding messages from a stream.
pub struct Reader<S: web_transport_trait::RecvStream, V> {
	stream: S,
	buffer: BytesMut,
	version: V,
	stream_id: Option<u64>,
	offset: u64,
}

impl<S: web_transport_trait::RecvStream, V> Reader<S, V> {
	pub fn new(stream: S, version: V) -> Self {
		let identity = stream.stream_id();
		Self {
			stream,
			buffer: Default::default(),
			version,
			stream_id: identity.map(|identity| identity.id()),
			offset: identity.map_or(0, |identity| identity.offset()),
		}
	}

	/// Return the underlying transport stream ID, when available.
	pub fn stream_id(&self) -> Option<u64> {
		self.stream_id
	}

	/// Return the transport stream byte offset consumed by this reader.
	pub fn offset(&self) -> u64 {
		self.offset
	}

	/// Decode the next message from the stream.
	pub async fn decode<T: Decode<V> + Debug>(&mut self) -> Result<T, Error>
	where
		V: Clone,
	{
		loop {
			let mut cursor = io::Cursor::new(&self.buffer);
			match T::decode(&mut cursor, self.version.clone()) {
				Ok(msg) => {
					let consumed = cursor.position();
					self.buffer.advance(consumed as usize);
					self.offset += consumed;
					return Ok(msg);
				}
				Err(DecodeError::Short) => {
					// Try to read more data
					if !self.read_more().await? {
						// Stream closed while we still need more data
						return Err(DecodeError::Short.into());
					}
				}
				Err(e) => return Err(e.into()),
			}
		}
	}

	/// Decode the next message unless the stream is closed.
	///
	/// Cancel-safe with the transports we ship (`web_transport_quinn`, `qmux`).
	/// The only `.await` points are reads from the underlying transport; partial
	/// bytes accumulate in `self.buffer` and a re-entry resumes decoding from
	/// the same position, so dropping the future mid-message never desynchronizes
	/// the stream. This requires the transport's `read_buf` to be cancel-safe
	/// (Quinn's `RecvStream::read` is documented as such, and qmux's
	/// `RecvStream::read_chunk` is a `tokio::sync::mpsc::Receiver::recv`).
	/// New transport impls must preserve this property.
	pub async fn decode_maybe<T: Decode<V> + Debug>(&mut self) -> Result<Option<T>, Error>
	where
		V: Clone,
	{
		if !self.has_more().await? {
			return Ok(None);
		}

		Ok(Some(self.decode().await?))
	}

	/// Decode the next message from the stream without consuming it.
	pub async fn decode_peek<T: Decode<V> + Debug>(&mut self) -> Result<T, Error>
	where
		V: Clone,
	{
		loop {
			let mut cursor = io::Cursor::new(&self.buffer);
			match T::decode(&mut cursor, self.version.clone()) {
				Ok(msg) => return Ok(msg),
				Err(DecodeError::Short) => {
					// Try to read more data
					if !self.read_more().await? {
						// Stream closed while we still need more data
						return Err(DecodeError::Short.into());
					}
				}
				Err(e) => return Err(e.into()),
			}
		}
	}

	/// Read the next chunk, draining the reader's internal buffer first.
	pub async fn read_chunk(&mut self, max: usize) -> Result<Option<Bytes>, Error> {
		if !self.buffer.is_empty() {
			let n = cmp::min(self.buffer.len(), max);
			self.offset += n as u64;
			return Ok(Some(self.buffer.split_to(n).freeze()));
		}
		let chunk = self.stream.read_chunk(max).await.map_err(Error::from_transport)?;
		if let Some(chunk) = &chunk {
			self.offset += chunk.len() as u64;
		}
		Ok(chunk)
	}

	/// Read exactly the given number of bytes from the stream.
	pub async fn read_exact(&mut self, size: usize) -> Result<Bytes, Error> {
		// An optimization to avoid a copy if we have enough data in the buffer
		if self.buffer.len() >= size {
			self.offset += size as u64;
			return Ok(self.buffer.split_to(size).freeze());
		}

		let data = BytesMut::with_capacity(size.min(u16::MAX as usize));
		let mut buf = data.limit(size);

		let size = cmp::min(buf.remaining_mut(), self.buffer.len());
		let data = self.buffer.split_to(size);
		self.offset += size as u64;
		buf.put(data);

		while buf.has_remaining_mut() {
			match self.stream.read_buf(&mut buf).await {
				Ok(Some(n)) => {
					self.offset += n as u64;
				}
				Ok(None) => return Err(DecodeError::Short.into()),
				Err(e) => return Err(Error::from_transport(e)),
			}
		}

		Ok(buf.into_inner().freeze())
	}

	/// Wait until the stream is closed, erroring if there are any additional bytes.
	pub async fn closed(&mut self) -> Result<(), Error> {
		if self.has_more().await? {
			return Err(DecodeError::Short.into());
		}

		Ok(())
	}

	/// Returns true if there is more data available in the buffer or stream.
	pub(crate) async fn has_more(&mut self) -> Result<bool, Error> {
		if !self.buffer.is_empty() {
			return Ok(true);
		}

		self.read_more().await
	}

	/// Try to read more data from the stream. Returns true if data was read, false if stream closed.
	async fn read_more(&mut self) -> Result<bool, Error> {
		match self.stream.read_buf(&mut self.buffer).await {
			Ok(Some(_)) => Ok(true),
			Ok(None) => Ok(false),
			Err(e) => Err(Error::from_transport(e)),
		}
	}

	/// Abort the stream with the given error.
	pub fn abort(&mut self, err: &Error) {
		self.stream.stop(err.to_code());
	}

	/// Cast the reader to a different version, used during version negotiation.
	pub fn with_version<V2>(self, version: V2) -> Reader<S, V2> {
		Reader {
			stream: self.stream,
			buffer: self.buffer,
			version,
			stream_id: self.stream_id,
			offset: self.offset,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::coding::test;

	#[allow(dead_code)]
	fn offset_is_available_without_trace<S: web_transport_trait::RecvStream, V>() {
		let _: fn(&Reader<S, V>) -> u64 = Reader::offset;
	}

	#[tokio::test]
	async fn transport_identity_uses_transport_offset() {
		let mut reader = Reader::new(test::RecvStream::new(b"hello"), ());

		assert_eq!(reader.stream_id(), Some(17));
		assert_eq!(reader.offset(), 3);
		assert_eq!(reader.read_chunk(5).await.unwrap().unwrap(), b"hello"[..]);
		assert_eq!(reader.offset(), 8);
	}

	#[tokio::test]
	async fn has_more_buffers_without_advancing_offset() {
		let mut reader = Reader::new(
			test::RecvStream::new(b"\x07"),
			crate::Version::Ietf(crate::ietf::Version::Draft19),
		);

		assert!(reader.has_more().await.unwrap());
		assert_eq!(reader.offset(), 3);
		assert_eq!(reader.decode::<u64>().await.unwrap(), 7);
		assert_eq!(reader.offset(), 4);
		assert!(!reader.has_more().await.unwrap());
	}
}
