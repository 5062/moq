use std::{collections::VecDeque, fmt};

#[derive(Debug)]
pub struct Error;

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("test transport error")
	}
}

impl std::error::Error for Error {}

impl web_transport_trait::Error for Error {
	fn session_error(&self) -> Option<(u32, String)> {
		None
	}
}

pub struct RecvStream {
	data: VecDeque<u8>,
}

impl RecvStream {
	pub fn new(data: &[u8]) -> Self {
		Self {
			data: data.iter().copied().collect(),
		}
	}
}

impl web_transport_trait::RecvStream for RecvStream {
	type Error = Error;

	fn stream_id(&self) -> Option<web_transport_trait::StreamId> {
		Some(web_transport_trait::StreamId::new(17, 3))
	}

	async fn read(&mut self, dst: &mut [u8]) -> Result<Option<usize>, Self::Error> {
		if self.data.is_empty() {
			return Ok(None);
		}
		let size = dst.len().min(self.data.len());
		for slot in dst.iter_mut().take(size) {
			*slot = self.data.pop_front().unwrap();
		}
		Ok(Some(size))
	}

	fn stop(&mut self, _code: u32) {}

	async fn closed(&mut self) -> Result<(), Self::Error> {
		Ok(())
	}
}

#[derive(Default)]
pub struct SendStream;

impl web_transport_trait::SendStream for SendStream {
	type Error = Error;

	fn stream_id(&self) -> Option<web_transport_trait::StreamId> {
		Some(web_transport_trait::StreamId::new(17, 3))
	}

	async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
		Ok(buf.len())
	}

	fn set_priority(&mut self, _order: u8) {}

	fn finish(&mut self) -> Result<(), Self::Error> {
		Ok(())
	}

	fn reset(&mut self, _code: u32) {}

	async fn closed(&mut self) -> Result<(), Self::Error> {
		Ok(())
	}
}
