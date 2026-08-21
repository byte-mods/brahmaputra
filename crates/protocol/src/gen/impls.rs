// Generated Implementation
use std::io::{Error, ErrorKind, Write};
use std::convert::TryInto;
use std::str;


pub const VERSION: &str = "1.0.0";

// --- ZeroCopyByteBuff Implementation ---
#[derive(Debug, Clone, Copy)]
pub enum Endian {
    Big,
    Little,
}

pub struct ZeroCopyByteBuff<'a> {
    data: &'a [u8],       
    write_buf: Vec<u8>,   
    cursor: usize,
    multiplier: f64,
    endian: Endian,
}

impl<'a> ZeroCopyByteBuff<'a> {
    pub fn from_slice(slice: &'a [u8], endian: Endian) -> Self {
        Self {
            data: slice,
            write_buf: Vec::new(),
            cursor: 0,
            multiplier: 10000.0,
            endian,
        }
    }

    pub fn new_writer(capacity: usize, endian: Endian) -> Self {
        Self {
            data: &[],
            write_buf: Vec::with_capacity(capacity),
            cursor: 0,
            multiplier: 10000.0,
            endian,
        }
    }

	// zig-zag encoding: (n << 1) ^ (n >> 31)
	#[inline(always)]
	fn zigzag_encode32(n: i32) -> u32 {
		((n << 1) ^ (n >> 31)) as u32
	}

	#[inline(always)]
	fn zigzag_decode32(n: u32) -> i32 {
		((n >> 1) as i32) ^ (-((n & 1) as i32))
	}

	#[inline(always)]
	fn zigzag_encode64(n: i64) -> u64 {
		((n << 1) ^ (n >> 63)) as u64
	}

	#[inline(always)]
	fn zigzag_decode64(n: u64) -> i64 {
		((n >> 1) as i64) ^ (-((n & 1) as i64))
	}

	#[inline(always)]
	fn get_varint32(&mut self) -> u32 {
		let mut result: u32 = 0;
		let mut shift = 0;
        // Optimization: Unrolled loop for common case (1-5 bytes)
		loop {
            // SAFETY: We trust the data source. Unchecked access is faster.
			let byte = unsafe { *self.data.get_unchecked(self.cursor) };
			self.cursor += 1;
			result |= ((byte & 0x7F) as u32) << shift;
			if byte & 0x80 == 0 {
				break;
			}
			shift += 7;
		}
		result
	}

	#[inline(always)]
	fn put_varint32(&mut self, mut value: u32) {
        // FAST PATH: 1 byte
        if (value & !0x7F) == 0 {
            self.write_buf.push(value as u8);
            return;
        }
        // General path: Unsafe writes
        self.write_buf.reserve(5);
        unsafe {
            let mut ptr = self.write_buf.as_mut_ptr().add(self.write_buf.len());
            let mut len = 0;
            loop {
                if (value & !0x7F) == 0 {
                    ptr.write(value as u8);
                    len += 1;
                    break;
                }
                ptr.write((value as u8) | 0x80);
                ptr = ptr.add(1);
                len += 1;
                value >>= 7;
            }
            self.write_buf.set_len(self.write_buf.len() + len);
        }
	}

	#[inline(always)]
	fn get_varint64(&mut self) -> u64 {
		let mut result: u64 = 0;
		let mut shift = 0;
		loop {
            // SAFETY: Unchecked access
			let byte = unsafe { *self.data.get_unchecked(self.cursor) };
			self.cursor += 1;
			result |= ((byte & 0x7F) as u64) << shift;
			if byte & 0x80 == 0 {
				break;
			}
			shift += 7;
		}
		result
	}

	#[inline(always)]
	fn put_varint64(&mut self, mut value: u64) {
        // FAST PATH: 1 byte
        if (value & !0x7F) == 0 {
            self.write_buf.push(value as u8);
            return;
        }
        // General path: Unsafe writes
        self.write_buf.reserve(10);
        unsafe {
            let mut ptr = self.write_buf.as_mut_ptr().add(self.write_buf.len());
            let mut len = 0;
            loop {
                if (value & !0x7F) == 0 {
                    ptr.write(value as u8);
                    len += 1;
                    break;
                }
                ptr.write((value as u8) | 0x80);
                ptr = ptr.add(1);
                len += 1;
                value >>= 7;
            }
            self.write_buf.set_len(self.write_buf.len() + len);
        }
	}

    #[inline(always)]
    pub fn get_i32(&mut self) -> i32 {
        let val = self.get_varint32();
		Self::zigzag_decode32(val)
    }

	#[inline(always)]
    pub fn get_bool(&mut self) -> bool {
        // SAFETY: Unchecked access
        let b = unsafe { *self.data.get_unchecked(self.cursor) };
        self.cursor += 1;
		b != 0
    }

    #[inline(always)]
    pub fn get_str(&mut self) -> Result<&'a str, &'static str> {
		let len = self.get_i32() as usize;
        if len == 0 { return Ok(""); }
        // SAFETY: We assume valid UTF-8 and sufficient length for speed.
        let s_bytes = unsafe { self.data.get_unchecked(self.cursor..self.cursor + len) };
        self.cursor += len;
        // SAFETY: Skipping UTF-8 check
        Ok(unsafe { str::from_utf8_unchecked(s_bytes) })
    }

    #[inline(always)]
    pub fn get_float(&mut self) -> f64 {
        let val = self.get_i64(); 
        val as f64 / self.multiplier
    }

    #[inline(always)]
    pub fn get_i64(&mut self) -> i64 {
		let val = self.get_varint64();
		Self::zigzag_decode64(val)
    }

    #[inline(always)]
    pub fn put_i64(&mut self, value: i64) {
		self.put_varint64(Self::zigzag_encode64(value));
    }

    #[inline(always)]
    pub fn put_i32(&mut self, value: i32) {
		self.put_varint32(Self::zigzag_encode32(value));
    }
	
	#[inline(always)]
	pub fn put_bool(&mut self, value: bool) {
		self.write_buf.push(if value { 1 } else { 0 });
	}

	#[inline(always)]
    pub fn put_str(&mut self, value: &str) {
        let len = value.len();
		self.put_i32(len as i32);
        // Unsafe copy
        self.write_buf.reserve(len);
        unsafe {
            let ptr = self.write_buf.as_mut_ptr().add(self.write_buf.len());
            std::ptr::copy_nonoverlapping(value.as_ptr(), ptr, len);
            self.write_buf.set_len(self.write_buf.len() + len);
        }
    }

	#[inline(always)]
    pub fn put_float(&mut self, value: f64) {
        let i_val = (value * self.multiplier) as i64;
		self.put_varint64(Self::zigzag_encode64(i_val));
    }

    pub fn finish(self) -> Vec<u8> {
        self.write_buf
    }
}

// --- Generated Impl ---

impl ProduceRequest {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.topic);
		
		
		
		buf.put_i32(*&self.partition);
		
		
		
		buf.put_i32(*&self.acks);
		
		
		
		buf.put_i32(*&self.timeout_ms);
		
		
		
		buf.put_i64(*&self.batches_length);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = ProduceRequest::default();
		
		
		obj.topic = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.partition = buf.get_i32();
		
		
		
		obj.acks = buf.get_i32();
		
		
		
		obj.timeout_ms = buf.get_i32();
		
		
		
		obj.batches_length = buf.get_i64();
		
		
		Ok(obj)
	}
}

impl ProduceResponse {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.topic);
		
		
		
		buf.put_i32(*&self.partition);
		
		
		
		buf.put_i32(*&self.error_code);
		
		
		
		buf.put_i64(*&self.base_offset);
		
		
		
		buf.put_i64(*&self.log_append_time_ms);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = ProduceResponse::default();
		
		
		obj.topic = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.partition = buf.get_i32();
		
		
		
		obj.error_code = buf.get_i32();
		
		
		
		obj.base_offset = buf.get_i64();
		
		
		
		obj.log_append_time_ms = buf.get_i64();
		
		
		Ok(obj)
	}
}

impl FetchRequest {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.topic);
		
		
		
		buf.put_i32(*&self.partition);
		
		
		
		buf.put_i64(*&self.fetch_offset);
		
		
		
		buf.put_i32(*&self.max_bytes);
		
		
		
		buf.put_i32(*&self.max_wait_ms);
		
		
		
		buf.put_i32(*&self.min_bytes);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = FetchRequest::default();
		
		
		obj.topic = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.partition = buf.get_i32();
		
		
		
		obj.fetch_offset = buf.get_i64();
		
		
		
		obj.max_bytes = buf.get_i32();
		
		
		
		obj.max_wait_ms = buf.get_i32();
		
		
		
		obj.min_bytes = buf.get_i32();
		
		
		Ok(obj)
	}
}

impl FetchResponse {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.topic);
		
		
		
		buf.put_i32(*&self.partition);
		
		
		
		buf.put_i32(*&self.error_code);
		
		
		
		buf.put_i64(*&self.high_watermark);
		
		
		
		buf.put_i64(*&self.last_stable_offset);
		
		
		
		buf.put_i64(*&self.batches_length);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = FetchResponse::default();
		
		
		obj.topic = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.partition = buf.get_i32();
		
		
		
		obj.error_code = buf.get_i32();
		
		
		
		obj.high_watermark = buf.get_i64();
		
		
		
		obj.last_stable_offset = buf.get_i64();
		
		
		
		obj.batches_length = buf.get_i64();
		
		
		Ok(obj)
	}
}

impl ListOffsetsRequest {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.topic);
		
		
		
		buf.put_i32(*&self.partition);
		
		
		
		buf.put_i64(*&self.timestamp);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = ListOffsetsRequest::default();
		
		
		obj.topic = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.partition = buf.get_i32();
		
		
		
		obj.timestamp = buf.get_i64();
		
		
		Ok(obj)
	}
}

impl ListOffsetsResponse {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.topic);
		
		
		
		buf.put_i32(*&self.partition);
		
		
		
		buf.put_i32(*&self.error_code);
		
		
		
		buf.put_i64(*&self.offset);
		
		
		
		buf.put_i64(*&self.timestamp);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = ListOffsetsResponse::default();
		
		
		obj.topic = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.partition = buf.get_i32();
		
		
		
		obj.error_code = buf.get_i32();
		
		
		
		obj.offset = buf.get_i64();
		
		
		
		obj.timestamp = buf.get_i64();
		
		
		Ok(obj)
	}
}

impl BrokerInfo {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_i32(*&self.broker_id);
		
		
		
		buf.put_str(&self.host);
		
		
		
		buf.put_i32(*&self.port);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = BrokerInfo::default();
		
		
		obj.broker_id = buf.get_i32();
		
		
		
		obj.host = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.port = buf.get_i32();
		
		
		Ok(obj)
	}
}

impl PartitionInfo {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_i32(*&self.partition);
		
		
		
		buf.put_i32(*&self.leader);
		
		
		
		buf.put_i32(self.replicas.len() as i32);
		for item in &self.replicas {
			buf.put_i32(*item);
		}
		
		
		
		buf.put_i32(self.isr.len() as i32);
		for item in &self.isr {
			buf.put_i32(*item);
		}
		
		
		
		buf.put_i32(*&self.leader_epoch);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = PartitionInfo::default();
		
		
		obj.partition = buf.get_i32();
		
		
		
		obj.leader = buf.get_i32();
		
		
		
		let replicas_len = buf.get_i32();
		for _ in 0..replicas_len {
			let val = buf.get_i32();
			obj.replicas.push(val);
		}
		
		
		
		let isr_len = buf.get_i32();
		for _ in 0..isr_len {
			let val = buf.get_i32();
			obj.isr.push(val);
		}
		
		
		
		obj.leader_epoch = buf.get_i32();
		
		
		Ok(obj)
	}
}

impl TopicInfo {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.name);
		
		
		
		buf.put_i32(*&self.error_code);
		
		
		
		buf.put_i32(self.partitions.len() as i32);
		for item in &self.partitions {
			item.encode_to(buf)?;
		}
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = TopicInfo::default();
		
		
		obj.name = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.error_code = buf.get_i32();
		
		
		
		let partitions_len = buf.get_i32();
		for _ in 0..partitions_len {
			let val = PartitionInfo::decode_from(buf)?;
			obj.partitions.push(val);
		}
		
		
		Ok(obj)
	}
}

impl MetadataRequest {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_i32(self.topics.len() as i32);
		for item in &self.topics {
			buf.put_str(item);
		}
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = MetadataRequest::default();
		
		
		let topics_len = buf.get_i32();
		for _ in 0..topics_len {
			let val = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
			obj.topics.push(val);
		}
		
		
		Ok(obj)
	}
}

impl MetadataResponse {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_i32(self.brokers.len() as i32);
		for item in &self.brokers {
			item.encode_to(buf)?;
		}
		
		
		
		buf.put_i32(*&self.controller_id);
		
		
		
		buf.put_i32(self.topics.len() as i32);
		for item in &self.topics {
			item.encode_to(buf)?;
		}
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = MetadataResponse::default();
		
		
		let brokers_len = buf.get_i32();
		for _ in 0..brokers_len {
			let val = BrokerInfo::decode_from(buf)?;
			obj.brokers.push(val);
		}
		
		
		
		obj.controller_id = buf.get_i32();
		
		
		
		let topics_len = buf.get_i32();
		for _ in 0..topics_len {
			let val = TopicInfo::decode_from(buf)?;
			obj.topics.push(val);
		}
		
		
		Ok(obj)
	}
}

impl GroupMemberInfo {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.member_id);
		
		
		
		buf.put_i32(self.subscription_topics.len() as i32);
		for item in &self.subscription_topics {
			buf.put_str(item);
		}
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = GroupMemberInfo::default();
		
		
		obj.member_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		let subscription_topics_len = buf.get_i32();
		for _ in 0..subscription_topics_len {
			let val = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
			obj.subscription_topics.push(val);
		}
		
		
		Ok(obj)
	}
}

impl JoinGroupRequest {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.group_id);
		
		
		
		buf.put_i32(*&self.session_timeout_ms);
		
		
		
		buf.put_i32(*&self.rebalance_timeout_ms);
		
		
		
		buf.put_str(&self.member_id);
		
		
		
		buf.put_i32(self.subscription_topics.len() as i32);
		for item in &self.subscription_topics {
			buf.put_str(item);
		}
		
		
		
		buf.put_str(&self.group_instance_id);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = JoinGroupRequest::default();
		
		
		obj.group_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.session_timeout_ms = buf.get_i32();
		
		
		
		obj.rebalance_timeout_ms = buf.get_i32();
		
		
		
		obj.member_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		let subscription_topics_len = buf.get_i32();
		for _ in 0..subscription_topics_len {
			let val = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
			obj.subscription_topics.push(val);
		}
		
		
		
		obj.group_instance_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		Ok(obj)
	}
}

impl JoinGroupResponse {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_i32(*&self.error_code);
		
		
		
		buf.put_i32(*&self.generation);
		
		
		
		buf.put_str(&self.member_id);
		
		
		
		buf.put_str(&self.leader_member_id);
		
		
		
		buf.put_i32(self.members.len() as i32);
		for item in &self.members {
			item.encode_to(buf)?;
		}
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = JoinGroupResponse::default();
		
		
		obj.error_code = buf.get_i32();
		
		
		
		obj.generation = buf.get_i32();
		
		
		
		obj.member_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.leader_member_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		let members_len = buf.get_i32();
		for _ in 0..members_len {
			let val = GroupMemberInfo::decode_from(buf)?;
			obj.members.push(val);
		}
		
		
		Ok(obj)
	}
}

impl AssignedPartition {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.topic);
		
		
		
		buf.put_i32(*&self.partition);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = AssignedPartition::default();
		
		
		obj.topic = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.partition = buf.get_i32();
		
		
		Ok(obj)
	}
}

impl MemberAssignment {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.member_id);
		
		
		
		buf.put_i32(self.partitions.len() as i32);
		for item in &self.partitions {
			item.encode_to(buf)?;
		}
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = MemberAssignment::default();
		
		
		obj.member_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		let partitions_len = buf.get_i32();
		for _ in 0..partitions_len {
			let val = AssignedPartition::decode_from(buf)?;
			obj.partitions.push(val);
		}
		
		
		Ok(obj)
	}
}

impl SyncGroupRequest {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.group_id);
		
		
		
		buf.put_i32(*&self.generation);
		
		
		
		buf.put_str(&self.member_id);
		
		
		
		buf.put_i32(self.assignments.len() as i32);
		for item in &self.assignments {
			item.encode_to(buf)?;
		}
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = SyncGroupRequest::default();
		
		
		obj.group_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.generation = buf.get_i32();
		
		
		
		obj.member_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		let assignments_len = buf.get_i32();
		for _ in 0..assignments_len {
			let val = MemberAssignment::decode_from(buf)?;
			obj.assignments.push(val);
		}
		
		
		Ok(obj)
	}
}

impl SyncGroupResponse {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_i32(*&self.error_code);
		
		
		
		buf.put_i32(self.assignment.len() as i32);
		for item in &self.assignment {
			item.encode_to(buf)?;
		}
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = SyncGroupResponse::default();
		
		
		obj.error_code = buf.get_i32();
		
		
		
		let assignment_len = buf.get_i32();
		for _ in 0..assignment_len {
			let val = AssignedPartition::decode_from(buf)?;
			obj.assignment.push(val);
		}
		
		
		Ok(obj)
	}
}

impl HeartbeatRequest {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.group_id);
		
		
		
		buf.put_i32(*&self.generation);
		
		
		
		buf.put_str(&self.member_id);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = HeartbeatRequest::default();
		
		
		obj.group_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.generation = buf.get_i32();
		
		
		
		obj.member_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		Ok(obj)
	}
}

impl HeartbeatResponse {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_i32(*&self.error_code);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = HeartbeatResponse::default();
		
		
		obj.error_code = buf.get_i32();
		
		
		Ok(obj)
	}
}

impl OffsetCommitEntry {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.topic);
		
		
		
		buf.put_i32(*&self.partition);
		
		
		
		buf.put_i64(*&self.offset);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = OffsetCommitEntry::default();
		
		
		obj.topic = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.partition = buf.get_i32();
		
		
		
		obj.offset = buf.get_i64();
		
		
		Ok(obj)
	}
}

impl OffsetCommitRequest {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.group_id);
		
		
		
		buf.put_i32(*&self.generation);
		
		
		
		buf.put_str(&self.member_id);
		
		
		
		buf.put_i32(self.offsets.len() as i32);
		for item in &self.offsets {
			item.encode_to(buf)?;
		}
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = OffsetCommitRequest::default();
		
		
		obj.group_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.generation = buf.get_i32();
		
		
		
		obj.member_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		let offsets_len = buf.get_i32();
		for _ in 0..offsets_len {
			let val = OffsetCommitEntry::decode_from(buf)?;
			obj.offsets.push(val);
		}
		
		
		Ok(obj)
	}
}

impl OffsetCommitResponse {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_i32(*&self.error_code);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = OffsetCommitResponse::default();
		
		
		obj.error_code = buf.get_i32();
		
		
		Ok(obj)
	}
}

impl OffsetFetchRequest {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.group_id);
		
		
		
		buf.put_i32(self.partitions.len() as i32);
		for item in &self.partitions {
			item.encode_to(buf)?;
		}
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = OffsetFetchRequest::default();
		
		
		obj.group_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		let partitions_len = buf.get_i32();
		for _ in 0..partitions_len {
			let val = AssignedPartition::decode_from(buf)?;
			obj.partitions.push(val);
		}
		
		
		Ok(obj)
	}
}

impl OffsetFetchEntry {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.topic);
		
		
		
		buf.put_i32(*&self.partition);
		
		
		
		buf.put_i64(*&self.offset);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = OffsetFetchEntry::default();
		
		
		obj.topic = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.partition = buf.get_i32();
		
		
		
		obj.offset = buf.get_i64();
		
		
		Ok(obj)
	}
}

impl OffsetFetchResponse {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_i32(*&self.error_code);
		
		
		
		buf.put_i32(self.offsets.len() as i32);
		for item in &self.offsets {
			item.encode_to(buf)?;
		}
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = OffsetFetchResponse::default();
		
		
		obj.error_code = buf.get_i32();
		
		
		
		let offsets_len = buf.get_i32();
		for _ in 0..offsets_len {
			let val = OffsetFetchEntry::decode_from(buf)?;
			obj.offsets.push(val);
		}
		
		
		Ok(obj)
	}
}

impl ListedGroup {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.group_id);
		
		
		
		buf.put_str(&self.state);
		
		
		
		buf.put_i32(*&self.generation);
		
		
		
		buf.put_i32(*&self.member_count);
		
		
		
		buf.put_i32(*&self.coordinator_partition);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = ListedGroup::default();
		
		
		obj.group_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.state = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.generation = buf.get_i32();
		
		
		
		obj.member_count = buf.get_i32();
		
		
		
		obj.coordinator_partition = buf.get_i32();
		
		
		Ok(obj)
	}
}

impl ListGroupsRequest {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_i32(self.states.len() as i32);
		for item in &self.states {
			buf.put_str(item);
		}
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = ListGroupsRequest::default();
		
		
		let states_len = buf.get_i32();
		for _ in 0..states_len {
			let val = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
			obj.states.push(val);
		}
		
		
		Ok(obj)
	}
}

impl ListGroupsResponse {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_i32(*&self.error_code);
		
		
		
		buf.put_i32(self.groups.len() as i32);
		for item in &self.groups {
			item.encode_to(buf)?;
		}
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = ListGroupsResponse::default();
		
		
		obj.error_code = buf.get_i32();
		
		
		
		let groups_len = buf.get_i32();
		for _ in 0..groups_len {
			let val = ListedGroup::decode_from(buf)?;
			obj.groups.push(val);
		}
		
		
		Ok(obj)
	}
}

impl DescribedMember {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.member_id);
		
		
		
		buf.put_i32(self.subscription_topics.len() as i32);
		for item in &self.subscription_topics {
			buf.put_str(item);
		}
		
		
		
		buf.put_i32(self.assignment.len() as i32);
		for item in &self.assignment {
			item.encode_to(buf)?;
		}
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = DescribedMember::default();
		
		
		obj.member_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		let subscription_topics_len = buf.get_i32();
		for _ in 0..subscription_topics_len {
			let val = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
			obj.subscription_topics.push(val);
		}
		
		
		
		let assignment_len = buf.get_i32();
		for _ in 0..assignment_len {
			let val = AssignedPartition::decode_from(buf)?;
			obj.assignment.push(val);
		}
		
		
		Ok(obj)
	}
}

impl DescribeGroupRequest {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.group_id);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = DescribeGroupRequest::default();
		
		
		obj.group_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		Ok(obj)
	}
}

impl DescribeGroupResponse {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_i32(*&self.error_code);
		
		
		
		buf.put_str(&self.group_id);
		
		
		
		buf.put_str(&self.state);
		
		
		
		buf.put_i32(*&self.generation);
		
		
		
		buf.put_str(&self.leader_member_id);
		
		
		
		buf.put_i32(*&self.coordinator_partition);
		
		
		
		buf.put_i32(self.members.len() as i32);
		for item in &self.members {
			item.encode_to(buf)?;
		}
		
		
		
		buf.put_i32(self.offsets.len() as i32);
		for item in &self.offsets {
			item.encode_to(buf)?;
		}
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = DescribeGroupResponse::default();
		
		
		obj.error_code = buf.get_i32();
		
		
		
		obj.group_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.state = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.generation = buf.get_i32();
		
		
		
		obj.leader_member_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.coordinator_partition = buf.get_i32();
		
		
		
		let members_len = buf.get_i32();
		for _ in 0..members_len {
			let val = DescribedMember::decode_from(buf)?;
			obj.members.push(val);
		}
		
		
		
		let offsets_len = buf.get_i32();
		for _ in 0..offsets_len {
			let val = OffsetFetchEntry::decode_from(buf)?;
			obj.offsets.push(val);
		}
		
		
		Ok(obj)
	}
}

impl ProduceMultiPartition {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.topic);
		
		
		
		buf.put_i32(*&self.partition);
		
		
		
		buf.put_i64(*&self.batches_length);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = ProduceMultiPartition::default();
		
		
		obj.topic = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.partition = buf.get_i32();
		
		
		
		obj.batches_length = buf.get_i64();
		
		
		Ok(obj)
	}
}

impl ProduceMultiRequest {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_i32(*&self.acks);
		
		
		
		buf.put_i32(*&self.timeout_ms);
		
		
		
		buf.put_i32(self.partitions.len() as i32);
		for item in &self.partitions {
			item.encode_to(buf)?;
		}
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = ProduceMultiRequest::default();
		
		
		obj.acks = buf.get_i32();
		
		
		
		obj.timeout_ms = buf.get_i32();
		
		
		
		let partitions_len = buf.get_i32();
		for _ in 0..partitions_len {
			let val = ProduceMultiPartition::decode_from(buf)?;
			obj.partitions.push(val);
		}
		
		
		Ok(obj)
	}
}

impl ProduceMultiResult {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.topic);
		
		
		
		buf.put_i32(*&self.partition);
		
		
		
		buf.put_i32(*&self.error_code);
		
		
		
		buf.put_i64(*&self.base_offset);
		
		
		
		buf.put_i64(*&self.log_append_time_ms);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = ProduceMultiResult::default();
		
		
		obj.topic = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.partition = buf.get_i32();
		
		
		
		obj.error_code = buf.get_i32();
		
		
		
		obj.base_offset = buf.get_i64();
		
		
		
		obj.log_append_time_ms = buf.get_i64();
		
		
		Ok(obj)
	}
}

impl ProduceMultiResponse {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_i32(self.results.len() as i32);
		for item in &self.results {
			item.encode_to(buf)?;
		}
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = ProduceMultiResponse::default();
		
		
		let results_len = buf.get_i32();
		for _ in 0..results_len {
			let val = ProduceMultiResult::decode_from(buf)?;
			obj.results.push(val);
		}
		
		
		Ok(obj)
	}
}

impl FetchMultiPartition {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.topic);
		
		
		
		buf.put_i32(*&self.partition);
		
		
		
		buf.put_i64(*&self.fetch_offset);
		
		
		
		buf.put_i32(*&self.max_bytes);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = FetchMultiPartition::default();
		
		
		obj.topic = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.partition = buf.get_i32();
		
		
		
		obj.fetch_offset = buf.get_i64();
		
		
		
		obj.max_bytes = buf.get_i32();
		
		
		Ok(obj)
	}
}

impl FetchMultiRequest {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_i32(*&self.max_wait_ms);
		
		
		
		buf.put_i32(*&self.min_bytes);
		
		
		
		buf.put_i32(self.partitions.len() as i32);
		for item in &self.partitions {
			item.encode_to(buf)?;
		}
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = FetchMultiRequest::default();
		
		
		obj.max_wait_ms = buf.get_i32();
		
		
		
		obj.min_bytes = buf.get_i32();
		
		
		
		let partitions_len = buf.get_i32();
		for _ in 0..partitions_len {
			let val = FetchMultiPartition::decode_from(buf)?;
			obj.partitions.push(val);
		}
		
		
		Ok(obj)
	}
}

impl FetchMultiResult {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.topic);
		
		
		
		buf.put_i32(*&self.partition);
		
		
		
		buf.put_i32(*&self.error_code);
		
		
		
		buf.put_i64(*&self.high_watermark);
		
		
		
		buf.put_i64(*&self.last_stable_offset);
		
		
		
		buf.put_i64(*&self.batches_length);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = FetchMultiResult::default();
		
		
		obj.topic = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.partition = buf.get_i32();
		
		
		
		obj.error_code = buf.get_i32();
		
		
		
		obj.high_watermark = buf.get_i64();
		
		
		
		obj.last_stable_offset = buf.get_i64();
		
		
		
		obj.batches_length = buf.get_i64();
		
		
		Ok(obj)
	}
}

impl FetchMultiResponse {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_i32(self.results.len() as i32);
		for item in &self.results {
			item.encode_to(buf)?;
		}
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = FetchMultiResponse::default();
		
		
		let results_len = buf.get_i32();
		for _ in 0..results_len {
			let val = FetchMultiResult::decode_from(buf)?;
			obj.results.push(val);
		}
		
		
		Ok(obj)
	}
}

impl ApiVersionRange {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_i32(*&self.api_key);
		
		
		
		buf.put_i32(*&self.min_version);
		
		
		
		buf.put_i32(*&self.max_version);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = ApiVersionRange::default();
		
		
		obj.api_key = buf.get_i32();
		
		
		
		obj.min_version = buf.get_i32();
		
		
		
		obj.max_version = buf.get_i32();
		
		
		Ok(obj)
	}
}

impl ApiVersionsRequest {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.client_software_name);
		
		
		
		buf.put_str(&self.client_software_version);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = ApiVersionsRequest::default();
		
		
		obj.client_software_name = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.client_software_version = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		Ok(obj)
	}
}

impl ApiVersionsResponse {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_i32(*&self.error_code);
		
		
		
		buf.put_i32(self.api_versions.len() as i32);
		for item in &self.api_versions {
			item.encode_to(buf)?;
		}
		
		
		
		buf.put_str(&self.broker_version);
		
		
		
		buf.put_i32(*&self.throttle_time_ms);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = ApiVersionsResponse::default();
		
		
		obj.error_code = buf.get_i32();
		
		
		
		let api_versions_len = buf.get_i32();
		for _ in 0..api_versions_len {
			let val = ApiVersionRange::decode_from(buf)?;
			obj.api_versions.push(val);
		}
		
		
		
		obj.broker_version = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.throttle_time_ms = buf.get_i32();
		
		
		Ok(obj)
	}
}

impl OffsetCommitRecord {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.group_id);
		
		
		
		buf.put_str(&self.topic);
		
		
		
		buf.put_i32(*&self.partition);
		
		
		
		buf.put_i64(*&self.offset);
		
		
		
		buf.put_i64(*&self.commit_timestamp_ms);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = OffsetCommitRecord::default();
		
		
		obj.group_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.topic = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.partition = buf.get_i32();
		
		
		
		obj.offset = buf.get_i64();
		
		
		
		obj.commit_timestamp_ms = buf.get_i64();
		
		
		Ok(obj)
	}
}

impl GroupMemberRecord {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.member_id);
		
		
		
		buf.put_i32(self.subscription_topics.len() as i32);
		for item in &self.subscription_topics {
			buf.put_str(item);
		}
		
		
		
		buf.put_i32(self.assignment.len() as i32);
		for item in &self.assignment {
			item.encode_to(buf)?;
		}
		
		
		
		buf.put_str(&self.group_instance_id);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = GroupMemberRecord::default();
		
		
		obj.member_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		let subscription_topics_len = buf.get_i32();
		for _ in 0..subscription_topics_len {
			let val = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
			obj.subscription_topics.push(val);
		}
		
		
		
		let assignment_len = buf.get_i32();
		for _ in 0..assignment_len {
			let val = AssignedPartition::decode_from(buf)?;
			obj.assignment.push(val);
		}
		
		
		
		obj.group_instance_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		Ok(obj)
	}
}

impl GroupMetadataRecord {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.group_id);
		
		
		
		buf.put_i32(*&self.generation);
		
		
		
		buf.put_str(&self.leader_member_id);
		
		
		
		buf.put_i32(self.members.len() as i32);
		for item in &self.members {
			item.encode_to(buf)?;
		}
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = GroupMetadataRecord::default();
		
		
		obj.group_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.generation = buf.get_i32();
		
		
		
		obj.leader_member_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		let members_len = buf.get_i32();
		for _ in 0..members_len {
			let val = GroupMemberRecord::decode_from(buf)?;
			obj.members.push(val);
		}
		
		
		Ok(obj)
	}
}

impl TombstoneRecord {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.group_id);
		
		
		
		buf.put_str(&self.topic);
		
		
		
		buf.put_i32(*&self.partition);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = TombstoneRecord::default();
		
		
		obj.group_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.topic = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.partition = buf.get_i32();
		
		
		Ok(obj)
	}
}

impl AuthenticateRequest {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.username);
		
		
		
		buf.put_str(&self.password);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = AuthenticateRequest::default();
		
		
		obj.username = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.password = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		Ok(obj)
	}
}

impl AuthenticateResponse {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_i32(*&self.error_code);
		
		
		
		buf.put_str(&self.principal);
		
		
		
		buf.put_str(&self.role);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = AuthenticateResponse::default();
		
		
		obj.error_code = buf.get_i32();
		
		
		
		obj.principal = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.role = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		Ok(obj)
	}
}

impl LeaveGroupRequest {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_str(&self.group_id);
		
		
		
		buf.put_str(&self.member_id);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = LeaveGroupRequest::default();
		
		
		obj.group_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		
		obj.member_id = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();
		
		
		Ok(obj)
	}
}

impl LeaveGroupResponse {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		
		Ok(wtr)
		
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		
		
		buf.put_i32(*&self.error_code);
		
		
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = LeaveGroupResponse::default();
		
		
		obj.error_code = buf.get_i32();
		
		
		Ok(obj)
	}
}
