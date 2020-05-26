use super::*;
use std::convert::TryFrom;

use super::{Error, Result};
use crate::codec::data_type::Int;
use crate::prelude::*;
use codec::number::{NumberDecoder, NumberEncoder};

pub struct ChunkedVecInt {
    length: usize,
    null_cnt: usize,
    null_bitmap: Vec<u8>,
    data: Vec<u8>,
}

impl ChunkedVecInt {
    const ELEMENT_SIZE: usize = std::mem::size_of::<Int>();

    pub fn new(init_cap: usize) -> Self {
        ChunkedVecInt {
            data: Vec::with_capacity(Self::ELEMENT_SIZE * init_cap),
            null_bitmap: Vec::with_capacity((init_cap + 7) / 8),
            null_cnt: 0,
            length: 0,
        }
    }

    pub fn from_vec(data: Vec<Option<Int>>) -> Self {
        let mut x = Self::new(data.len());
        for element in data {
            if let Some(data) = element {
                x.append(data).unwrap();
            } else {
                x.append_null();
            }
        }
        x
    }

    /// Return whether the datum for the row is null or not.
    pub fn is_null(&self, row_idx: usize) -> bool {
        if self.null_cnt == 0 {
            return false;
        }

        if let Some(null_byte) = self.null_bitmap.get(row_idx >> 3) {
            null_byte & (1 << ((row_idx) & 7)) == 0
        } else {
            panic!("index out of range!");
        }
    }

    /// Update the null bitmap and count when append a datum.
    /// `on` is false means the datum is null.
    #[inline]
    pub fn append_null_bitmap(&mut self, on: bool) {
        let idx = self.length >> 3;
        if idx >= self.null_bitmap.len() {
            self.null_bitmap.push(0);
        }
        if on {
            let pos = self.length & 7;
            self.null_bitmap[idx] |= 1 << pos;
        } else {
            self.null_cnt += 1;
        }
    }

    /// Append null to the chunked vector.
    #[inline]
    pub fn append_null(&mut self) {
        self.append_null_bitmap(false);
        let len = Self::ELEMENT_SIZE + self.data.len();
        self.data.resize(len, 0);
        self.length += 1;
    }

    /// Called when datum has been appended.
    #[inline]
    fn finish_append(&mut self) {
        self.append_null_bitmap(true);
        self.length += 1;
        self.data.resize(self.length * Self::ELEMENT_SIZE, 0);
    }

    /// Append u64 datum to the chunked vector.
    #[inline]
    pub fn append(&mut self, v: Int) -> Result<()> {
        self.data.write_i64_le(v)?;
        self.finish_append();
        Ok(())
    }

    /// Get reference to datum of the row in the chunked vector.
    pub fn get_ref(&self, row_idx: usize) -> Option<&Int> {
        if self.is_null(row_idx) {
            None
        } else {
            let start = row_idx * Self::ELEMENT_SIZE;
            let end = start + Self::ELEMENT_SIZE;
            let ref_data = &self.data[start..end];
            Some(unsafe { std::mem::transmute::<&u8, &Int>(&ref_data[0]) })
        }
    }

    /// Return the total rows in the column.
    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunked_int_element_len() {
        assert_eq!(ChunkedVecInt::ELEMENT_SIZE, 8);
    }

    #[test]
    fn test_chunked_int_new() {
        let _x = ChunkedVecInt::new(233);
    }

    fn helper_new_chunked_int() -> ChunkedVecInt {
        ChunkedVecInt::from_vec(vec![
            None,
            Some(233),
            Some(65536),
            None,
            Some(-233),
            Some(233333333),
            None,
        ])
    }

    #[test]
    fn test_chunked_int_from_vec() {
        let _x = helper_new_chunked_int();
    }

    #[test]
    fn test_chunked_int_null() {
        let x = helper_new_chunked_int();

        let result = vec![true, false, false, true, false, false, true];

        for i in 0..x.len() {
            assert_eq!(x.is_null(i), result[i]);
        }
    }

    #[test]
    fn test_chunked_int_len() {
        let x = helper_new_chunked_int();

        assert_eq!(x.len(), 7);
    }

    #[test]
    fn test_chunked_read_ref() {
        let x = helper_new_chunked_int();

        assert_eq!(*x.get_ref(1).unwrap(), 233);
        assert_eq!(*x.get_ref(2).unwrap(), 65536);
        assert_eq!(*x.get_ref(4).unwrap(), -233);
        assert_eq!(*x.get_ref(5).unwrap(), 233333333);
    }
}
