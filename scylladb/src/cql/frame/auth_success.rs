//! This module implements the needed structure and traits used for successful autentication.

use super::decoder::{
    bytes,
    Decoder,
};
use std::convert::TryFrom;

/// The structure for successful autentication.
pub struct AuthSuccess {
    token: Option<Vec<u8>>,
}

impl AuthSuccess {
    /// Create a new `AuthSuccess` structure from frame decoder.
    pub fn new(decoder: &mut Decoder) -> anyhow::Result<Self> {
        Self::try_from(decoder)
    }
    /// Get the autentication token.
    pub fn token(&self) -> Option<&Vec<u8>> {
        self.token.as_ref()
    }
}

impl TryFrom<&mut Decoder> for AuthSuccess {
    type Error = anyhow::Error;

    fn try_from(decoder: &mut Decoder) -> Result<Self, Self::Error> {
        Ok(Self {
            token: bytes(decoder.reader())?,
        })
    }
}
