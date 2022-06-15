//! This module implements the challenge part of the challenge–response authentication.

use std::convert::TryFrom;

use super::decoder::{
    bytes,
    Decoder,
};

/// The Autentication Challenge structure with the token field.
pub(crate) struct AuthChallenge {
    #[allow(unused)]
    token: Option<Vec<u8>>,
}

impl AuthChallenge {
    /// Create a new `AuthChallenge ` from the body of frame.
    pub(crate) fn new(decoder: &mut Decoder) -> anyhow::Result<Self> {
        Self::try_from(decoder)
    }
}

impl TryFrom<&mut Decoder> for AuthChallenge {
    type Error = anyhow::Error;

    fn try_from(decoder: &mut Decoder) -> Result<Self, Self::Error> {
        Ok(Self {
            token: bytes(decoder.reader())?,
        })
    }
}
