//! This module implements the structure used in autentication process.

use super::decoder::{
    string,
    Decoder,
    Frame,
};
use std::convert::TryFrom;

/// The `Authenticate` sturcutre with the autenticator name.
pub struct Authenticate {
    /// The autenticator name.
    pub authenticator: String,
}

impl Authenticate {
    /// Create a new autenticator from the frame decoder.
    pub fn new(decoder: &mut Decoder) -> anyhow::Result<Self> {
        Self::try_from(decoder)
    }
    /// Get the autenticator name.
    #[allow(unused)]
    pub fn authenticator(&self) -> &str {
        &self.authenticator[..]
    }
}

impl TryFrom<&mut Decoder> for Authenticate {
    type Error = anyhow::Error;

    fn try_from(decoder: &mut Decoder) -> Result<Self, Self::Error> {
        anyhow::ensure!(decoder.is_authenticate());
        Ok(Self {
            authenticator: string(decoder.reader())?,
        })
    }
}
