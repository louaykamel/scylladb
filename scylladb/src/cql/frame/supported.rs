//! This module implements the Supported frame.

use super::decoder::{
    string_multimap,
    Decoder,
};
use std::collections::HashMap;

/// The supported frame with options field.
pub struct Supported {
    options: HashMap<String, Vec<String>>,
}

impl Supported {
    /// Create a Supported frame from frame decoder.
    pub fn new(decoder: &mut Decoder) -> anyhow::Result<Self> {
        let options = string_multimap(decoder.reader())?;
        Ok(Self { options })
    }
    /// Get the options in the Supported frame.
    pub fn get_options(&self) -> &HashMap<String, Vec<String>> {
        &self.options
    }
}
