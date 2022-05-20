//! This module defines the ready frame.

use super::decoder::{
    Decoder,
    Frame,
};

pub struct Ready;

impl Ready {
    pub fn new() -> Self {
        Self
    }
}
