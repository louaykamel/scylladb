//! This module implements the Options frame.

use super::opcode::OPTIONS;

/// Blanket cql frame header for OPTIONS frame.
const OPTIONS_HEADER: &'static [u8] = &[4, 0, 0, 0, OPTIONS, 0, 0, 0, 0];

/// The Options frame structure.
pub(crate) struct Options(pub Vec<u8>);

pub(crate) struct OptionsBuilder<Stage> {
    buffer: Vec<u8>,
    #[allow(unused)]
    stage: Stage,
}

struct OptionsHeader;
pub(crate) struct OptionsBuild;

impl OptionsBuilder<OptionsHeader> {
    pub fn new() -> OptionsBuilder<OptionsBuild> {
        let mut buffer: Vec<u8> = Vec::new();
        buffer.extend_from_slice(&OPTIONS_HEADER);
        OptionsBuilder::<OptionsBuild> {
            buffer,
            stage: OptionsBuild,
        }
    }
}

impl OptionsBuilder<OptionsBuild> {
    pub(crate) fn build(self) -> Options {
        Options(self.buffer)
    }
}

impl Options {
    pub(crate) fn new() -> OptionsBuilder<OptionsBuild> {
        OptionsBuilder::<OptionsHeader>::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    // note: junk data
    fn simple_options_builder_test() {
        let Options(_payload) = Options::new().build(); // build uncompressed
    }
}
