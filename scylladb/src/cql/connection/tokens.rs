use crate::{
    cql::{
        frame::decoder::{
            ColumnDecoder,
            Frame,
        },
        Decoder,
        Metadata,
        Rows,
    },
    rows,
};

rows!(
    rows: Info,
    row: Row {
        data_center: String,
        tokens: Vec<String>,
    },
    row_into: Row
);
