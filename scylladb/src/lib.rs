pub mod cql;
#[cfg(not(feature = "app"))]
pub use cql::*;
#[cfg(feature = "app")]
pub mod app;

#[cfg(feature = "app")]
pub mod prelude {
    pub use super::{
        app::{
            access::*,
            cluster::ClusterHandleExt,
            worker::*,
            ScyllaHandleExt,
            *,
        },
        cql::{
            Batch,
            Binder,
            ColumnDecoder,
            ColumnEncoder,
            ColumnValue,
            Consistency,
            Decoder,
            Frame,
            Iter,
            Prepare,
            PreparedStatement,
            Query,
            QueryStatement,
            Row,
            Rows,
            RowsDecoder,
            Statements,
            TokenEncoder,
            VoidDecoder,
        },
    };
    pub use maplit::{
        self,
        *,
    };
    pub use overclock::core::*;
    pub use scylladb_macros::*;
    pub use scylladb_parse::*;
}
