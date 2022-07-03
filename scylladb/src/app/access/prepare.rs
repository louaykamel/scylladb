use super::*;
use crate::prelude::{
    Prepare,
    PrepareWorker,
};

/// Specifies helper functions for creating static prepare requests from a keyspace with any access trait definition

pub trait GetStaticPrepareRequest: Keyspace {
    /// Create a static prepare request from a keyspace with a `Select<K, V>` definition.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// #[derive(Clone, Debug)]
    /// struct MyKeyspace {
    ///     pub name: String,
    /// }
    /// # impl MyKeyspace {
    /// #     pub fn new(name: &str) -> Self {
    /// #         Self {
    /// #             name: name.to_string().into(),
    /// #         }
    /// #     }
    /// # }
    /// impl Keyspace for MyKeyspace {
    ///     fn name(&self) -> String {
    ///         self.name.clone()
    ///     }
    ///
    ///     fn opts(&self) -> KeyspaceOpts {
    ///         KeyspaceOptsBuilder::default()
    ///             .replication(Replication::network_topology(maplit::btreemap! {
    ///                 "datacenter1" => 1,
    ///             }))
    ///             .durable_writes(true)
    ///             .build()
    ///             .unwrap()
    ///     }
    /// }
    /// # type MyKeyType = i32;
    /// # type MyVarType = String;
    /// # type MyValueType = f32;
    /// impl Select<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> SelectStatement {
    ///         parse_statement!("SELECT val FROM my_table where key = ? AND var = ?")
    ///     }
    ///     fn bind_values<B: Binder>(builder: B, key: &MyKeyType, variables: &MyVarType) -> B {
    ///         builder.bind(key).bind(variables)
    ///     }
    /// }
    /// MyKeyspace::new("my_keyspace")
    ///     .prepare_select::<MyKeyType, MyVarType, MyValueType>()
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn prepare_select<K, V, O>(&self) -> PrepareRequest
    where
        Self: Select<K, V, O>,
    {
        PrepareRequest::new(self.statement().with_keyspace(self.name()))
    }

    /// Create a static prepare request from a keyspace with a `Insert<K, V>` definition.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// #[derive(Clone, Debug)]
    /// struct MyKeyspace {
    ///     pub name: String,
    /// }
    /// # impl MyKeyspace {
    /// #     pub fn new(name: &str) -> Self {
    /// #         Self {
    /// #             name: name.to_string().into(),
    /// #         }
    /// #     }
    /// # }
    /// impl Keyspace for MyKeyspace {
    ///     fn name(&self) -> String {
    ///         self.name.clone()
    ///     }
    ///
    ///     fn opts(&self) -> KeyspaceOpts {
    ///         KeyspaceOptsBuilder::default()
    ///             .replication(Replication::network_topology(maplit::btreemap! {
    ///                 "datacenter1" => 1,
    ///             }))
    ///             .durable_writes(true)
    ///             .build()
    ///             .unwrap()
    ///     }
    /// }
    /// # type MyKeyType = i32;
    /// # #[derive(Default)]
    /// struct MyValueType {
    ///     value1: f32,
    ///     value2: f32,
    /// }
    /// impl Insert<MyKeyType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> InsertStatement {
    ///         parse_statement!("INSERT INTO my_table (key, val1, val2) VALUES (?,?,?)")
    ///     }
    ///
    ///     fn bind_values<B: Binder>(builder: B, key: &MyKeyType, value: &MyValueType) -> B {
    ///         builder.value(key).value(&value.value1).value(&value.value2)
    ///     }
    /// }
    /// MyKeyspace::new("my_keyspace")
    ///     .prepare_insert::<MyKeyType, MyValueType>()
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn prepare_insert<K, V>(&self) -> PrepareRequest
    where
        Self: Insert<K, V>,
    {
        PrepareRequest::new(self.statement().with_keyspace(self.name()))
    }

    /// Create a static prepare request from a keyspace with a `Update<K, V>` definition.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// #[derive(Clone, Debug)]
    /// struct MyKeyspace {
    ///     pub name: String,
    /// }
    /// # impl MyKeyspace {
    /// #     pub fn new(name: &str) -> Self {
    /// #         Self {
    /// #             name: name.to_string().into(),
    /// #         }
    /// #     }
    /// # }
    /// impl Keyspace for MyKeyspace {
    ///     fn name(&self) -> String {
    ///         self.name.clone()
    ///     }
    ///
    ///     fn opts(&self) -> KeyspaceOpts {
    ///         KeyspaceOptsBuilder::default()
    ///             .replication(Replication::network_topology(maplit::btreemap! {
    ///                 "datacenter1" => 1,
    ///             }))
    ///             .durable_writes(true)
    ///             .build()
    ///             .unwrap()
    ///     }
    /// }
    /// # type MyKeyType = i32;
    /// # type MyVarType = String;
    /// # #[derive(Default)]
    /// struct MyValueType {
    ///     value1: f32,
    ///     value2: f32,
    /// }
    /// impl Update<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> UpdateStatement {
    ///         parse_statement!("UPDATE my_table SET val1 = ?, val2 = ? WHERE key = ? AND var = ?")
    ///     }
    ///
    ///     fn bind_values<B: Binder>(builder: B, key: &MyKeyType, variables: &MyVarType, value: &MyValueType) -> B {
    ///         builder
    ///             .bind(&value.value1)
    ///             .value(&value.value2)
    ///             .value(key)
    ///             .bind(variables)
    ///     }
    /// }
    /// MyKeyspace::new("my_keyspace")
    ///     .prepare_update::<MyKeyType, MyVarType, MyValueType>()
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn prepare_update<K, V, I>(&self) -> PrepareRequest
    where
        Self: Update<K, V, I>,
    {
        PrepareRequest::new(self.statement().with_keyspace(self.name()))
    }

    /// Create a static prepare request from a keyspace with a `Delete<K, V>` definition.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// #[derive(Clone, Debug)]
    /// struct MyKeyspace {
    ///     pub name: String,
    /// }
    /// # impl MyKeyspace {
    /// #     pub fn new(name: &str) -> Self {
    /// #         Self {
    /// #             name: name.to_string().into(),
    /// #         }
    /// #     }
    /// # }
    /// impl Keyspace for MyKeyspace {
    ///     fn name(&self) -> String {
    ///         self.name.clone()
    ///     }
    ///
    ///     fn opts(&self) -> KeyspaceOpts {
    ///         KeyspaceOptsBuilder::default()
    ///             .replication(Replication::network_topology(maplit::btreemap! {
    ///                 "datacenter1" => 1,
    ///             }))
    ///             .durable_writes(true)
    ///             .build()
    ///             .unwrap()
    ///     }
    /// }
    /// # type MyKeyType = i32;
    /// # type MyVarType = String;
    /// # type MyValueType = f32;
    /// impl Delete<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> DeleteStatement {
    ///         parse_statement!("DELETE FROM my_table WHERE key = ? AND var = ?")
    ///     }
    ///     fn bind_values<B: Binder>(builder: B, key: &MyKeyType, variables: &MyVarType) -> B {
    ///         builder.bind(key).bind(variables)
    ///     }
    /// }
    /// MyKeyspace::new("my_keyspace")
    ///     .prepare_delete::<MyKeyType, MyVarType, MyValueType>()
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn prepare_delete<K, V, D>(&self) -> PrepareRequest
    where
        Self: Delete<K, V, D>,
    {
        PrepareRequest::new(self.statement().with_keyspace(self.name()))
    }
}

/// Specifies helper functions for creating dynamic prepare requests from anything that can be interpreted as a keyspace

pub trait GetDynamicPrepareRequest: Keyspace {
    /// Create a dynamic prepare request from a statement. The token `{{keyspace}}` will be replaced with the keyspace
    /// name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "my_keyspace"
    ///     .prepare_with(parse_statement!("DELETE FROM my_table WHERE key = ?"))
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn prepare_with(&self, statement: impl Into<DataManipulationStatement>) -> PrepareRequest {
        PrepareRequest::new(statement)
    }
}

/// Specifies helper functions for creating dynamic prepare requests from anything that can be interpreted as a
/// statement

pub trait AsDynamicPrepareRequest: Into<DataManipulationStatement> {
    /// Create a dynamic prepare request from a statement.
    /// name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// parse_statement!("DELETE FROM my_keyspace.my_table WHERE key = ?")
    ///     .prepare()
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn prepare(self) -> PrepareRequest {
        PrepareRequest::new(self)
    }
}

impl<S: Keyspace> GetStaticPrepareRequest for S {}
impl<S: Keyspace> GetDynamicPrepareRequest for S {}
impl<T: Into<DataManipulationStatement>> AsDynamicPrepareRequest for T {}

/// A request to prepare a record which can be sent to the ring
#[derive(Debug, Clone)]
pub struct PrepareRequest {
    pub(crate) statement: DataManipulationStatement,
    pub(crate) token: i64,
}

impl PrepareRequest {
    fn new(statement: impl Into<DataManipulationStatement>) -> Self {
        PrepareRequest {
            statement: statement.into(),
            token: rand::random(),
        }
    }
}

impl Request for PrepareRequest {
    fn token(&self) -> i64 {
        self.token
    }

    fn statement(&self) -> Statement {
        self.statement.clone().into()
    }

    fn statement_by_id(&self, id: &[u8; 16]) -> Option<DataManipulationStatement> {
        let statement_id: [u8; 16] = md5::compute(self.statement.to_string().as_bytes()).into();
        if &statement_id == id {
            self.statement.clone().into()
        } else {
            None
        }
    }

    fn payload(&self) -> Vec<u8> {
        Prepare::new().statement(&self.statement.to_string()).build().unwrap().0
    }

    fn keyspace(&self) -> Option<String> {
        self.statement.get_keyspace()
    }
}

#[async_trait::async_trait]
impl SendRequestExt for PrepareRequest {
    type Marker = DecodeVoid;
    type Worker = PrepareWorker;
    const TYPE: RequestType = RequestType::Execute;

    fn worker(self) -> Box<Self::Worker> {
        Box::new(PrepareWorker::from(self))
    }
}
