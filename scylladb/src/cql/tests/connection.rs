#[allow(unused_imports)]
use crate::cql::Cql;

#[tokio::test]
async fn establish_connection_with_regular_cql_port() {
    let cql = Cql::new()
        .address(([172, 17, 0, 2], 9042).into())
        .shard_id(0)
        .tokens()
        .build()
        .await;
    assert!(cql.is_ok());
}

#[tokio::test]
async fn establish_connection_with_shard_aware_port() {
    let cql = Cql::new()
        .address(([172, 17, 0, 2], 19042).into())
        .shard_id(0)
        .tokens()
        .build()
        .await;
    assert!(cql.is_ok());
}
