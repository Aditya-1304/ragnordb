use super::command_codec::TabletCommand;
use crate::ids::{NodeId, RaftGroupId, RequestId, TabletId, Timestamp};
use crate::proto::rpc;

#[derive(Debug, Clone, PartialEq)]
pub struct RpcFrame {
    pub msg_type: MessageType,
    pub raft_group_id: RaftGroupId,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    RaftConsensus,
    TabletCommandRequest,
    TabletCommandResponse,
    MetadataRequest,
    MetadataResponse,
}

impl MessageType {
    pub fn to_proto(&self) -> rpc::MessageType {
        match self {
            MessageType::RaftConsensus => rpc::MessageType::RaftConsensus,
            MessageType::TabletCommandRequest => rpc::MessageType::TabletCommandRequest,
            MessageType::TabletCommandResponse => rpc::MessageType::TabletCommandResponse,
            MessageType::MetadataRequest => rpc::MessageType::MetadataRequest,
            MessageType::MetadataResponse => rpc::MessageType::MetadataResponse,
        }
    }

    pub fn from_proto(proto: rpc::MessageType) -> Result<Self, &'static str> {
        match proto {
            rpc::MessageType::RaftConsensus => Ok(MessageType::RaftConsensus),
            rpc::MessageType::TabletCommandRequest => Ok(MessageType::TabletCommandRequest),
            rpc::MessageType::TabletCommandResponse => Ok(MessageType::TabletCommandResponse),
            rpc::MessageType::MetadataRequest => Ok(MessageType::MetadataRequest),
            rpc::MessageType::MetadataResponse => Ok(MessageType::MetadataResponse),
            rpc::MessageType::Unspecified => Err("unspecified message type"),
        }
    }
}

impl RpcFrame {
    pub fn to_proto(&self) -> rpc::RpcFrame {
        rpc::RpcFrame {
            msg_type: self.msg_type.to_proto() as i32,
            raft_group_id: Some(self.raft_group_id.to_proto()),
            payload: self.payload.clone(),
        }
    }

    pub fn from_proto(proto: rpc::RpcFrame) -> Result<Self, &'static str> {
        Ok(RpcFrame {
            msg_type: MessageType::from_proto(
                rpc::MessageType::try_from(proto.msg_type).map_err(|_| "invalid msg_type")?,
            )?,
            raft_group_id: RaftGroupId::from_proto(
                proto.raft_group_id.ok_or("missing raft_group_id")?,
            ),
            payload: proto.payload,
        })
    }
}

pub struct TabletCommandRequest {
    pub request_id: RequestId,
    pub command: TabletCommand,
}

impl TabletCommandRequest {
    pub fn to_proto(&self) -> rpc::TabletCommandRequest {
        rpc::TabletCommandRequest {
            request_id: Some(self.request_id.to_proto()),
            command: Some(self.command.to_proto()),
        }
    }

    pub fn from_proto(proto: rpc::TabletCommandRequest) -> Result<Self, &'static str> {
        Ok(TabletCommandRequest {
            request_id: RequestId::from_proto(proto.request_id.ok_or("missing request_id")?)?,
            command: TabletCommand::from_proto(proto.command.ok_or("missing command")?)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TabletCommandResponse {
    pub request_id: RequestId,
    pub success: bool,
    pub error_message: String,
    pub error_code: String,
    pub retryable: bool,
    pub result_data: Vec<u8>,
}

impl TabletCommandResponse {
    pub fn to_proto(&self) -> rpc::TabletCommandResponse {
        rpc::TabletCommandResponse {
            request_id: Some(self.request_id.to_proto()),
            success: self.success,
            error_message: self.error_message.clone(),
            error_code: self.error_code.clone(),
            retryable: self.retryable,
            result_data: self.result_data.clone(),
        }
    }

    pub fn from_proto(proto: rpc::TabletCommandResponse) -> Result<Self, &'static str> {
        Ok(TabletCommandResponse {
            request_id: RequestId::from_proto(proto.request_id.ok_or("missing request_id")?)?,
            success: proto.success,
            error_message: proto.error_message,
            error_code: proto.error_code,
            retryable: proto.retryable,
            result_data: proto.result_data,
        })
    }
}

pub enum MetadataRequest {
    AllocateTimestamp,
    LookupTablet { table_id: u64, key: Vec<u8> },
    LookupSchema { table_id: u64 },
}

impl MetadataRequest {
    pub fn to_proto(&self) -> rpc::MetadataRequest {
        let request = match self {
            MetadataRequest::AllocateTimestamp => Some(
                rpc::metadata_request::Request::AllocateTimestamp(rpc::AllocateTimestampRequest {}),
            ),
            MetadataRequest::LookupTablet { table_id, key } => Some(
                rpc::metadata_request::Request::LookupTablet(rpc::LookupTabletRequest {
                    table_id: *table_id,
                    key: key.clone(),
                }),
            ),
            MetadataRequest::LookupSchema { table_id } => Some(
                rpc::metadata_request::Request::LookupSchema(rpc::LookupSchemaRequest {
                    table_id: *table_id,
                }),
            ),
        };
        rpc::MetadataRequest { request }
    }

    pub fn from_proto(proto: rpc::MetadataRequest) -> Result<Self, &'static str> {
        match proto.request {
            Some(rpc::metadata_request::Request::AllocateTimestamp(_)) => {
                Ok(MetadataRequest::AllocateTimestamp)
            }
            Some(rpc::metadata_request::Request::LookupTablet(req)) => {
                Ok(MetadataRequest::LookupTablet {
                    table_id: req.table_id,
                    key: req.key,
                })
            }
            Some(rpc::metadata_request::Request::LookupSchema(req)) => {
                Ok(MetadataRequest::LookupSchema {
                    table_id: req.table_id,
                })
            }
            None => Err("missing metadata request"),
        }
    }
}

pub enum MetadataResponse {
    AllocateTimestamp {
        timestamp: Timestamp,
    },
    LookupTablet {
        tablet_id: TabletId,
        leader_id: NodeId,
        replica_ids: Vec<NodeId>,
    },
    LookupSchema {
        schema_bytes: Vec<u8>,
        schema_version: u64,
    },
}

impl MetadataResponse {
    pub fn to_proto(&self) -> rpc::MetadataResponse {
        let response = match self {
            MetadataResponse::AllocateTimestamp { timestamp } => {
                Some(rpc::metadata_response::Response::AllocateTimestamp(
                    rpc::AllocateTimestampResponse {
                        timestamp: Some(timestamp.to_proto()),
                    },
                ))
            }
            MetadataResponse::LookupTablet {
                tablet_id,
                leader_id,
                replica_ids,
            } => Some(rpc::metadata_response::Response::LookupTablet(
                rpc::LookupTabletResponse {
                    tablet_id: Some(tablet_id.to_proto()),
                    leader_id: Some(leader_id.to_proto()),
                    replica_ids: replica_ids.iter().map(|id| id.to_proto()).collect(),
                },
            )),
            MetadataResponse::LookupSchema {
                schema_bytes,
                schema_version,
            } => Some(rpc::metadata_response::Response::LookupSchema(
                rpc::LookupSchemaResponse {
                    schema_bytes: schema_bytes.clone(),
                    schema_version: *schema_version,
                },
            )),
        };
        rpc::MetadataResponse { response }
    }

    pub fn from_proto(proto: rpc::MetadataResponse) -> Result<Self, &'static str> {
        match proto.response {
            Some(rpc::metadata_response::Response::AllocateTimestamp(resp)) => {
                Ok(MetadataResponse::AllocateTimestamp {
                    timestamp: Timestamp::from_proto(resp.timestamp.ok_or("missing timestamp")?),
                })
            }
            Some(rpc::metadata_response::Response::LookupTablet(resp)) => {
                Ok(MetadataResponse::LookupTablet {
                    tablet_id: TabletId::from_proto(resp.tablet_id.ok_or("missing tablet_id")?),
                    leader_id: NodeId::from_proto(resp.leader_id.ok_or("missing leader_id")?),
                    replica_ids: resp
                        .replica_ids
                        .into_iter()
                        .map(NodeId::from_proto)
                        .collect(),
                })
            }

            Some(rpc::metadata_response::Response::LookupSchema(resp)) => {
                Ok(MetadataResponse::LookupSchema {
                    schema_bytes: resp.schema_bytes,
                    schema_version: resp.schema_version,
                })
            }
            None => Err("missing metadata response"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_type_unspecified_rejected() {
        assert!(MessageType::from_proto(rpc::MessageType::Unspecified).is_err());
    }

    #[test]
    fn rpc_frame_roundtrip() {
        let frame = RpcFrame {
            msg_type: MessageType::TabletCommandRequest,
            raft_group_id: RaftGroupId(5),
            payload: vec![1, 2, 3, 4],
        };
        let proto = frame.to_proto();
        let decoded = RpcFrame::from_proto(proto).unwrap();
        assert!(matches!(
            decoded.msg_type,
            MessageType::TabletCommandRequest
        ));
        assert_eq!(decoded.raft_group_id.0, 5);
        assert_eq!(decoded.payload, vec![1, 2, 3, 4]);
    }

    #[test]
    fn tablet_command_request_roundtrip() {
        use crate::command_codec::{CommitCommand, TabletCommand};
        let req = TabletCommandRequest {
            request_id: RequestId {
                client_id: 12345,
                sequence: 1,
            },
            command: TabletCommand::Commit(CommitCommand {
                txn_id: crate::ids::TxnId(1),
                start_timestamp: Timestamp(100),
                commit_timestamp: Timestamp(105),
                key: b"/table/1/pk/1".to_vec(),
            }),
        };
        let proto = req.to_proto();
        let decoded = TabletCommandRequest::from_proto(proto).unwrap();
        assert_eq!(decoded.request_id.sequence, 1);
        assert!(matches!(decoded.command, TabletCommand::Commit(_)));
    }

    #[test]
    fn tablet_command_response_roundtrip() {
        let resp = TabletCommandResponse {
            request_id: RequestId {
                client_id: 999,
                sequence: 5,
            },
            success: true,
            error_message: String::new(),
            error_code: String::new(),
            retryable: false,
            result_data: vec![10, 20, 30],
        };
        let proto = resp.to_proto();
        let decoded = TabletCommandResponse::from_proto(proto).unwrap();
        assert!(decoded.success);
        assert_eq!(decoded.result_data, vec![10, 20, 30]);
    }

    #[test]
    fn metadata_request_allocate_ts_roundtrip() {
        let req = MetadataRequest::AllocateTimestamp;
        let proto = req.to_proto();
        let decoded = MetadataRequest::from_proto(proto).unwrap();
        assert!(matches!(decoded, MetadataRequest::AllocateTimestamp));
    }

    #[test]
    fn metadata_request_lookup_tablet_roundtrip() {
        let req = MetadataRequest::LookupTablet {
            table_id: 100,
            key: b"pk1".to_vec(),
        };
        let proto = req.to_proto();
        let decoded = MetadataRequest::from_proto(proto).unwrap();
        assert!(matches!(
            decoded,
            MetadataRequest::LookupTablet { table_id: 100, .. }
        ));
    }

    #[test]
    fn metadata_request_lookup_schema_roundtrip() {
        let req = MetadataRequest::LookupSchema { table_id: 200 };
        let proto = req.to_proto();
        let decoded = MetadataRequest::from_proto(proto).unwrap();
        assert!(matches!(
            decoded,
            MetadataRequest::LookupSchema { table_id: 200 }
        ));
    }

    #[test]
    fn metadata_request_missing_rejected() {
        let proto = rpc::MetadataRequest { request: None };
        assert!(MetadataRequest::from_proto(proto).is_err());
    }

    #[test]
    fn metadata_response_allocate_ts_roundtrip() {
        let resp = MetadataResponse::AllocateTimestamp {
            timestamp: Timestamp(500),
        };
        let proto = resp.to_proto();
        let decoded = MetadataResponse::from_proto(proto).unwrap();
        assert!(
            matches!(decoded, MetadataResponse::AllocateTimestamp { timestamp } if timestamp.0 == 500)
        );
    }

    #[test]
    fn metadata_response_lookup_tablet_roundtrip() {
        let resp = MetadataResponse::LookupTablet {
            tablet_id: TabletId(10),
            leader_id: NodeId(1),
            replica_ids: vec![NodeId(1), NodeId(2), NodeId(3)],
        };
        let proto = resp.to_proto();
        let decoded = MetadataResponse::from_proto(proto).unwrap();
        assert!(
            matches!(decoded, MetadataResponse::LookupTablet { tablet_id, .. } if tablet_id.0 == 10)
        );
    }

    #[test]
    fn metadata_response_lookup_schema_roundtrip() {
        let resp = MetadataResponse::LookupSchema {
            schema_bytes: vec![0xAA, 0xBB],
            schema_version: 3,
        };
        let proto = resp.to_proto();
        let decoded = MetadataResponse::from_proto(proto).unwrap();
        assert!(matches!(
            decoded,
            MetadataResponse::LookupSchema {
                schema_version: 3,
                ..
            }
        ));
    }

    #[test]
    fn metadata_response_missing_rejected() {
        let proto = rpc::MetadataResponse { response: None };
        assert!(MetadataResponse::from_proto(proto).is_err());
    }
}
