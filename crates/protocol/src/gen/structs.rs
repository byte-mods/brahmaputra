// Generated Structs


#[derive(Debug, Default, Clone)]
pub struct ProduceRequest {
	pub topic: String,
	pub partition: i32,
	pub acks: i32,
	pub timeout_ms: i32,
	pub batches_length: i64,
	
}

#[derive(Debug, Default, Clone)]
pub struct ProduceResponse {
	pub topic: String,
	pub partition: i32,
	pub error_code: i32,
	pub base_offset: i64,
	pub log_append_time_ms: i64,
	
}

#[derive(Debug, Default, Clone)]
pub struct FetchRequest {
	pub topic: String,
	pub partition: i32,
	pub fetch_offset: i64,
	pub max_bytes: i32,
	pub max_wait_ms: i32,
	pub min_bytes: i32,
	pub isolation_level: i32,
	
}

#[derive(Debug, Default, Clone)]
pub struct FetchResponse {
	pub topic: String,
	pub partition: i32,
	pub error_code: i32,
	pub high_watermark: i64,
	pub last_stable_offset: i64,
	pub batches_length: i64,
	
}

#[derive(Debug, Default, Clone)]
pub struct ListOffsetsRequest {
	pub topic: String,
	pub partition: i32,
	pub timestamp: i64,
	
}

#[derive(Debug, Default, Clone)]
pub struct ListOffsetsResponse {
	pub topic: String,
	pub partition: i32,
	pub error_code: i32,
	pub offset: i64,
	pub timestamp: i64,
	
}

#[derive(Debug, Default, Clone)]
pub struct BrokerInfo {
	pub broker_id: i32,
	pub host: String,
	pub port: i32,
	
}

#[derive(Debug, Default, Clone)]
pub struct PartitionInfo {
	pub partition: i32,
	pub leader: i32,
	pub replicas: Vec<i32>,
	pub isr: Vec<i32>,
	pub leader_epoch: i32,
	
}

#[derive(Debug, Default, Clone)]
pub struct TopicInfo {
	pub name: String,
	pub error_code: i32,
	pub partitions: Vec<PartitionInfo>,
	
}

#[derive(Debug, Default, Clone)]
pub struct MetadataRequest {
	pub topics: Vec<String>,
	
}

#[derive(Debug, Default, Clone)]
pub struct MetadataResponse {
	pub error_code: i32,
	pub brokers: Vec<BrokerInfo>,
	pub controller_id: i32,
	pub topics: Vec<TopicInfo>,
	
}

#[derive(Debug, Default, Clone)]
pub struct GroupMemberInfo {
	pub member_id: String,
	pub subscription_topics: Vec<String>,
	pub assignment: Vec<AssignedPartition>,
	
}

#[derive(Debug, Default, Clone)]
pub struct JoinGroupRequest {
	pub group_id: String,
	pub session_timeout_ms: i32,
	pub rebalance_timeout_ms: i32,
	pub member_id: String,
	pub subscription_topics: Vec<String>,
	pub group_instance_id: String,
	
}

#[derive(Debug, Default, Clone)]
pub struct JoinGroupResponse {
	pub error_code: i32,
	pub generation: i32,
	pub member_id: String,
	pub leader_member_id: String,
	pub members: Vec<GroupMemberInfo>,
	
}

#[derive(Debug, Default, Clone)]
pub struct AssignedPartition {
	pub topic: String,
	pub partition: i32,
	
}

#[derive(Debug, Default, Clone)]
pub struct MemberAssignment {
	pub member_id: String,
	pub partitions: Vec<AssignedPartition>,
	
}

#[derive(Debug, Default, Clone)]
pub struct SyncGroupRequest {
	pub group_id: String,
	pub generation: i32,
	pub member_id: String,
	pub assignments: Vec<MemberAssignment>,
	
}

#[derive(Debug, Default, Clone)]
pub struct SyncGroupResponse {
	pub error_code: i32,
	pub assignment: Vec<AssignedPartition>,
	
}

#[derive(Debug, Default, Clone)]
pub struct HeartbeatRequest {
	pub group_id: String,
	pub generation: i32,
	pub member_id: String,
	
}

#[derive(Debug, Default, Clone)]
pub struct HeartbeatResponse {
	pub error_code: i32,
	
}

#[derive(Debug, Default, Clone)]
pub struct OffsetCommitEntry {
	pub topic: String,
	pub partition: i32,
	pub offset: i64,
	
}

#[derive(Debug, Default, Clone)]
pub struct OffsetCommitRequest {
	pub group_id: String,
	pub generation: i32,
	pub member_id: String,
	pub offsets: Vec<OffsetCommitEntry>,
	
}

#[derive(Debug, Default, Clone)]
pub struct OffsetCommitResponse {
	pub error_code: i32,
	
}

#[derive(Debug, Default, Clone)]
pub struct OffsetFetchRequest {
	pub group_id: String,
	pub partitions: Vec<AssignedPartition>,
	
}

#[derive(Debug, Default, Clone)]
pub struct OffsetFetchEntry {
	pub topic: String,
	pub partition: i32,
	pub offset: i64,
	
}

#[derive(Debug, Default, Clone)]
pub struct OffsetFetchResponse {
	pub error_code: i32,
	pub offsets: Vec<OffsetFetchEntry>,
	
}

#[derive(Debug, Default, Clone)]
pub struct ListedGroup {
	pub group_id: String,
	pub state: String,
	pub generation: i32,
	pub member_count: i32,
	pub coordinator_partition: i32,
	
}

#[derive(Debug, Default, Clone)]
pub struct ListGroupsRequest {
	pub states: Vec<String>,
	
}

#[derive(Debug, Default, Clone)]
pub struct ListGroupsResponse {
	pub error_code: i32,
	pub groups: Vec<ListedGroup>,
	
}

#[derive(Debug, Default, Clone)]
pub struct DescribedMember {
	pub member_id: String,
	pub subscription_topics: Vec<String>,
	pub assignment: Vec<AssignedPartition>,
	
}

#[derive(Debug, Default, Clone)]
pub struct DescribeGroupRequest {
	pub group_id: String,
	
}

#[derive(Debug, Default, Clone)]
pub struct DescribeGroupResponse {
	pub error_code: i32,
	pub group_id: String,
	pub state: String,
	pub generation: i32,
	pub leader_member_id: String,
	pub coordinator_partition: i32,
	pub members: Vec<DescribedMember>,
	pub offsets: Vec<OffsetFetchEntry>,
	
}

#[derive(Debug, Default, Clone)]
pub struct ProduceMultiPartition {
	pub topic: String,
	pub partition: i32,
	pub batches_length: i64,
	
}

#[derive(Debug, Default, Clone)]
pub struct ProduceMultiRequest {
	pub acks: i32,
	pub timeout_ms: i32,
	pub partitions: Vec<ProduceMultiPartition>,
	
}

#[derive(Debug, Default, Clone)]
pub struct ProduceMultiResult {
	pub topic: String,
	pub partition: i32,
	pub error_code: i32,
	pub base_offset: i64,
	pub log_append_time_ms: i64,
	
}

#[derive(Debug, Default, Clone)]
pub struct ProduceMultiResponse {
	pub results: Vec<ProduceMultiResult>,
	
}

#[derive(Debug, Default, Clone)]
pub struct FetchMultiPartition {
	pub topic: String,
	pub partition: i32,
	pub fetch_offset: i64,
	pub max_bytes: i32,
	
}

#[derive(Debug, Default, Clone)]
pub struct FetchMultiRequest {
	pub max_wait_ms: i32,
	pub min_bytes: i32,
	pub isolation_level: i32,
	pub partitions: Vec<FetchMultiPartition>,
	
}

#[derive(Debug, Default, Clone)]
pub struct FetchMultiResult {
	pub topic: String,
	pub partition: i32,
	pub error_code: i32,
	pub high_watermark: i64,
	pub last_stable_offset: i64,
	pub batches_length: i64,
	
}

#[derive(Debug, Default, Clone)]
pub struct FetchMultiResponse {
	pub results: Vec<FetchMultiResult>,
	
}

#[derive(Debug, Default, Clone)]
pub struct ApiVersionRange {
	pub api_key: i32,
	pub min_version: i32,
	pub max_version: i32,
	
}

#[derive(Debug, Default, Clone)]
pub struct ApiVersionsRequest {
	pub client_software_name: String,
	pub client_software_version: String,
	
}

#[derive(Debug, Default, Clone)]
pub struct ApiVersionsResponse {
	pub error_code: i32,
	pub api_versions: Vec<ApiVersionRange>,
	pub broker_version: String,
	pub throttle_time_ms: i32,
	
}

#[derive(Debug, Default, Clone)]
pub struct OffsetCommitRecord {
	pub group_id: String,
	pub topic: String,
	pub partition: i32,
	pub offset: i64,
	pub commit_timestamp_ms: i64,
	
}

#[derive(Debug, Default, Clone)]
pub struct GroupMemberRecord {
	pub member_id: String,
	pub subscription_topics: Vec<String>,
	pub assignment: Vec<AssignedPartition>,
	pub group_instance_id: String,
	
}

#[derive(Debug, Default, Clone)]
pub struct GroupMetadataRecord {
	pub group_id: String,
	pub generation: i32,
	pub leader_member_id: String,
	pub members: Vec<GroupMemberRecord>,
	
}

#[derive(Debug, Default, Clone)]
pub struct TombstoneRecord {
	pub group_id: String,
	pub topic: String,
	pub partition: i32,
	
}

#[derive(Debug, Default, Clone)]
pub struct AuthenticateRequest {
	pub username: String,
	pub password: String,
	
}

#[derive(Debug, Default, Clone)]
pub struct AuthenticateResponse {
	pub error_code: i32,
	pub principal: String,
	pub role: String,
	
}

#[derive(Debug, Default, Clone)]
pub struct LeaveGroupRequest {
	pub group_id: String,
	pub member_id: String,
	
}

#[derive(Debug, Default, Clone)]
pub struct LeaveGroupResponse {
	pub error_code: i32,
	
}

#[derive(Debug, Default, Clone)]
pub struct DescribeClusterBroker {
	pub broker_id: i32,
	pub host: String,
	pub port: i32,
	pub rack: String,
	
}

#[derive(Debug, Default, Clone)]
pub struct DescribeClusterRequest {
	pub include_cluster_authorized_operations: bool,
	
}

#[derive(Debug, Default, Clone)]
pub struct DescribeClusterResponse {
	pub error_code: i32,
	pub cluster_id: String,
	pub controller_id: i32,
	pub brokers: Vec<DescribeClusterBroker>,
	
}

#[derive(Debug, Default, Clone)]
pub struct ConfigEntry {
	pub name: String,
	pub value: String,
	pub is_default: bool,
	
}

#[derive(Debug, Default, Clone)]
pub struct DescribeConfigsRequest {
	pub resource_type: String,
	pub resource_name: String,
	pub config_names: Vec<String>,
	
}

#[derive(Debug, Default, Clone)]
pub struct DescribeConfigsResponse {
	pub error_code: i32,
	pub resource_type: String,
	pub resource_name: String,
	pub configs: Vec<ConfigEntry>,
	
}

#[derive(Debug, Default, Clone)]
pub struct LogDirPartition {
	pub topic: String,
	pub partition: i32,
	pub size_bytes: i64,
	pub offset_lag: i64,
	pub is_leader: bool,
	
}

#[derive(Debug, Default, Clone)]
pub struct LogDirInfo {
	pub error_code: i32,
	pub log_dir: String,
	pub offline_reason: String,
	pub total_bytes: i64,
	pub usable_bytes: i64,
	pub partitions: Vec<LogDirPartition>,
	
}

#[derive(Debug, Default, Clone)]
pub struct DescribeLogDirsRequest {
	pub topics: Vec<String>,
	
}

#[derive(Debug, Default, Clone)]
pub struct DescribeLogDirsResponse {
	pub error_code: i32,
	pub log_dirs: Vec<LogDirInfo>,
	
}

#[derive(Debug, Default, Clone)]
pub struct DeleteRecordsPartition {
	pub topic: String,
	pub partition: i32,
	pub offset: i64,
	
}

#[derive(Debug, Default, Clone)]
pub struct DeleteRecordsRequest {
	pub timeout_ms: i32,
	pub partitions: Vec<DeleteRecordsPartition>,
	
}

#[derive(Debug, Default, Clone)]
pub struct DeleteRecordsResult {
	pub topic: String,
	pub partition: i32,
	pub error_code: i32,
	pub low_watermark: i64,
	
}

#[derive(Debug, Default, Clone)]
pub struct DeleteRecordsResponse {
	pub results: Vec<DeleteRecordsResult>,
	
}

#[derive(Debug, Default, Clone)]
pub struct TxnPartition {
	pub topic: String,
	pub partition: i32,
	
}

#[derive(Debug, Default, Clone)]
pub struct AddPartitionsToTxnRequest {
	pub transactional_id: String,
	pub producer_id: i64,
	pub producer_epoch: i32,
	pub partitions: Vec<TxnPartition>,
	
}

#[derive(Debug, Default, Clone)]
pub struct AddPartitionsToTxnResponse {
	pub error_code: i32,
	
}

#[derive(Debug, Default, Clone)]
pub struct AddOffsetsToTxnRequest {
	pub transactional_id: String,
	pub producer_id: i64,
	pub producer_epoch: i32,
	pub group_id: String,
	
}

#[derive(Debug, Default, Clone)]
pub struct AddOffsetsToTxnResponse {
	pub error_code: i32,
	
}

#[derive(Debug, Default, Clone)]
pub struct EndTxnRequest {
	pub transactional_id: String,
	pub producer_id: i64,
	pub producer_epoch: i32,
	pub committed: bool,
	
}

#[derive(Debug, Default, Clone)]
pub struct EndTxnResponse {
	pub error_code: i32,
	
}

#[derive(Debug, Default, Clone)]
pub struct TxnOffsetCommitRequest {
	pub transactional_id: String,
	pub producer_id: i64,
	pub producer_epoch: i32,
	pub group_id: String,
	pub offsets: Vec<OffsetCommitEntry>,
	
}

#[derive(Debug, Default, Clone)]
pub struct TxnOffsetCommitResponse {
	pub error_code: i32,
	
}

#[derive(Debug, Default, Clone)]
pub struct TransactionStateRecord {
	pub transactional_id: String,
	pub producer_id: i64,
	pub producer_epoch: i32,
	pub state: String,
	pub timeout_ms: i32,
	pub last_update_ms: i64,
	pub partitions: Vec<TxnPartition>,
	
}

#[derive(Debug, Default, Clone)]
pub struct TxnMarker {
	pub producer_id: i64,
	pub producer_epoch: i32,
	pub committed: bool,
	pub partitions: Vec<TxnPartition>,
	
}

#[derive(Debug, Default, Clone)]
pub struct WriteTxnMarkersRequest {
	pub markers: Vec<TxnMarker>,
	
}

#[derive(Debug, Default, Clone)]
pub struct TxnMarkerResult {
	pub topic: String,
	pub partition: i32,
	pub error_code: i32,
	
}

#[derive(Debug, Default, Clone)]
pub struct WriteTxnMarkersResponse {
	pub results: Vec<TxnMarkerResult>,
	
}
