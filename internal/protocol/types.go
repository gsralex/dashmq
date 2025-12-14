package protocol

// API Keys
const (
	APIKeyProduce         = 0
	APIKeyFetch           = 1
	APIKeyListOffsets     = 2
	APIKeyMetadata        = 3
	APIKeyOffsetCommit    = 8
	APIKeyOffsetFetch     = 9
	APIKeyFindCoordinator = 10
	APIKeyJoinGroup       = 11
	APIKeyHeartbeat       = 12
	APIKeyLeaveGroup      = 13
	APIKeySyncGroup       = 14
	APIKeyDescribeGroups  = 15
	APIKeyListGroups      = 16
	APIKeySASLHandshake   = 17
	APIKeyAPIVersions     = 18
	APIKeyCreateTopics    = 19
	APIKeyDeleteTopics    = 20
)

// Error codes
const (
	ErrNone                               = 0
	ErrUnknown                            = -1
	ErrOffsetOutOfRange                   = 1
	ErrCorruptMessage                     = 2
	ErrUnknownTopicOrPartition            = 3
	ErrInvalidFetchSize                   = 4
	ErrLeaderNotAvailable                 = 5
	ErrNotLeaderForPartition              = 6
	ErrRequestTimedOut                    = 7
	ErrBrokerNotAvailable                 = 8
	ErrReplicaNotAvailable                = 9
	ErrMessageTooLarge                    = 10
	ErrStaleControllerEpoch               = 11
	ErrOffsetMetadataTooLarge             = 12
	ErrNetworkException                   = 13
	ErrCoordinatorLoadInProgress          = 14
	ErrCoordinatorNotAvailable            = 15
	ErrNotCoordinator                     = 16
	ErrInvalidTopicException              = 17
	ErrRecordListTooLarge                 = 18
	ErrNotEnoughReplicas                  = 19
	ErrNotEnoughReplicasAfterAppend       = 20
	ErrInvalidRequiredAcks                = 21
	ErrIllegalGeneration                  = 22
	ErrInconsistentGroupProtocol          = 23
	ErrInvalidGroupId                     = 24
	ErrUnknownMemberId                    = 25
	ErrInvalidSessionTimeout              = 26
	ErrRebalanceInProgress                = 27
	ErrInvalidCommitOffsetSize            = 28
	ErrTopicAuthorizationFailed           = 29
	ErrGroupAuthorizationFailed           = 30
	ErrClusterAuthorizationFailed         = 31
	ErrInvalidTimestamp                   = 32
	ErrUnsupportedSASLMechanism           = 33
	ErrIllegalSASLState                   = 34
	ErrUnsupportedVersion                 = 35
	ErrTopicAlreadyExists                 = 36
	ErrInvalidPartitions                  = 37
	ErrInvalidReplicationFactor           = 38
	ErrInvalidReplicaAssignment           = 39
	ErrInvalidConfig                      = 40
	ErrNotController                      = 41
	ErrInvalidRequest                     = 42
	ErrUnsupportedForMessageFormat        = 43
	ErrPolicyViolation                    = 44
	ErrOutOfOrderSequenceNumber           = 45
	ErrDuplicateSequenceNumber            = 46
	ErrInvalidProducerEpoch               = 47
	ErrInvalidTransactionState            = 48
	ErrInvalidProducerIdMapping           = 49
	ErrInvalidTransactionTimeout          = 50
	ErrConcurrentTransactions             = 51
	ErrTransactionCoordinatorFenced       = 52
	ErrTransactionalIdAuthorizationFailed = 53
	ErrSecurityDisabled                   = 54
	ErrOperationNotAttempted              = 55
	ErrKafkaStorageError                  = 56
	ErrLogDirNotFound                     = 57
	ErrSASLAuthenticationFailed           = 58
	ErrUnknownProducerId                  = 59
	ErrReassignmentInProgress             = 60
	ErrDelegationTokenAuthDisabled        = 61
	ErrDelegationTokenNotFound            = 62
	ErrDelegationTokenOwnerMismatch       = 63
	ErrDelegationTokenRequestNotAllowed   = 64
	ErrDelegationTokenAuthorizationFailed = 65
	ErrDelegationTokenExpired             = 66
	ErrInvalidPrincipalType               = 67
	ErrNonEmptyGroup                      = 68
	ErrGroupIdNotFound                    = 69
	ErrFetchSessionIdNotFound             = 70
	ErrInvalidFetchSessionEpoch           = 71
	ErrListenerNotFound                   = 72
	ErrTopicDeletionDisabled              = 73
	ErrFencedLeaderEpoch                  = 74
	ErrUnknownLeaderEpoch                 = 75
	ErrUnsupportedCompressionType         = 76
	ErrStaleBrokerEpoch                   = 77
	ErrOffsetNotAvailable                 = 78
	ErrMemberIdRequired                   = 79
	ErrPreferredLeaderNotAvailable        = 80
	ErrGroupMaxSizeReached                = 81
	ErrFencedInstanceId                   = 82
)

// Compression types
const (
	CompressionNone   = 0
	CompressionGZIP   = 1
	CompressionSnappy = 2
	CompressionLZ4    = 3
	CompressionZSTD   = 4
)

// Timestamp types
const (
	TimestampCreateTime    = 0
	TimestampLogAppendTime = 1
)

// Request header
type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string
}

// Response header
type ResponseHeader struct {
	CorrelationID int32
}
