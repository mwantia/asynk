package event

const (
	MetadataRetryCount    string = "retry_count"
	MetadataRetryLimit    string = "retry_limit"
	MetadataLastError     string = "last_error"
	MetadataLastAttempt   string = "last_attempt"
	MetadataArchiveReason string = "archive_reason"
)

type Metadata map[string]string
