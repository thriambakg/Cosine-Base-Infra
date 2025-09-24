# SQS Module Variables
# modules/sqs/variables.tf

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment name (e.g., development, staging, production)"
  type        = string
}

variable "queue_name" {
  description = "Name of the SQS queue (will be prefixed with project_name and suffixed with environment)"
  type        = string
}

variable "purpose" {
  description = "Purpose/description of the queue for tagging"
  type        = string
  default     = "MessageQueue"
}

variable "message_retention_seconds" {
  description = "The number of seconds Amazon SQS retains a message"
  type        = number
  default     = 1209600 # 14 days
}

variable "visibility_timeout_seconds" {
  description = "The visibility timeout for the queue"
  type        = number
  default     = 30
}

variable "max_receive_count" {
  description = "The number of times a message is delivered to the source queue before being moved to the dead-letter queue"
  type        = number
  default     = 3
}

variable "delay_seconds" {
  description = "The time in seconds that the delivery of all messages in the queue will be delayed"
  type        = number
  default     = 0
}

variable "max_message_size" {
  description = "The limit of how many bytes a message can contain before Amazon SQS rejects it"
  type        = number
  default     = 262144 # 256 KB
}

variable "receive_wait_time_seconds" {
  description = "The time for which a ReceiveMessage call will wait for a message to arrive (long polling)"
  type        = number
  default     = 0
}

variable "enable_dlq" {
  description = "Whether to enable a dead letter queue"
  type        = bool
  default     = true
}

variable "dlq_message_retention_seconds" {
  description = "The number of seconds Amazon SQS retains a message in the dead letter queue"
  type        = number
  default     = 1209600 # 14 days
}

variable "dlq_visibility_timeout_seconds" {
  description = "The visibility timeout for the dead letter queue"
  type        = number
  default     = 30
}

variable "fifo_queue" {
  description = "Whether to create a FIFO queue"
  type        = bool
  default     = false
}

variable "content_based_deduplication" {
  description = "Enables content-based deduplication for FIFO queues"
  type        = bool
  default     = false
}

variable "kms_key_id" {
  description = "The ID of an AWS-managed customer master key (CMK) for Amazon SQS or a custom CMK"
  type        = string
  default     = null
}

variable "kms_data_key_reuse_period_seconds" {
  description = "The length of time, in seconds, for which Amazon SQS can reuse a data key to encrypt or decrypt messages before calling AWS KMS again"
  type        = number
  default     = 300
}

variable "message_ttl_seconds" {
  description = "The time in seconds after which a message will be automatically deleted (TTL)"
  type        = number
  default     = null
}

variable "tags" {
  description = "A map of tags to assign to the resource"
  type        = map(string)
  default     = {}
}
