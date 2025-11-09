# OpenSearch Domain Module Outputs

output "domain_id" {
  description = "Unique identifier for the OpenSearch domain"
  value       = aws_opensearch_domain.this.domain_id
}

output "domain_name" {
  description = "Name of the OpenSearch domain"
  value       = aws_opensearch_domain.this.domain_name
}

output "domain_arn" {
  description = "ARN of the OpenSearch domain"
  value       = aws_opensearch_domain.this.arn
}

output "domain_endpoint" {
  description = "Domain-specific endpoint used to submit index, search, and data upload requests"
  value       = aws_opensearch_domain.this.endpoint
}

output "kibana_endpoint" {
  description = "Domain-specific endpoint for Kibana without https scheme"
  value       = aws_opensearch_domain.this.kibana_endpoint
}

output "domain_id_output" {
  description = "Unique identifier for the OpenSearch domain (alias for domain_id)"
  value       = aws_opensearch_domain.this.domain_id
}

output "processing" {
  description = "Status of a configuration change in the domain"
  value       = aws_opensearch_domain.this.processing
}

