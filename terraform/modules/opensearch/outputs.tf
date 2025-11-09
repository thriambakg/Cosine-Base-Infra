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

output "dashboard_endpoint" {
  description = "Domain-specific endpoint for OpenSearch Dashboards (replaces deprecated kibana_endpoint)"
  value       = aws_opensearch_domain.this.dashboard_endpoint
}

output "domain_id_output" {
  description = "Unique identifier for the OpenSearch domain (alias for domain_id)"
  value       = aws_opensearch_domain.this.domain_id
}

