# State moves for resources that gained count toggles (enable_kms / enable_sqs)

moved {
  from = aws_kms_grant.glue_dynamodb_key_access
  to   = aws_kms_grant.glue_dynamodb_key_access[0]
}

moved {
  from = aws_kms_grant.idv_update_glue_dynamodb_key_access
  to   = aws_kms_grant.idv_update_glue_dynamodb_key_access[0]
}

moved {
  from = aws_kms_grant.congress_bills_glue_dynamodb_key_access
  to   = aws_kms_grant.congress_bills_glue_dynamodb_key_access[0]
}

moved {
  from = aws_lambda_event_source_mapping.news_processor_sqs
  to   = aws_lambda_event_source_mapping.news_processor_sqs[0]
}

moved {
  from = aws_lambda_event_source_mapping.orphan_subaward_sqs_trigger
  to   = aws_lambda_event_source_mapping.orphan_subaward_sqs_trigger[0]
}

moved {
  from = aws_lambda_event_source_mapping.dlq_sqs_trigger
  to   = aws_lambda_event_source_mapping.dlq_sqs_trigger[0]
}

moved {
  from = aws_lambda_event_source_mapping.congress_bills_bill_text_sqs_trigger
  to   = aws_lambda_event_source_mapping.congress_bills_bill_text_sqs_trigger[0]
}

moved {
  from = aws_lambda_event_source_mapping.lda_batch_sqs_trigger
  to   = aws_lambda_event_source_mapping.lda_batch_sqs_trigger[0]
}

moved {
  from = aws_lambda_event_source_mapping.lda_pac_autocomplete_sqs_trigger
  to   = aws_lambda_event_source_mapping.lda_pac_autocomplete_sqs_trigger[0]
}
