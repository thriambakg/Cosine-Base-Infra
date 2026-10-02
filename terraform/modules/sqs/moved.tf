moved {
  from = aws_sqs_queue.main
  to   = aws_sqs_queue.main[0]
}
