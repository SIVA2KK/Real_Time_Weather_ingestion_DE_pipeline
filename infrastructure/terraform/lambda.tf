resource "aws_lambda_function" "weather_puller" {
  function_name = "weather-api-puller"
  handler       = "lambda_puller.lambda_handler"
  runtime       = "python3.12"
  timeout       = 30
  memory_size   = 256
  filename = "C:/projects/weather_ingestion/lambda.zip"

  environment {
    variables = {
      S3_BUCKET = aws_s3_bucket.weather_lake.bucket
    }
  }

  role = aws_iam_role.lambda_role.arn
}

# EventBridge: fire every 60 seconds
resource "aws_cloudwatch_event_rule" "weather_trigger" {
  name                = "weather-puller-trigger"
  schedule_expression = "rate(1 minute)"
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule = aws_cloudwatch_event_rule.weather_trigger.name
  arn  = aws_lambda_function.weather_puller.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.weather_puller.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.weather_trigger.arn
}