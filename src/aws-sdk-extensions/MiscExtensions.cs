#nullable enable
using Amazon;

namespace aws_sdk_extensions
{
    public static class MiscExtensions
    {
        public static string SqsArnToUrl(string arn) =>
            // ARN: arn:aws:sqs:us-east-2:123456789012:1st-one
            // URL: https://sqs.us-east-2.amazonaws.com/123456789012/1st-one
            Arn.Parse(arn).SqsArnToUrl();

        public static string SqsArnToUrl(this Arn arn) =>
            // ARN: arn:aws:sqs:us-east-2:123456789012:1st-one
            // URL: https://sqs.us-east-2.amazonaws.com/123456789012/1st-one
            $"https://sqs.{arn.Region}.amazonaws.com/{arn.AccountId}/{arn.Resource}";
    }
}
