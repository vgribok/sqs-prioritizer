#nullable enable
using Constructs;

namespace sqs_long_delay_construct
{
    /// <summary>
    /// Provisions an SQS queue/DLQ pair, plus other infra components for enabling 
    /// long delays between message processing retry attempts.
    /// </summary>
    public class SqsWithLongDelayRetry : Construct
    {
        public SqsWithLongDelayRetry(Construct scope, string id, SqsWithLongDelayRetryProps? props)
            : base(scope, id)
        {
        }
    }
}
