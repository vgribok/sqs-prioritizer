#nullable enable

using Amazon;
using Microsoft.Extensions.Logging;

namespace SqsProcessorContainer
{
    /// <summary>
    /// Convenience class when subclasses do their own message parsing
    /// and deserialization of messages.
    /// </summary>
    public abstract class TextMessageProcessor : SqsProcessor<string>
    {
        public TextMessageProcessor(Arn[] queueArns,
                                    ILogger<TextMessageProcessor> logger,
                                    int highPriorityWaitTimeoutSeconds,
                                    int failureVisibilityTimeoutSeconds,
                                    int messageBatchSize)
            : base(queueArns, logger, highPriorityWaitTimeoutSeconds, failureVisibilityTimeoutSeconds, messageBatchSize)
        {
        }

        protected override string DeserializeMessage(string messageBody) => messageBody;
    }
}
