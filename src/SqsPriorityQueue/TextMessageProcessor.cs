#nullable enable

using Microsoft.Extensions.Logging;

namespace SqsProcessorContainer
{
    /// <summary>
    /// Convenience class when subclasses do their own message parsing
    /// and deserialization of messages.
    /// </summary>
    public abstract class TextMessageProcessor : SqsProcessor<string>
    {
        public TextMessageProcessor(SqsPrioritySettings settings, ILogger<TextMessageProcessor> logger)
            : base(settings, logger)
        {
        }

        protected override string DeserializeMessage(string messageBody) => messageBody;
    }
}
