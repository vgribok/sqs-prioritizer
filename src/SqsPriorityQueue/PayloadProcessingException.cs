#nullable enable

namespace SqsProcessorContainer
{
    /// <summary>
    /// Thrown and caught by the <see cref="SqsProcessor{TMsgModel}"/> 
    /// to ensure consistent error handling and re-processing/DLQ forwarding.
    /// </summary>
    public class PayloadProcessingException : Exception
    {
        public PayloadProcessingException(Exception inner, string? errorText = null) 
            : base(errorText ?? inner.Message, inner) 
        { 
        }
    }
}
