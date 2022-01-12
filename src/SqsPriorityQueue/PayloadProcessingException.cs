#nullable enable

namespace SqsProcessorContainer
{
    public class PayloadProcessingException : Exception
    {
        public PayloadProcessingException(Exception inner, string? errorText = null) 
            : base(errorText ?? inner.Message, inner) 
        { 
        }
    }
}
