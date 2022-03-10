using Microsoft.Extensions.DependencyInjection;

namespace SqsPriorityQueue
{
    public static class Extensions
    {
        /// <summary>
        /// Register priority queue processors. Once registered, processor collection can be obtained
        /// by DI-resolving either IPriorityQueueProcessor subclasses, or (if isTheOnlyProcessorType is
        /// set to true) by resolving IPriorityQueueProcessor and IEnumerable<IPriorityQueueProcessor>.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="services"></param>
        /// <param name="processorCountFactory">Function that returns number processors of a given processor type.</param>
        /// <param name="isTheOnlyProcessorType">If set to true, the processor and its collection can also be resolved 
        /// via resolving <see cref="IPriorityQueueProcessor"/> and <see cref="IEnumerable{IPriorityQueueProcessor}"/>.
        /// If registering multiple processor types, set this value to false for all processor types.
        /// </param>
        public static void RegisterProcessors<T>(this IServiceCollection services, bool isTheOnlyProcessorType, Func<IServiceProvider, Type, int> processorCountFactory)
            where T: class, IPriorityQueueProcessor
        {
            services.AddTransient<T>();
            services.AddTransient<IEnumerable<T>>(ioc => ProcessorCollectionFactory<T>(ioc, processorCountFactory(ioc, typeof(T))));

            if (isTheOnlyProcessorType)
            {
                services.AddTransient<IPriorityQueueProcessor>(ioc => ioc.GetRequiredService<T>());
                services.AddTransient<IEnumerable<IPriorityQueueProcessor>>(ioc => ioc.GetRequiredService<IEnumerable<T>>().Cast<IPriorityQueueProcessor>());
            }
        }

        /// <summary>
        /// Instantiates and initializes processor collection using DI
        /// </summary>
        /// <param name="iocContainer"></param>
        /// <returns></returns>
        private static IEnumerable<T> ProcessorCollectionFactory<T>(IServiceProvider iocContainer, int processorCount)
             where T : class, IPriorityQueueProcessor
        {
            for (int i = 0; i < processorCount; i++)
            {
                var processor = iocContainer.GetRequiredService<T>();
                processor.ListenerId = i.ToString();
                yield return processor;
            }
        }
    }
}
