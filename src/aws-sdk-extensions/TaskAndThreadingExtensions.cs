using System;
using System.Threading;
using System.Threading.Tasks;

namespace aws_sdk_extensions
{
    public static class TaskAndThreadingExtensions
    {
        /// <summary>
        /// Enables non-blocking wait on a threading handle.
        /// </summary>
        /// <remarks>
        /// From https://stackoverflow.com/a/49428829/516508
        /// </remarks>
        /// <param name="waitHandle"></param>
        /// <param name="timeoutMillisec"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static Task<bool> WaitOneAsync(this WaitHandle waitHandle, int timeoutMillisec)
        {
            if (waitHandle == null)
                throw new ArgumentNullException(nameof(waitHandle));

            var tcs = new TaskCompletionSource<bool>();

            RegisteredWaitHandle registeredWaitHandle = ThreadPool.RegisterWaitForSingleObject(
                waitHandle,
                callBack: (state, timedOut) => { tcs.TrySetResult(!timedOut); },
                state: null,
                millisecondsTimeOutInterval: timeoutMillisec,
                executeOnlyOnce: true);

            return tcs.Task.ContinueWith((antecedent) =>
            {
                registeredWaitHandle.Unregister(waitObject: null);
                try
                {
                    return antecedent.Result;
                }
                catch
                {
                    return false;
                }
            });
        }
    }
}
