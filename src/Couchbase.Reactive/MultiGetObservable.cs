using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Disposables;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Couchbase.Reactive
{
    internal class MultiGetObservable<TKey, TResult> : IObservable<TResult>
    {
        private readonly IList<TKey> _keys;
        private readonly Func<TKey, TResult> _getFunction;

        public MultiGetObservable(IList<TKey> keys, Func<TKey, TResult> getFunction)
        {
            if (getFunction == null)
            {
                throw new ArgumentNullException("getFunction");
            }

            _keys = keys;
            _getFunction = getFunction;
        }

        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            if ((_keys == null) || (_keys.Count == 0))
            {
                observer.OnCompleted();

                return Disposable.Empty;
            }

            var cancellationTokenSource = new CancellationTokenSource();

            Task.Factory.StartNew(() =>
            {
                Parallel.ForEach(_keys, (key, loopState) =>
                {
                    try
                    {
                        if (!cancellationTokenSource.IsCancellationRequested)
                        {
                            var result = _getFunction.Invoke(key);

                            if (!cancellationTokenSource.IsCancellationRequested)
                            {
                                observer.OnNext(result);
                            }
                            else
                            {
                                loopState.Break();
                            }
                        }
                        else
                        {
                            loopState.Break();
                        }
                    }
                    catch (Exception ex)
                    {
                        // First exception should stop processing
                        cancellationTokenSource.Cancel();
                        loopState.Break();

                        observer.OnError(ex);
                    }
                });

                if (!cancellationTokenSource.IsCancellationRequested)
                {
                    observer.OnCompleted();
                }
            }, cancellationTokenSource.Token);

            // When result is disposed, cancel processing
            return Disposable.Create(() => cancellationTokenSource.Cancel());
        }
    }
}
