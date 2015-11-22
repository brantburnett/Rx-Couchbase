using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Disposables;
using System.Text;
using System.Threading.Tasks;
using Couchbase.Core;

namespace Couchbase.Reactive
{
    internal class MultiGetObservable<TKey, TResult> : IObservable<TResult>
    {
        private readonly IList<TKey> _keys;
        private readonly Func<TKey, TResult> _getFunction;
        private readonly ParallelOptions _options;

        public MultiGetObservable(IList<TKey> keys, Func<TKey, TResult> getFunction, ParallelOptions options)
        {
            if (getFunction == null)
            {
                throw new ArgumentNullException("getFunction");
            }
            if (options == null)
            {
                throw new ArgumentNullException("options");
            }

            _keys = keys;
            _getFunction = getFunction;
            _options = options;
        }

        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            if ((_keys == null) || (_keys.Count == 0))
            {
                observer.OnCompleted();

                return Disposable.Empty;
            }

            var partitioner = Partitioner.Create(_keys, true);

            var partitions = partitioner.GetPartitions(_options.MaxDegreeOfParallelism);

            var isDisposed = false;

            var tasks = partitions.Select(keyEnumerator =>
            {
                return Task.Factory.StartNew(() =>
                {
                    try
                    {
                        while (keyEnumerator.MoveNext())
                        {
                            if (isDisposed)
                            {
                                // Observable was disposed, so stop sending events
                                break;
                            }

                            _options.CancellationToken.ThrowIfCancellationRequested();

                            var key = keyEnumerator.Current;
                            var result = _getFunction.Invoke(key);

                            observer.OnNext(result);
                        }
                    }
                    catch (Exception ex)
                    {
                        observer.OnError(ex);
                    }
                    finally
                    {
                        keyEnumerator.Dispose();
                    }
                }, _options.CancellationToken, TaskCreationOptions.None, _options.TaskScheduler);
            });

            Task.WhenAll(tasks).ContinueWith(t => observer.OnCompleted());

            return Disposable.Create(() => isDisposed = true);
        }
    }
}
