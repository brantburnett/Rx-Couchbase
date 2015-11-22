using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Disposables;
using System.Text;
using System.Threading.Tasks;
using Couchbase.Core;
using Couchbase.Views;

namespace Couchbase.Reactive
{
    internal class ViewQueryObservable<T> : IObservable<ViewRow<T>>
    {
        private readonly IBucket _bucket;
        private readonly IViewQueryable _query;

        public ViewQueryObservable(IBucket bucket, IViewQueryable query)
        {
            if (bucket == null)
            {
                throw new ArgumentNullException("bucket");
            }
            if (query == null)
            {
                throw new ArgumentNullException("query");
            }

            _bucket = bucket;
            _query = query;
        }

        public IDisposable Subscribe(IObserver<ViewRow<T>> observer)
        {
            var disposed = false;

            var task = _bucket.QueryAsync<T>(_query);

            // Pass exceptions to the observer as an error
            task.ContinueWith(t =>
            {
                if (!disposed)
                {
                    observer.OnError((Exception) t.Exception ?? new InvalidOperationException("Unknown Error"));
                }
            }, TaskContinuationOptions.OnlyOnFaulted);

            // Pass canceled tasks as completion to the observer
            task.ContinueWith(t =>
            {
                if (!disposed)
                {
                    observer.OnCompleted();
                }
            }, TaskContinuationOptions.OnlyOnCanceled);

            // Handle successful task completion
            task.ContinueWith(t =>
            {
                if (!disposed)
                {
                    if (t.Result.Success)
                    {
                        // On success, deliver all rows to the observer

                        foreach (var row in t.Result.Rows)
                        {
                            observer.OnNext(row);
                        }

                        observer.OnCompleted();
                    }
                    else
                    {
                        // On view error, deliver as an error to the observer
                        if (t.Result.Exception != null)
                        {
                            observer.OnError(t.Result.Exception);
                        }
                        else
                        {
                            var ex = new CouchbaseViewQueryException(t.Result.StatusCode, t.Result.Error);

                            observer.OnError(ex);
                        }
                    }
                }
            }, TaskContinuationOptions.OnlyOnRanToCompletion);

            return Disposable.Create(() => disposed = true);
        }
    }
}
