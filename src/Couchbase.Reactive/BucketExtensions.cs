using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Couchbase.Core;
using System.Collections.Concurrent;
using Couchbase.N1QL;
using Couchbase.Views;

namespace Couchbase.Reactive
{
    public static class BucketExtensions
    {
        public static IObservable<IOperationResult<T>> GetObservable<T>(this IBucket bucket, string key)
        {
            return Observable.FromAsync(() => bucket.GetAsync<T>(key));
        }

        public static IObservable<KeyValuePair<string, IOperationResult<T>>> GetObservable<T>(this IBucket bucket, IList<string> keys)
        {
            if (bucket == null)
            {
                throw new ArgumentNullException("bucket");
            }

            Func<string, KeyValuePair<string, IOperationResult<T>>> getFunction =
                key => new KeyValuePair<string, IOperationResult<T>>(key, bucket.Get<T>(key));

            return new MultiGetObservable<string, KeyValuePair<string, IOperationResult<T>>>(keys, getFunction);
        }

        public static IObservable<IOperationResult<T>> GetAndTouchObservable<T>(this IBucket bucket, string key, TimeSpan expiration)
        {
            return Observable.FromAsync(() => bucket.GetAndTouchAsync<T>(key, expiration));
        }

        public static IObservable<IDocumentResult<T>> GetDocumentObservable<T>(this IBucket bucket, string id)
        {
            return Observable.FromAsync(() => bucket.GetDocumentAsync<T>(id));
        }

        public static IObservable<IDocumentResult<T>> GetAndTouchDocumentObservable<T>(this IBucket bucket, string id, TimeSpan expiration)
        {
            return Observable.FromAsync(() => bucket.GetAndTouchDocumentAsync<T>(id, expiration));
        }

        public static IObservable<KeyValuePair<string, IDocumentResult<T>>> GetDocumentObservable<T>(this IBucket bucket, IList<string> ids)
        {
            if (bucket == null)
            {
                throw new ArgumentNullException("bucket");
            }

            Func<string, KeyValuePair<string, IDocumentResult<T>>> getFunction =
                key => new KeyValuePair<string, IDocumentResult<T>>(key, bucket.GetDocument<T>(key));

            return new MultiGetObservable<string, KeyValuePair<string, IDocumentResult<T>>>(ids, getFunction);
        }

        public static IObservable<ViewRow<T>> QueryObservable<T>(this IBucket bucket, IViewQueryable query)
        {
            if (bucket == null)
            {
                throw new ArgumentNullException("bucket");
            }
            if (query == null)
            {
                throw new ArgumentNullException("query");
            }

            return new ViewQueryObservable<T>(bucket, query);
        }

        public static IObservable<T> QueryObservable<T>(this IBucket bucket, string query)
        {
            if (bucket == null)
            {
                throw new ArgumentNullException("bucket");
            }
            if (query == null)
            {
                throw new ArgumentNullException("query");
            }

            return bucket.QueryObservable<T>(new QueryRequest(query));
        }

        public static IObservable<T> QueryObservable<T>(this IBucket bucket, IQueryRequest query)
        {
            if (bucket == null)
            {
                throw new ArgumentNullException("bucket");
            }
            if (query == null)
            {
                throw new ArgumentNullException("query");
            }

            return new N1QlQueryObservable<T>(bucket, query);
        }
    }
}
