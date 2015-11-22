using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Couchbase.Reactive.IntegrationTests.Documents;
using NUnit.Framework;

namespace Couchbase.Reactive.IntegrationTests
{
    [TestFixture]
    public class BucketExtensionsTests
    {
        [Test]
        public void GetObject()
        {
            using (var cluster = new Cluster())
            {
                using (var bucket = cluster.OpenBucket("beer-sample"))
                {
                    var observable = bucket.GetObservable<Beer>("21st_amendment_brewery_cafe-21a_ipa");

                    observable.ForEachAsync(p => Console.WriteLine(p.Value.Name)).Wait();
                }
            }
        }

        [Test]
        public void GetDocument()
        {
            using (var cluster = new Cluster())
            {
                using (var bucket = cluster.OpenBucket("beer-sample"))
                {
                    var observable = bucket.GetDocumentObservable<Beer>("21st_amendment_brewery_cafe-21a_ipa");

                    observable.ForEachAsync(p => Console.WriteLine(p.Document.Content.Name)).Wait();
                }
            }
        }

        [Test]
        public void MultiGetObject()
        {
            using (var cluster = new Cluster())
            {
                using (var bucket = cluster.OpenBucket("beer-sample"))
                {
                    var keys = new[]
                    {
                        "21st_amendment_brewery_cafe-21a_ipa",
                        "21st_amendment_brewery_cafe-563_stout",
                        "21st_amendment_brewery_cafe-amendment_pale_ale"
                    };

                    var observable = bucket.GetObservable<Beer>(keys);

                    observable.ForEachAsync(p => Console.WriteLine("{0} - {1}", p.Key, p.Value.Value.Name)).Wait();
                }
            }
        }

        [Test]
        public void MultiGetDocument()
        {
            using (var cluster = new Cluster())
            {
                using (var bucket = cluster.OpenBucket("beer-sample"))
                {
                    var keys = new[]
                    {
                        "21st_amendment_brewery_cafe-21a_ipa",
                        "21st_amendment_brewery_cafe-563_stout",
                        "21st_amendment_brewery_cafe-amendment_pale_ale"
                    };

                    var observable = bucket.GetDocumentObservable<Beer>(keys);

                    observable.ForEachAsync(p => Console.WriteLine("{0} - {1}", p.Key, p.Value.Document.Content.Name)).Wait();
                }
            }
        }

        [Test]
        public void QueryView()
        {
            using (var cluster = new Cluster())
            {
                using (var bucket = cluster.OpenBucket("beer-sample"))
                {
                    var query = bucket.CreateQuery("beer", "brewery_beers").Limit(10);

                    var observable = bucket.QueryObservable<Beer>(query);

                    var lockObj = new object();
                    observable.ForEachAsync(p =>
                    {
                        lock (lockObj)
                        {
                            Console.WriteLine(p.Key);
                            Console.WriteLine(p.Id);
                        }
                    }).Wait();
                }
            }
        }

        [Test]
        public void QueryView_ExpectError()
        {
            using (var cluster = new Cluster())
            {
                using (var bucket = cluster.OpenBucket("beer-sample"))
                {
                    var query = bucket.CreateQuery("beer", "brewery_beers_fake_view").Limit(10);

                    var observable = bucket.QueryObservable<Beer>(query);

                    var lockObj = new object();
                    Exception gotException = null;

                    lock (lockObj)
                    {
                        observable.Subscribe(
                            p => // OnNext
                            {
                                lock (lockObj)
                                {
                                    Console.WriteLine(p.Key);
                                    Console.WriteLine(p.Id);
                                }
                            },
                            ex => // OnError
                            {
                                lock (lockObj)
                                {
                                    gotException = ex;

                                    Monitor.Pulse(lockObj);
                                }
                            },
                            () => // OnComplete
                            {
                                lock (lockObj)
                                {
                                    Monitor.Pulse(lockObj);
                                }
                            });

                        // Wait for a pulse which indicates that the observable is complete
                        Monitor.Wait(lockObj);
                    }

                    Assert.IsInstanceOf<CouchbaseViewQueryException>(gotException);
                    Console.WriteLine(gotException.Message);
                }
            }
        }

        [Test]
        public void QueryN1Ql()
        {
            using (var cluster = new Cluster())
            {
                using (var bucket = cluster.OpenBucket("beer-sample"))
                {
                    var observable = bucket.QueryObservable<Beer>("SELECT `beer-sample`.* FROM `beer-sample` WHERE type = 'beer' LIMIT 10");

                    var lockObj = new object();
                    observable.ForEachAsync(p =>
                    {
                        lock (lockObj)
                        {
                            Console.WriteLine(p.Name);
                        }
                    }).Wait();
                }
            }
        }

        [Test]
        public void QueryN1Ql_ExpectError()
        {
            using (var cluster = new Cluster())
            {
                using (var bucket = cluster.OpenBucket("beer-sample"))
                {
                    var query = bucket.CreateQuery("beer", "brewery_beers_fake_view").Limit(10);

                    var observable = bucket.QueryObservable<Beer>("SELECT `beer-sample`.* FROM `beer - sample` WHERE type = 'beer' LIMIT 10 THIS IS BAD SYNTAX");

                    var lockObj = new object();
                    Exception gotException = null;

                    lock (lockObj)
                    {
                        observable.Subscribe(
                            p => // OnNext
                            {
                                lock (lockObj)
                                {
                                    Console.WriteLine(p.Name);
                                }
                            },
                            ex => // OnError
                            {
                                lock (lockObj)
                                {
                                    gotException = ex;

                                    Monitor.Pulse(lockObj);
                                }
                            },
                            () => // OnComplete
                            {
                                lock (lockObj)
                                {
                                    Monitor.Pulse(lockObj);
                                }
                            });

                        // Wait for a pulse which indicates that the observable is complete
                        Monitor.Wait(lockObj);
                    }

                    Assert.IsInstanceOf<CouchbaseN1QlQueryException>(gotException);
                    Console.WriteLine(gotException.Message);
                }
            }
        }
    }
}
