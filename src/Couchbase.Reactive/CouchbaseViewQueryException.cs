using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Couchbase.Reactive
{
    public class CouchbaseViewQueryException : ApplicationException
    {
        private readonly HttpStatusCode _statusCode;
        public HttpStatusCode StatusCode
        {
            get { return _statusCode; }
        }

        private readonly string _error;
        public string Error
        {
            get { return _error; }
        }
        internal CouchbaseViewQueryException(HttpStatusCode statusCode, string error)
            : base(error)
        {
            _statusCode = statusCode;
            _error = error;
        }
    }
}
