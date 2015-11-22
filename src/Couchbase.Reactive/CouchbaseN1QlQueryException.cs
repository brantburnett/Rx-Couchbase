using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Couchbase.N1QL;

namespace Couchbase.Reactive
{
    public class CouchbaseN1QlQueryException : ApplicationException
    {
        private readonly QueryStatus _status;
        public QueryStatus Status
        {
            get { return _status; }
        }

        private readonly IList<Error> _errors;
        public IList<Error> Errors
        {
            get { return _errors; }
        }
        internal CouchbaseN1QlQueryException(QueryStatus status, IList<Error> errors)
            : base(errors != null && errors.Count == 1 ?
                       errors.First().Message :
                       "N1QL query error, see Status and Errors properties for details.")
        {
            _status = status;
            _errors = errors;
        }
    }
}
