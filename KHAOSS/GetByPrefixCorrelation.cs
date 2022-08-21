using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    public class GetByPrefixCorrelation
    {
        private readonly string prefix;

        private TaskCompletionSource<IEnumerable<KeyValuePair<string, Document>>> completionSource;
        public string Prefix => this.prefix;
        private bool sortResults;
        public bool SortResults => sortResults;
        public GetByPrefixCorrelation(string prefix, bool sortResults)
        {
            this.prefix = prefix;
            this.sortResults = sortResults;
            completionSource = new TaskCompletionSource<IEnumerable<KeyValuePair<string, Document>>>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public void SetResult(IEnumerable<KeyValuePair<string, Document>> documents)
        {
            completionSource.SetResult(documents);
        }

        public Task<IEnumerable<KeyValuePair<string, Document>>> Task => completionSource.Task;

    }
}
