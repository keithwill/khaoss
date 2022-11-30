using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    internal class GetByPrefixCorrelation<T> where T : IEntity
    {
        private readonly string prefix;

        private TaskCompletionSource<IEnumerable<T>> completionSource;
        public string Prefix => this.prefix;
        private bool sortResults;
        public bool SortResults => sortResults;
        public GetByPrefixCorrelation(string prefix, bool sortResults)
        {
            this.prefix = prefix;
            this.sortResults = sortResults;
            completionSource = new TaskCompletionSource<IEnumerable<T>>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public void SetResult(IEnumerable<T> documents)
        {
            completionSource.SetResult(documents);
        }

        public Task<IEnumerable<T>> Task => completionSource.Task;

    }
}
