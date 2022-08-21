using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    public class GetCorrelation
    {
        
        private string key;

        private TaskCompletionSource<Document> completionSource;
        public string Key => this.key;

        public GetCorrelation(string key)
        {
            this.key = key;
            completionSource = new TaskCompletionSource<Document>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public void SetResult(Document document)
        {
            completionSource.SetResult(document);
        }

        public Task<Document> Task => completionSource.Task;


        public void Reset()
        {
            this.key = null;
            this.completionSource = null;
        }

    }
}
