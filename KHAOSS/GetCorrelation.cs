﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    public class GetCorrelation<T> where T : IEntity
    {
        
        private string key;

        private TaskCompletionSource<T> completionSource;
        public string Key => this.key;

        public GetCorrelation(string key)
        {
            this.key = key;
            completionSource = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public void SetResult(T document)
        {
            completionSource.SetResult(document);
        }

        public Task<T> Task => completionSource.Task;


        public void Reset()
        {
            this.key = null;
            this.completionSource = null;
        }

    }
}
