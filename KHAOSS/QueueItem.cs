using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    public sealed class QueueItem
    {
        public GetByPrefixCorrelation GetByPrefixCorrelation;
        public GetCorrelation GetCorrelation;
        public Transaction Transaction;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            GetByPrefixCorrelation = null;
            GetCorrelation = null;
            Transaction = null;
        }
    }
}
