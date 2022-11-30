using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    internal sealed class QueueItem<T> where T : IEntity
    {
        public GetByPrefixCorrelation<T> GetByPrefixCorrelation;
        public GetCorrelation<T> GetCorrelation;
        public Transaction<T> Transaction;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            GetByPrefixCorrelation = null;
            GetCorrelation = null;
            Transaction = null;
        }
    }
}
