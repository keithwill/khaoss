using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    public interface IMemoryStore<T> where T : IEntity
    {
        T Get(string key);
        IEnumerable<T> GetByPrefix(string prefix, bool sortResults);
        bool ProcessTransaction(Transaction<T> transaction);
        void LoadChange(string key, T document);
        long DeadSpace { get; }
        void IncrementDeadEntities();
        void RemoveAllDocuments();
        void ResetDeadSpace();
        public long EntityCount {get;}
        public long DeadEntityCount {get;}
    }
}
