using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    public class NaiveObjectPool<T> where T : class, new()
    {

        private readonly ConcurrentQueue<T> queue;

        public NaiveObjectPool()
        {
            this.queue = new ConcurrentQueue<T>();

        }

        public int PooledInstanceCount => this.queue.Count;

        public T Lease()
        {
            if (queue.TryDequeue(out var result))
            {
                return result;
            } else
            {
                return new T();
            }
        }

        public void Return(T value)
        {
            this.queue.Enqueue(value);
        }

    }
}
