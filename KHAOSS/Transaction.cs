using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    public class Transaction<T> where T : IEntity
    {
        private TaskCompletionSource taskCompletionSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        private T entity;
        private T[] entities;

        public T Entity { get => entity; internal set => entity = value; }
        public T[] Entities { get => entities; internal set => entities = value; }


        public Transaction(T entity)
        {
            Entity = entity;
        }

        public Transaction(T[] entities)
        {
            Entities = entities;
        }

        public bool IsSingleChange => this.Entity != null;

        public void Complete()
        {
            this.taskCompletionSource.SetResult();
        }

        public void SetError(Exception ex)
        {
            this.taskCompletionSource.SetException(ex);
        }

        public Task Task => taskCompletionSource.Task;

    }
}
