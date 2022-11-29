using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    public interface IDataStore<TBaseEntity> where TBaseEntity : class, IEntity
    {
        Task<TBaseEntity[]> Save(TBaseEntity[] entities);
        Task<T> Save<T>(T entity) where T : class, TBaseEntity;
        Task<T> Get<T>(string key) where T : class, TBaseEntity;
        Task<IEnumerable<T>> GetByPrefix<T>(string prefix, bool sortResults) where T : class, TBaseEntity;
    }
}
