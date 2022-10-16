namespace KHAOSS
{
    public interface IRepository
    {
        Task ForceSave<T>(string key, T entity);
        Task<Entity<T>> Get<T>(string key);
        IAsyncEnumerable<Entity<T>> GetByPrefix<T>(string prefix, bool sortResults);
        IAsyncEnumerable<T> GetByPrefixUntracked<T>(string prefix, bool sortResults);
        Task Save<T>(string key, Entity<T> entity);
    }
}