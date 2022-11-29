using System.Threading;
using System.Threading.Tasks;

namespace KHAOSS
{
    public interface IDataEngine<TEntity> where TEntity : class, IEntity
    {
        Task StartAsync(CancellationToken cancellationToken);
        Task StopAsync(CancellationToken cancellationToken);

        void RemoveAllDocuments();

        long EntityCount { get; }
        long DeadEntityCount { get; }
        double DeadSpacePercentage { get; }

        Task ForceMaintenance();
    }
}