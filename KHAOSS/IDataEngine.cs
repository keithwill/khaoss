using System.Threading;
using System.Threading.Tasks;

namespace KHAOSS
{
    public interface IDataEngine
    {
        Task StartAsync(CancellationToken cancellationToken);
        Task StopAsync(CancellationToken cancellationToken);

        void RemoveAllDocuments();

        long DeadSpace { get; }

        Task ForceMaintenance();
    }
}