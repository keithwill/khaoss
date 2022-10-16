namespace KHAOSS
{
    public interface IEntitySerializer
    {
        byte[] Serialize<T>(T entity);
        T Deserialize<T>(byte[] data);
    }
}