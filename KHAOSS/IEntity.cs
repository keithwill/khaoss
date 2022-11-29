namespace KHAOSS;

public interface IEntity
{
    public string Key { get; init; }
    public int Version { get; init; }
    public bool Deleted { get; init; }

    public IEntity WithVersion(int version);

}