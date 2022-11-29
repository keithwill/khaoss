namespace KHAOSS;

public interface IEntity
{
    public string Key { get; }
    public int Version { get;  }
    public bool Deleted { get; }

    public IEntity WithVersion(int version);

}