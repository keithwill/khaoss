namespace KHAOSS;

public class Entity<T>
{
    public string Key { get; set; }
    public KHAOSS.Document Document { get; set; }
    public int RetreiveVersion { get; set; }
    public T Value { get; set; }
}