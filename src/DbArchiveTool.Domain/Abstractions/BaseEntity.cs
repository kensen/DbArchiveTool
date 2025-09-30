namespace DbArchiveTool.Domain.Abstractions;

public abstract class BaseEntity
{
    public Guid Id { get; protected set; } = Guid.NewGuid();
    public DateTime CreatedAtUtc { get; protected set; } = DateTime.UtcNow;
    public string CreatedBy { get; protected set; } = "SYSTEM";
    public DateTime UpdatedAtUtc { get; protected set; } = DateTime.UtcNow;
    public string UpdatedBy { get; protected set; } = "SYSTEM";
    public bool IsDeleted { get; protected set; }
    public void MarkDeleted(string user)
    {
        IsDeleted = true;
        UpdatedBy = user;
        UpdatedAtUtc = DateTime.UtcNow;
    }

    public void Touch(string user)
    {
        UpdatedBy = user;
        UpdatedAtUtc = DateTime.UtcNow;
    }
}
