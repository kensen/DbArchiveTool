using System;

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

    public void InitializeAudit(string user)
    {
        if (string.IsNullOrWhiteSpace(user))
        {
            throw new ArgumentException("操作人不能为空。", nameof(user));
        }

        var normalized = user.Trim();
        CreatedBy = normalized;
        UpdatedBy = normalized;
        CreatedAtUtc = DateTime.UtcNow;
        UpdatedAtUtc = CreatedAtUtc;
    }

    public void OverrideId(Guid id)
    {
        if (id == Guid.Empty)
        {
            throw new ArgumentException("标识不能为空。", nameof(id));
        }

        Id = id;
    }

    public void RestoreAudit(DateTime createdAtUtc, string createdBy, DateTime updatedAtUtc, string updatedBy, bool isDeleted)
    {
        CreatedAtUtc = createdAtUtc;
        CreatedBy = createdBy;
        UpdatedAtUtc = updatedAtUtc;
        UpdatedBy = updatedBy;
        IsDeleted = isDeleted;
    }
}
