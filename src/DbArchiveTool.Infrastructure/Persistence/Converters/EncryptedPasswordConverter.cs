using System;
using DbArchiveTool.Application.Abstractions;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace DbArchiveTool.Infrastructure.Persistence.Converters;

/// <summary>
/// 密码加密转换器，用于EF Core自动加密/解密密码字段
/// </summary>
public sealed class EncryptedPasswordConverter : ValueConverter<string?, string?>
{
    public EncryptedPasswordConverter(IPasswordEncryptionService encryptionService)
        : base(
            // 保存到数据库时加密
            plainPassword => encryptionService.Encrypt(plainPassword ?? string.Empty),
            // 从数据库读取时解密
            encryptedPassword => encryptionService.Decrypt(encryptedPassword ?? string.Empty)
        )
    {
    }
}
