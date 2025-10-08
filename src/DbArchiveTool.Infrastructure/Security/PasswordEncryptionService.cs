using System;
using System.Text;
using DbArchiveTool.Application.Abstractions;
using Microsoft.AspNetCore.DataProtection;

namespace DbArchiveTool.Infrastructure.Security;

/// <summary>
/// 基于 ASP.NET Core Data Protection API 的密码加密服务
/// </summary>
public sealed class PasswordEncryptionService : IPasswordEncryptionService
{
    private readonly IDataProtector _protector;
    private const string EncryptedPrefix = "ENCRYPTED:";

    public PasswordEncryptionService(IDataProtectionProvider dataProtectionProvider)
    {
        // 创建专用于密码加密的 Protector
        _protector = dataProtectionProvider.CreateProtector("DbArchiveTool.PasswordProtection");
    }

    /// <summary>
    /// 加密密码
    /// </summary>
    public string Encrypt(string plainPassword)
    {
        if (string.IsNullOrEmpty(plainPassword))
        {
            return plainPassword;
        }

        // 如果已经加密过，直接返回
        if (IsEncrypted(plainPassword))
        {
            return plainPassword;
        }

        try
        {
            var encrypted = _protector.Protect(plainPassword);
            return $"{EncryptedPrefix}{encrypted}";
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"密码加密失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 解密密码
    /// </summary>
    public string Decrypt(string encryptedPassword)
    {
        if (string.IsNullOrEmpty(encryptedPassword))
        {
            return encryptedPassword;
        }

        // 如果没有加密前缀，说明是明文密码（兼容旧数据）
        if (!IsEncrypted(encryptedPassword))
        {
            return encryptedPassword;
        }

        try
        {
            // 移除前缀
            var cipherText = encryptedPassword.Substring(EncryptedPrefix.Length);
            return _protector.Unprotect(cipherText);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"密码解密失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 判断是否已加密
    /// </summary>
    public bool IsEncrypted(string password)
    {
        return !string.IsNullOrEmpty(password) && password.StartsWith(EncryptedPrefix);
    }
}
