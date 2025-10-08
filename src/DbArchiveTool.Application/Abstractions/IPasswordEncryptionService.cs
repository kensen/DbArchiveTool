namespace DbArchiveTool.Application.Abstractions;

/// <summary>
/// 密码加密服务接口
/// </summary>
public interface IPasswordEncryptionService
{
    /// <summary>
    /// 加密密码
    /// </summary>
    /// <param name="plainPassword">明文密码</param>
    /// <returns>加密后的密码</returns>
    string Encrypt(string plainPassword);

    /// <summary>
    /// 解密密码
    /// </summary>
    /// <param name="encryptedPassword">加密的密码</param>
    /// <returns>明文密码</returns>
    string Decrypt(string encryptedPassword);

    /// <summary>
    /// 判断是否已加密
    /// </summary>
    /// <param name="password">密码字符串</param>
    /// <returns>true 表示已加密</returns>
    bool IsEncrypted(string password);
}
