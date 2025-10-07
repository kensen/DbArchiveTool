using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.DataSources;
using DbArchiveTool.Shared.DataSources;
using DbArchiveTool.Shared.Results;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Application.DataSources;

/// <summary>归档数据源应用服务。</summary>
internal sealed class ArchiveDataSourceAppService : IArchiveDataSourceAppService
{
    private readonly IDataSourceRepository _repository;
    private readonly IArchiveConnectionTester _connectionTester;
    private readonly ILogger<ArchiveDataSourceAppService> _logger;

    public ArchiveDataSourceAppService(
        IDataSourceRepository repository,
        IArchiveConnectionTester connectionTester,
        ILogger<ArchiveDataSourceAppService> logger)
    {
        _repository = repository;
        _connectionTester = connectionTester;
        _logger = logger;
    }

    public async Task<Result<IReadOnlyList<ArchiveDataSourceDto>>> GetAsync(CancellationToken cancellationToken = default)
    {
        var items = await _repository.ListAsync(cancellationToken);
        var dtos = items.Select(MapToDto).ToList();
        return Result<IReadOnlyList<ArchiveDataSourceDto>>.Success(dtos);
    }

    public async Task<Result<ArchiveDataSourceDto>> GetByIdAsync(Guid id, CancellationToken cancellationToken = default)
    {
        if (id == Guid.Empty)
        {
            return Result<ArchiveDataSourceDto>.Failure("数据源ID无效");
        }

        var entity = await _repository.GetAsync(id, cancellationToken);
        if (entity is null)
        {
            return Result<ArchiveDataSourceDto>.Failure("未找到指定的数据源");
        }

        return Result<ArchiveDataSourceDto>.Success(MapToDto(entity));
    }

    public async Task<Result<Guid>> CreateAsync(CreateArchiveDataSourceRequest request, CancellationToken cancellationToken = default)
    {
        var validationError = ValidatePayload(
            request.Name,
            request.ServerAddress,
            request.ServerPort,
            request.DatabaseName,
            request.UseIntegratedSecurity,
            request.UserName);

        if (validationError is not null)
        {
            return Result<Guid>.Failure(validationError);
        }

        var existing = await _repository.ListAsync(cancellationToken);
        if (existing.Any(x => string.Equals(x.Name, request.Name, StringComparison.OrdinalIgnoreCase)))
        {
            return Result<Guid>.Failure("数据源名称已存在,请避免重复");
        }

        var entity = new ArchiveDataSource(
            request.Name,
            request.Description,
            request.ServerAddress,
            request.ServerPort,
            request.DatabaseName,
            request.UseIntegratedSecurity,
            request.UserName,
            request.Password);

        await _repository.AddAsync(entity, cancellationToken);
        await _repository.SaveChangesAsync(cancellationToken);

        return Result<Guid>.Success(entity.Id);
    }

    public async Task<Result<bool>> UpdateAsync(UpdateArchiveDataSourceRequest request, CancellationToken cancellationToken = default)
    {
        if (request.Id == Guid.Empty)
        {
            return Result<bool>.Failure("缺少数据源标识");
        }

        var validationError = ValidatePayload(
            request.Name,
            request.ServerAddress,
            request.ServerPort,
            request.DatabaseName,
            request.UseIntegratedSecurity,
            request.UserName);

        if (validationError is not null)
        {
            return Result<bool>.Failure(validationError);
        }

        var entity = await _repository.GetAsync(request.Id, cancellationToken);
        if (entity is null)
        {
            return Result<bool>.Failure("数据源不存在或已删除");
        }

        var existing = await _repository.ListAsync(cancellationToken);
        if (existing.Any(x => x.Id != request.Id && string.Equals(x.Name, request.Name, StringComparison.OrdinalIgnoreCase)))
        {
            return Result<bool>.Failure("数据源名称已存在,请更换");
        }

        var password = request.UseIntegratedSecurity
            ? null
            : (string.IsNullOrWhiteSpace(request.Password) ? entity.Password : request.Password);

        entity.Update(
            request.Name,
            request.Description,
            request.ServerAddress,
            request.ServerPort,
            request.DatabaseName,
            request.UseIntegratedSecurity,
            request.UserName,
            password,
            request.OperatorName ?? "SYSTEM");

        await _repository.SaveChangesAsync(cancellationToken);

        return Result<bool>.Success(true);
    }

    public async Task<Result<bool>> TestConnectionAsync(TestArchiveDataSourceRequest request, CancellationToken cancellationToken = default)
    {
        var builder = BuildSqlConnectionString(
            request.ServerAddress,
            request.ServerPort,
            request.DatabaseName,
            request.UseIntegratedSecurity,
            request.UserName,
            request.Password);

        var result = await _connectionTester.TestConnectionAsync(builder.ConnectionString, cancellationToken);
        if (!result.IsSuccess)
        {
            _logger.LogWarning("Dapper 测试连接失败: {Server}/{Database} - {Message}", request.ServerAddress, request.DatabaseName, result.Error);
            return Result<bool>.Failure(result.Error ?? "连接失败");
        }

        return Result<bool>.Success(true);
    }

    private static ArchiveDataSourceDto MapToDto(ArchiveDataSource entity)
    {
        var displayConnection = entity.UseIntegratedSecurity
            ? $"{entity.ServerAddress}:{entity.ServerPort}\\{entity.DatabaseName} (Windows)"
            : $"{entity.ServerAddress}:{entity.ServerPort}\\{entity.DatabaseName} (SQL: {entity.UserName})";

        return new ArchiveDataSourceDto
        {
            Id = entity.Id,
            Name = entity.Name,
            Description = entity.Description,
            ServerAddress = entity.ServerAddress,
            ServerPort = entity.ServerPort,
            DatabaseName = entity.DatabaseName,
            UseIntegratedSecurity = entity.UseIntegratedSecurity,
            UserName = entity.UserName,
            IsEnabled = entity.IsEnabled,
            DisplayConnection = displayConnection
        };
    }

    private static string? ValidatePayload(
        string name,
        string serverAddress,
        int serverPort,
        string databaseName,
        bool useIntegratedSecurity,
        string? userName)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return "数据源名称不能为空";
        }

        if (string.IsNullOrWhiteSpace(serverAddress))
        {
            return "服务器地址不能为空";
        }

        if (serverPort <= 0 || serverPort > 65535)
        {
            return "端口号必须在 1-65535 之间";
        }

        if (string.IsNullOrWhiteSpace(databaseName))
        {
            return "数据库名称不能为空";
        }

        if (!useIntegratedSecurity && string.IsNullOrWhiteSpace(userName))
        {
            return "使用 SQL 登录时请填写用户名";
        }

        return null;
    }

    private static SqlConnectionStringBuilder BuildSqlConnectionString(
        string serverAddress,
        int serverPort,
        string databaseName,
        bool useIntegratedSecurity,
        string? userName,
        string? password)
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = serverPort == 1433 ? serverAddress : $"{serverAddress},{serverPort}",
            InitialCatalog = databaseName,
            IntegratedSecurity = useIntegratedSecurity,
            TrustServerCertificate = true,
            ConnectTimeout = 5
        };

        if (!useIntegratedSecurity)
        {
            builder.UserID = userName ?? string.Empty;
            builder.Password = password ?? string.Empty;
        }

        return builder;
    }
}
