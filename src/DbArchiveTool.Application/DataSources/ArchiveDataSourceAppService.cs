using DbArchiveTool.Domain.DataSources;
using DbArchiveTool.Shared.Results;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System.Linq;

namespace DbArchiveTool.Application.DataSources;

/// <summary>归档数据源应用服务。</summary>
internal sealed class ArchiveDataSourceAppService : IArchiveDataSourceAppService
{
    private readonly IDataSourceRepository _repository;
    private readonly ILogger<ArchiveDataSourceAppService> _logger;

    public ArchiveDataSourceAppService(IDataSourceRepository repository, ILogger<ArchiveDataSourceAppService> logger)
    {
        _repository = repository;
        _logger = logger;
    }

    public async Task<Result<IReadOnlyList<ArchiveDataSourceDto>>> GetAsync(CancellationToken cancellationToken = default)
    {
        var items = await _repository.ListAsync(cancellationToken);
        var dtos = items.Select(MapToDto).ToList();
        return Result<IReadOnlyList<ArchiveDataSourceDto>>.Success(dtos);
    }

    public async Task<Result<Guid>> CreateAsync(CreateArchiveDataSourceRequest request, CancellationToken cancellationToken = default)
    {
        var validationError = ValidateCreateRequest(request);
        if (validationError is not null)
        {
            return Result<Guid>.Failure(validationError);
        }

        var existing = await _repository.ListAsync(cancellationToken);
        if (existing.Any(x => string.Equals(x.Name, request.Name, StringComparison.OrdinalIgnoreCase)))
        {
            return Result<Guid>.Failure("数据源名称已存在,请更换名称");
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

    public async Task<Result<bool>> TestConnectionAsync(TestArchiveDataSourceRequest request, CancellationToken cancellationToken = default)
    {
        var builder = BuildSqlConnectionString(request.ServerAddress, request.ServerPort, request.DatabaseName, request.UseIntegratedSecurity, request.UserName, request.Password);

        try
        {
            await using var connection = new SqlConnection(builder.ConnectionString);
            await connection.OpenAsync(cancellationToken);
            await connection.CloseAsync();
            return Result<bool>.Success(true);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "测试数据源连接失败: {Server}/{Database}", request.ServerAddress, request.DatabaseName);
            return Result<bool>.Failure($"连接失败: {ex.Message}");
        }
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

    private static string? ValidateCreateRequest(CreateArchiveDataSourceRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.Name))
        {
            return "数据源名称不能为空";
        }

        if (string.IsNullOrWhiteSpace(request.ServerAddress))
        {
            return "服务器地址不能为空";
        }

        if (request.ServerPort <= 0 || request.ServerPort > 65535)
        {
            return "端口号必须在 1-65535 之间";
        }

        if (string.IsNullOrWhiteSpace(request.DatabaseName))
        {
            return "数据库名称不能为空";
        }

        if (!request.UseIntegratedSecurity && string.IsNullOrWhiteSpace(request.UserName))
        {
            return "使用 SQL 身份验证时必须填写用户名";
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
