using AntDesign;
using DbArchiveTool.Application.DataSources;
using DbArchiveTool.Application.Partitions;
using DbArchiveTool.Shared.Archive;
using DbArchiveTool.Web.Core;
using DbArchiveTool.Web.Services;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.Logging;
using Microsoft.JSInterop;
using System.Linq;
using System.Text;

namespace DbArchiveTool.Web.Components;

/// <summary>
/// 批量执行结果
/// </summary>
public sealed class BatchExecutionResult
{
	public BatchExecutionResult(string partitionKey, bool success, string message, Guid? taskId)
	{
		PartitionKey = partitionKey;
		Success = success;
		Message = message;
		TaskId = taskId;
	}
	
	public string PartitionKey { get; }
	public bool Success { get; }
	public string Message { get; }
	public Guid? TaskId { get; }
}

/// <summary>分区归档向导组件，负责引导管理员完成分区切换归档。</summary>
public sealed partial class PartitionArchiveWizard : ComponentBase
{
	[Parameter] public Guid DataSourceId { get; set; }
	[Parameter] public string SchemaName { get; set; } = string.Empty;
	[Parameter] public string TableName { get; set; } = string.Empty;
	[Parameter] public string? PreSelectedPartitionKey { get; set; }
	[Parameter] public bool Visible { get; set; }
	[Parameter] public EventCallback<bool> VisibleChanged { get; set; }
	[Parameter] public EventCallback<Guid> OnSuccess { get; set; }
	[Parameter] public string Title { get; set; } = "分区归档向导";

	[Inject] private PartitionConfigurationApiClient PartitionConfigApi { get; set; } = default!;
	[Inject] private PartitionArchiveApiClient PartitionArchiveApi { get; set; } = default!;
	[Inject] private ArchiveDataSourceApiClient DataSourceApi { get; set; } = default!;
	[Inject] private ArchiveConfigurationApiClient ArchiveConfigApi { get; set; } = default!;
	[Inject] private MessageService Message { get; set; } = default!;
	[Inject] private AdminSessionState AdminSession { get; set; } = default!;
	[Inject] private ILogger<PartitionArchiveWizard> Logger { get; set; } = default!;
	[Inject] private IJSRuntime JSRuntime { get; set; } = default!;

	private readonly SwitchArchiveFormModel _form = new();
	private readonly List<PartitionConfigurationOption> _configurationOptions = new();
	private PartitionConfigurationOption? _selectedConfiguration;
	private PartitionConfigurationDetailModel? _selectedConfigurationDetail;
	
	// 归档配置相关字段
	private ArchiveConfigurationDetailModel? _loadedArchiveConfig;
	private ArchiveDataSourceDto? _currentDataSource;
	private bool _loadingArchiveConfig;
	private bool _initializing;
	private bool _loadingConfigurations;
	private bool _inspectionLoading;
	private bool _autoFixExecuting;
	private bool _submitting;
	private bool _wasVisible;
	private int _currentStep;
	private ArchiveMode _selectedMode = ArchiveMode.Switch;
	private PartitionSwitchInspectionResultDto? _inspectionResult;
	private ArchiveInspectionResultDto? _bcpInspectionResult;
	private ArchiveInspectionResultDto? _bulkCopyInspectionResult;
	private PartitionSwitchAutoFixResultDto? _autoFixResult;
	private readonly List<AutoFixOptionState> _autoFixOptions = new();
	private string? _executionPreviewMarkdown;
	private Guid? _lastSubmittedTaskId;
	
	// 批量执行相关字段
	private List<string> _partitionKeys = new();
	private bool _isBatchMode;
	private int _batchCurrentIndex;
	private int _batchSuccessCount;
	private int _batchFailureCount;
	private readonly List<BatchExecutionResult> _batchResults = new();

	private Guid? SelectedConfigurationId
	{
		get => _form.PartitionConfigurationId;
		set
		{
			if (_form.PartitionConfigurationId == value)
			{
				return;
			}

			_form.PartitionConfigurationId = value;

			if (!_initializing && !_loadingConfigurations)
			{
				if (value.HasValue)
				{
					var option = _configurationOptions.FirstOrDefault(o => o.Id == value.Value);
					if (option is not null)
					{
						_ = InvokeAsync(() => ApplyConfigurationAsync(option));
					}
				}
				else
				{
					_selectedConfiguration = null;
					_selectedConfigurationDetail = null;
				}
			}
		}
	}

	protected override async Task OnParametersSetAsync()
	{
		if (Visible && !_wasVisible)
		{
			await InitializeAsync();
		}

		_wasVisible = Visible;
	}

	private async Task InitializeAsync()
	{
		_initializing = true;
		_currentStep = 0;
		_selectedMode = ArchiveMode.Switch;
		_inspectionResult = null;
		_bcpInspectionResult = null;
		_bulkCopyInspectionResult = null;
		_autoFixResult = null;
		_autoFixOptions.Clear();
		_executionPreviewMarkdown = null;
		_lastSubmittedTaskId = null;
		
		// 解析分区键
		_partitionKeys.Clear();
		_batchResults.Clear();
		_batchCurrentIndex = 0;
		_batchSuccessCount = 0;
		_batchFailureCount = 0;
		
		if (!string.IsNullOrWhiteSpace(PreSelectedPartitionKey))
		{
			// 支持逗号分隔的多个分区号
			_partitionKeys = PreSelectedPartitionKey
				.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
				.Where(k => !string.IsNullOrWhiteSpace(k))
				.Distinct()
				.ToList();
		}
		
		_isBatchMode = _partitionKeys.Count > 1;
		_form.SourcePartitionKey = _partitionKeys.FirstOrDefault() ?? string.Empty;
		
		Logger.LogInformation("初始化归档向导: IsBatch={IsBatch}, PartitionCount={Count}, Keys={Keys}",
			_isBatchMode, _partitionKeys.Count, string.Join(",", _partitionKeys));

		try
		{
			// 先加载数据源信息,用于判断分区切换是否可用
			await LoadDataSourceInfoAsync();
			
			// 如果分区切换被禁用,自动切换到 BCP 模式
			if (IsSwitchModeDisabled() && _selectedMode == ArchiveMode.Switch)
			{
				_selectedMode = ArchiveMode.Bcp;
				Logger.LogInformation("分区切换模式被禁用(自定义目标服务器),自动切换到 BCP 模式");
			}
			
			await LoadConfigurationsAsync();
			if (_configurationOptions.Any())
			{
				var first = _configurationOptions.First();
				await ApplyConfigurationAsync(first);
			}
			else
			{
				_form.PartitionConfigurationId = null;
				_selectedConfiguration = null;
				_selectedConfigurationDetail = null;
				
				// 无配置时从数据源获取目标数据库并设置默认值
				if (_currentDataSource != null)
				{
					var targetDatabase = _currentDataSource.UseSourceAsTarget
						? _currentDataSource.DatabaseName
						: (_currentDataSource.TargetDatabaseName ?? _currentDataSource.DatabaseName);
					
					_form.TargetDatabase = targetDatabase;
					Logger.LogInformation("无配置场景: 设置目标数据库={TargetDatabase}", targetDatabase);
				}
				
				// 设置默认目标表名为 源表_bak
				_form.TargetTable = $"{SchemaName}.{TableName}_bak";
			}

			_form.RequestedBy = ResolveOperator();
			_form.BackupConfirmed = false;
			
			// 从 localStorage 加载 BCP 临时目录配置
			await LoadBcpTempDirectoryAsync();
		}
		finally
		{
			_initializing = false;
		}
	}

	/// <summary>
	/// 从 localStorage 加载 BCP 临时目录配置
	/// </summary>
	private async Task LoadBcpTempDirectoryAsync()
	{
		try
		{
			var savedTempDir = await JSRuntime.InvokeAsync<string?>("localStorage.getItem", "bcpTempDirectory");
			if (!string.IsNullOrWhiteSpace(savedTempDir))
			{
				_form.BcpTempDirectory = savedTempDir;
				Logger.LogDebug("从 localStorage 加载 BCP 临时目录: {TempDir}", savedTempDir);
			}
			else
			{
				// 如果没有保存的值，使用系统默认临时目录
				_form.BcpTempDirectory = Path.GetTempPath();
				Logger.LogDebug("使用默认 BCP 临时目录: {TempDir}", _form.BcpTempDirectory);
			}
		}
		catch (Exception ex)
		{
			Logger.LogWarning(ex, "加载 BCP 临时目录配置失败，使用默认值");
			_form.BcpTempDirectory = Path.GetTempPath();
		}
	}

	/// <summary>
	/// 保存 BCP 临时目录到 localStorage
	/// </summary>
	private async Task SaveBcpTempDirectoryAsync()
	{
		try
		{
			if (!string.IsNullOrWhiteSpace(_form.BcpTempDirectory))
			{
				await JSRuntime.InvokeVoidAsync("localStorage.setItem", "bcpTempDirectory", _form.BcpTempDirectory);
				Logger.LogDebug("保存 BCP 临时目录到 localStorage: {TempDir}", _form.BcpTempDirectory);
			}
		}
		catch (Exception ex)
		{
			Logger.LogWarning(ex, "保存 BCP 临时目录配置失败");
		}
	}

	private async Task LoadConfigurationsAsync()
	{
		_loadingConfigurations = true;
		_configurationOptions.Clear();

		try
		{
			Logger.LogInformation("加载配置: DataSourceId={DataSourceId}, SchemaName={SchemaName}, TableName={TableName}", 
				DataSourceId, SchemaName, TableName);

			var result = await PartitionConfigApi.GetByDataSourceAsync(DataSourceId);
			if (!result.IsSuccess || result.Value is null)
			{
				if (!string.IsNullOrWhiteSpace(result.Error))
				{
					Message.Warning(result.Error);
				}
				Logger.LogWarning("加载配置失败: {Error}", result.Error);
				return;
			}

			Logger.LogInformation("从API获取到 {Count} 条配置", result.Value.Count);
			foreach (var config in result.Value)
			{
				Logger.LogInformation("配置: SchemaName={SchemaName}, TableName={TableName}, Id={Id}", 
					config.SchemaName, config.TableName, config.Id);
			}

			foreach (var config in result.Value.Where(c => string.Equals(c.SchemaName, SchemaName, StringComparison.OrdinalIgnoreCase) && string.Equals(c.TableName, TableName, StringComparison.OrdinalIgnoreCase)))
			{
				_configurationOptions.Add(new PartitionConfigurationOption(config));
				Logger.LogInformation("匹配到配置: {ConfigId}", config.Id);
			}

			if (!_configurationOptions.Any())
			{
				Logger.LogInformation("未找到匹配的配置");
				Message.Info("未找到已保存的配置，可直接使用源表信息进行归档。");
			}
			else
			{
				Logger.LogInformation("找到 {Count} 条匹配的配置", _configurationOptions.Count);
			}
		}
		catch (Exception ex)
		{
			Logger.LogError(ex, "加载分区配置失败");
			Message.Error($"加载分区配置失败: {ex.Message}");
		}
		finally
		{
			_loadingConfigurations = false;
		}
	}

	private string ResolveOperator()
	{
		if (AdminSession.IsAuthenticated && !string.IsNullOrWhiteSpace(AdminSession.UserName))
		{
			return AdminSession.UserName!;
		}

		return Environment.UserName;
	}

	private async Task ApplyConfigurationAsync(PartitionConfigurationOption option)
	{
		_selectedConfiguration = option;
		if (_form.PartitionConfigurationId != option.Id)
		{
			_form.PartitionConfigurationId = option.Id;
		}

		_form.TargetTable = option.DefaultTargetTable;
		_form.TargetDatabase = option.DefaultTargetDatabase;
		_form.CreateStagingTable = option.IsCommitted ? false : true;
		_form.BackupConfirmed = false;
		_selectedConfigurationDetail = null;

		try
		{
			var detailResult = await PartitionConfigApi.GetAsync(option.Id);
			if (detailResult.IsSuccess && detailResult.Value is not null)
			{
				_selectedConfigurationDetail = detailResult.Value;
				if (!string.IsNullOrWhiteSpace(detailResult.Value.TargetTableName))
				{
					_form.TargetTable = detailResult.Value.TargetTableName;
				}

				if (!string.IsNullOrWhiteSpace(detailResult.Value.TargetDatabaseName))
				{
					_form.TargetDatabase = detailResult.Value.TargetDatabaseName;
				}
			}
		}
		catch (Exception ex)
		{
			Message.Warning($"读取配置详情失败: {ex.Message}");
		}

		// 如果配置中没有目标数据库，从数据源自动填充
		if (string.IsNullOrWhiteSpace(_form.TargetDatabase) && _currentDataSource != null)
		{
			var targetDatabase = _currentDataSource.UseSourceAsTarget
				? _currentDataSource.DatabaseName
				: (_currentDataSource.TargetDatabaseName ?? _currentDataSource.DatabaseName);
			
			_form.TargetDatabase = targetDatabase;
			Logger.LogInformation("配置中无目标数据库，从数据源自动填充: {TargetDatabase}", targetDatabase);
		}

		StateHasChanged();
	}

	/// <summary>
	/// 归档模式切换事件处理
	/// </summary>
	private async Task OnArchiveModeChangedAsync(ArchiveMode newMode)
	{
		if (_selectedMode == newMode)
		{
			return;
		}

		_selectedMode = newMode;
		Logger.LogInformation("归档模式切换: {NewMode}", newMode);

		// 对于BCP和BulkCopy模式,尝试加载已保存的归档配置
		if (newMode == ArchiveMode.Bcp || newMode == ArchiveMode.BulkCopy)
		{
			await LoadArchiveConfigurationAsync(newMode);
		}
		else
		{
			// Switch模式清空归档配置
			_loadedArchiveConfig = null;
		}

		StateHasChanged();
	}

	/// <summary>
	/// 加载归档配置(自动加载已保存配置或从数据源自动填充)
	/// </summary>
	private async Task LoadArchiveConfigurationAsync(ArchiveMode archiveMode)
	{
		_loadingArchiveConfig = true;
		_loadedArchiveConfig = null;
		_currentDataSource = null;

		try
		{
			Logger.LogInformation("开始加载归档配置: Mode={Mode}, DataSourceId={DataSourceId}, Schema={Schema}, Table={Table}",
				archiveMode, DataSourceId, SchemaName, TableName);

			// 同时加载 DataSource 信息以显示服务器地址
			await LoadDataSourceInfoAsync();

			// 查询已保存的归档配置
			var configs = await ArchiveConfigApi.GetAllAsync(DataSourceId, isEnabled: true);
			if (configs != null && configs.Any())
			{
				// 根据架构名、表名和归档方法查找匹配的配置
				var targetMethod = ToArchiveMethod(archiveMode);
				var matchingConfig = configs.FirstOrDefault(c =>
					string.Equals(c.SourceSchemaName, SchemaName, StringComparison.OrdinalIgnoreCase) &&
					string.Equals(c.SourceTableName, TableName, StringComparison.OrdinalIgnoreCase) &&
					c.ArchiveMethod == targetMethod);

				if (matchingConfig != null)
				{
					Logger.LogInformation("找到匹配的归档配置: ConfigId={ConfigId}, Name={Name}",
						matchingConfig.Id, matchingConfig.Name);

					// 加载完整配置详情
					var detail = await ArchiveConfigApi.GetByIdAsync(matchingConfig.Id);
					if (detail != null)
					{
						_loadedArchiveConfig = detail;
						Logger.LogInformation("成功加载归档配置详情");

						// 将配置填充到表单中
						PopulateFormFromArchiveConfig(_loadedArchiveConfig, archiveMode);

						Message.Success($"已自动加载归档配置: {_loadedArchiveConfig.Name}");
					}
					else
					{
						Logger.LogWarning("加载归档配置详情失败");
					}
				}
				else
				{
					Logger.LogInformation("未找到匹配的归档配置,将从数据源自动填充默认值");
					await AutoFillFromDataSourceAsync();
				}
			}
			else
			{
				Logger.LogInformation("没有可用的归档配置,将从数据源自动填充默认值");
				await AutoFillFromDataSourceAsync();
			}
		}
		catch (Exception ex)
		{
			Logger.LogError(ex, "加载归档配置失败");
			Message.Error($"加载归档配置失败: {ex.Message}");
		}
		finally
		{
			_loadingArchiveConfig = false;
		}
	}

	/// <summary>
	/// 自动保存归档配置(在执行前保存,便于下次归档时自动加载)
	/// </summary>
	private async Task SaveArchiveConfigurationAsync()
	{
		try
		{
			Logger.LogInformation("开始保存归档配置: Mode={Mode}, DataSourceId={DataSourceId}, Schema={Schema}, Table={Table}",
				_selectedMode, DataSourceId, SchemaName, TableName);

			// 如果已经加载了配置,则更新;否则创建新配置
			if (_loadedArchiveConfig != null)
			{
				// 更新现有配置
				var updateModel = new UpdateArchiveConfigurationModel
				{
					Name = _loadedArchiveConfig.Name,
					Description = _loadedArchiveConfig.Description,
					DataSourceId = DataSourceId,
					SourceSchemaName = SchemaName,
					SourceTableName = TableName,
					IsPartitionedTable = false, // 暂不支持分区表归档
					PartitionConfigurationId = null,
					// 提供占位符过滤条件以满足实体验证规则
					// 实际的分区筛选逻辑由 BackgroundTask 的 Metadata 控制
					ArchiveFilterColumn = "Id",
					ArchiveFilterCondition = "> 0",
					ArchiveMethod = ToArchiveMethod(_selectedMode),
					DeleteSourceDataAfterArchive = true,
					BatchSize = _selectedMode == ArchiveMode.Bcp ? _form.BcpBatchSize : _form.BulkCopyBatchSize
				};

				await ArchiveConfigApi.UpdateAsync(_loadedArchiveConfig.Id, updateModel);
				Logger.LogInformation("成功更新归档配置: ConfigId={ConfigId}", _loadedArchiveConfig.Id);
			}
			else
			{
				// 创建新配置
				var configName = $"{SchemaName}.{TableName}_{(_selectedMode == ArchiveMode.Bcp ? "BCP" : "BulkCopy")}_{DateTime.Now:yyyyMMdd_HHmmss}";
				var createModel = new CreateArchiveConfigurationModel
				{
					Name = configName,
					Description = $"自动创建的{(_selectedMode == ArchiveMode.Bcp ? "BCP" : "BulkCopy")}归档配置",
					DataSourceId = DataSourceId,
					SourceSchemaName = SchemaName,
					SourceTableName = TableName,
					IsPartitionedTable = false,
					PartitionConfigurationId = null,
					// 提供占位符过滤条件以满足实体验证规则
					// 实际的分区筛选逻辑由 BackgroundTask 的 Metadata 控制
					ArchiveFilterColumn = "Id",
					ArchiveFilterCondition = "> 0",
					ArchiveMethod = ToArchiveMethod(_selectedMode),
					DeleteSourceDataAfterArchive = true,
					BatchSize = _selectedMode == ArchiveMode.Bcp ? _form.BcpBatchSize : _form.BulkCopyBatchSize
				};

				_loadedArchiveConfig = await ArchiveConfigApi.CreateAsync(createModel);
				Logger.LogInformation("成功创建归档配置: ConfigId={ConfigId}, Name={Name}",
					_loadedArchiveConfig.Id, _loadedArchiveConfig.Name);
			}
		}
		catch (Exception ex)
		{
			Logger.LogError(ex, "保存归档配置失败");
			// 保存配置失败不应阻止归档执行,只记录日志
		}
	}

	/// <summary>
	/// 从归档配置填充表单
	/// </summary>
	private void PopulateFormFromArchiveConfig(ArchiveConfigurationDetailModel config, ArchiveMode mode)
	{
		// 填充通用字段
		// 从数据源自动填充目标数据库
		if (_currentDataSource != null)
		{
			var targetDatabase = _currentDataSource.UseSourceAsTarget
				? _currentDataSource.DatabaseName
				: (_currentDataSource.TargetDatabaseName ?? _currentDataSource.DatabaseName);
			
			_form.TargetDatabase = targetDatabase;
			Logger.LogInformation("从数据源自动填充目标数据库: {TargetDatabase}", targetDatabase);
		}
		else
		{
			_form.TargetDatabase = null;
			Logger.LogWarning("数据源信息未加载，无法自动填充目标数据库");
		}
		
		// 目标表名使用配置的源表名,自动添加_bak后缀
		_form.TargetTable = $"{config.SourceSchemaName}.{config.SourceTableName}_bak";

		// 根据模式填充特定参数
		if (mode == ArchiveMode.Bcp)
		{
			_form.BcpBatchSize = config.BatchSize;
			// 其他BCP参数使用默认值(已在字段初始化设置)
			Logger.LogInformation("从配置填充BCP参数: BatchSize={BatchSize}", config.BatchSize);
		}
		else if (mode == ArchiveMode.BulkCopy)
		{
			_form.BulkCopyBatchSize = config.BatchSize;
			// 其他BulkCopy参数使用默认值(已在字段初始化设置)
			Logger.LogInformation("从配置填充BulkCopy参数: BatchSize={BatchSize}", config.BatchSize);
		}
	}

	/// <summary>
	/// 加载数据源信息,用于显示服务器地址等信息
	/// </summary>
	private async Task LoadDataSourceInfoAsync()
	{
		try
		{
			var dataSourceResult = await DataSourceApi.GetAsync();
			if (dataSourceResult.IsSuccess && dataSourceResult.Value != null)
			{
				_currentDataSource = dataSourceResult.Value.FirstOrDefault(ds => ds.Id == DataSourceId);
				if (_currentDataSource != null)
				{
					Logger.LogInformation("成功加载数据源信息: Name={Name}, Server={Server}",
						_currentDataSource.Name, _currentDataSource.ServerAddress);
				}
				else
				{
					Logger.LogWarning("未找到DataSourceId={DataSourceId}的数据源", DataSourceId);
				}
			}
		}
		catch (Exception ex)
		{
			Logger.LogError(ex, "加载数据源信息失败");
		}
	}

	/// <summary>
	/// 从数据源信息自动填充默认值
	/// </summary>
	private async Task AutoFillFromDataSourceAsync()
	{
		try
		{
			// 如果还没加载数据源信息,则先加载
			if (_currentDataSource == null)
			{
				await LoadDataSourceInfoAsync();
			}

			if (_currentDataSource != null)
			{
				// 自动填充目标数据库
				var targetDatabase = _currentDataSource.UseSourceAsTarget
					? _currentDataSource.DatabaseName
					: (_currentDataSource.TargetDatabaseName ?? _currentDataSource.DatabaseName);

				_form.TargetDatabase = targetDatabase;

				// 自动填充目标表名(源表_bak)
				_form.TargetTable = $"{SchemaName}.{TableName}_bak";

				Logger.LogInformation("从数据源自动填充: TargetDatabase={TargetDatabase}, TargetTable={TargetTable}",
					targetDatabase, _form.TargetTable);

				Message.Info("已从数据源自动填充默认配置");
			}
			else
			{
				Logger.LogWarning("无法从数据源自动填充,数据源信息未加载");
			}
		}
		catch (Exception ex)
		{
			Logger.LogError(ex, "从数据源自动填充失败");
		}
	}

	private string FormatTableName() => string.IsNullOrWhiteSpace(SchemaName) || string.IsNullOrWhiteSpace(TableName)
		? "(未指定)"
		: $"{SchemaName}.{TableName}";

	/// <summary>
	/// 判断分区切换模式是否可用(需要同源同实例)
	/// </summary>
	private bool IsSwitchModeDisabled()
	{
		if (_currentDataSource == null)
		{
			return false;
		}

		// 如果配置了自定义目标服务器(UseSourceAsTarget=false),则禁用分区切换
		if (!_currentDataSource.UseSourceAsTarget)
		{
			return true;
		}

		return false;
	}

	/// <summary>
	/// 获取分区切换禁用的提示文字
	/// </summary>
	private string GetSwitchDisabledReason()
	{
		if (_currentDataSource == null)
		{
			return string.Empty;
		}

		if (!_currentDataSource.UseSourceAsTarget)
		{
			return "分区切换要求源服务器和目标服务器为同一实例,当前配置了自定义目标服务器,无法使用分区切换模式";
		}

		return string.Empty;
	}

	/// <summary>
	/// 获取当前归档模式的显示文本
	/// </summary>
	private string GetArchiveModeDisplayText()
	{
		return _selectedMode switch
		{
			ArchiveMode.Switch => "分区切换",
			ArchiveMode.Bcp => "BCP 归档",
			ArchiveMode.BulkCopy => "BulkCopy 归档",
			_ => "未知模式"
		};
	}

	private string DisplayPartitionKey()
	{
		if (!_partitionKeys.Any())
		{
			return "(未选择)";
		}
		
		if (_isBatchMode)
		{
			return $"{string.Join(", ", _partitionKeys.Take(3))}{(_partitionKeys.Count > 3 ? $"... 共{_partitionKeys.Count}个分区" : "")}";
		}
		
		return _form.SourcePartitionKey;
	}

	private static string FormatTableInfo(PartitionSwitchTableInfoDto table)
		=> $"{table.SchemaName}.{table.TableName}";

	private bool CanProceedToNextStep()
	{
		return _currentStep switch
		{
			// Step 0: 选择归档模式 - 现在三种模式都可以进入下一步
			0 => true,
			
			// Step 1: 填写参数
			1 => ValidateStep1Parameters(),
			
			// Step 2: 预检结果 - 所有模式都需要预检通过
			2 => _selectedMode switch
			{
				ArchiveMode.Switch => _inspectionResult is not null && _inspectionResult.CanExecute,
				ArchiveMode.Bcp => _bcpInspectionResult is not null && _bcpInspectionResult.CanExecute,
				ArchiveMode.BulkCopy => _bulkCopyInspectionResult is not null && _bulkCopyInspectionResult.CanExecute,
				_ => false
			},
			
			_ => true
		};
	}

	/// <summary>
	/// 验证Step 1的参数是否完整
	/// </summary>
	private bool ValidateStep1Parameters()
	{
		// 通用参数验证
		if (string.IsNullOrWhiteSpace(_form.TargetTable))
		{
			return false;
		}

		// Switch模式需要分区键
		if (_selectedMode == ArchiveMode.Switch && string.IsNullOrWhiteSpace(_form.SourcePartitionKey))
		{
			return false;
		}

		// BCP/BulkCopy模式验证
		if (_selectedMode == ArchiveMode.Bcp)
		{
			// 验证BCP参数
			if (_form.BcpBatchSize <= 0 || _form.BcpTimeoutSeconds <= 0)
			{
				return false;
			}
			if (string.IsNullOrWhiteSpace(_form.BcpTempDirectory))
			{
				return false;
			}
		}
		else if (_selectedMode == ArchiveMode.BulkCopy)
		{
			// 验证BulkCopy参数
			if (_form.BulkCopyBatchSize <= 0 || _form.BulkCopyTimeoutSeconds <= 0)
			{
				return false;
			}
		}

		return true;
	}

	private void PrevStep()
	{
		if (_currentStep <= 0)
		{
			return;
		}

		_currentStep--;
	}

	private async Task NextStepAsync()
	{
		if (!CanProceedToNextStep())
		{
			return;
		}

		// Step 1 → Step 2: 三种模式都执行预检
		if (_currentStep == 1)
		{
			// 确保数据源信息已加载
			if (_currentDataSource == null)
			{
				await LoadDataSourceInfoAsync();
			}

			if (_selectedMode == ArchiveMode.Switch)
			{
				// Switch模式执行分区切换预检
				if (!await RunInspectionAsync())
				{
					return;
				}
			}
			else if (_selectedMode == ArchiveMode.Bcp)
			{
				// BCP模式执行BCP预检
				if (!await RunBcpInspectionAsync())
				{
					return;
				}
			}
			else if (_selectedMode == ArchiveMode.BulkCopy)
			{
				// BulkCopy模式执行BulkCopy预检
				if (!await RunBulkCopyInspectionAsync())
				{
					return;
				}
			}
		}

		if (_currentStep < 3)
		{
			_currentStep++;
		}
	}

	private async Task<bool> RunInspectionAsync()
	{
		if (string.IsNullOrWhiteSpace(_form.SourcePartitionKey))
		{
			Message.Warning("请填写源分区编号。");
			return false;
		}

		if (string.IsNullOrWhiteSpace(_form.TargetTable))
		{
			Message.Warning("请填写目标表名称。");
			return false;
		}

		_inspectionLoading = true;
		_autoFixResult = null;
		_autoFixOptions.Clear();
		_executionPreviewMarkdown = null;

		try
		{
			var request = new SwitchArchiveInspectRequest(
				_form.PartitionConfigurationId,
				DataSourceId,
				SchemaName,
				TableName,
				_form.SourcePartitionKey.Trim(),
				_form.TargetTable.Trim(),
				string.IsNullOrWhiteSpace(_form.TargetDatabase) ? null : _form.TargetDatabase?.Trim(),
				_form.CreateStagingTable,
				_form.RequestedBy);

			var result = await PartitionArchiveApi.InspectSwitchAsync(request);
			if (!result.IsSuccess || result.Value is null)
			{
				Message.Error(result.Error ?? "预检失败，请稍后重试。");
				return false;
			}

			_inspectionResult = result.Value;
			ResetAutoFixSelections(_inspectionResult.AutoFixSteps);
			UpdateExecutionPreviewMarkdown();
			Message.Success("预检已完成。");
			return true;
		}
		finally
		{
			_inspectionLoading = false;
			StateHasChanged();
		}
	}

	/// <summary>
	/// 执行BCP归档预检
	/// </summary>
	private async Task<bool> RunBcpInspectionAsync()
	{
		if (string.IsNullOrWhiteSpace(_form.TargetTable))
		{
			Message.Warning("请填写目标表名称。");
			return false;
		}

		// 保存 BCP 临时目录配置到 localStorage
		await SaveBcpTempDirectoryAsync();

		_inspectionLoading = true;
		_bcpInspectionResult = null;

		try
		{
			var request = new BcpArchiveInspectRequest(
				DataSourceId,
				SchemaName,
				TableName,
				"1", // BCP模式暂不支持分区,使用占位值
				_form.TargetTable.Trim(),
				string.IsNullOrWhiteSpace(_form.TargetDatabase) ? null : _form.TargetDatabase?.Trim(),
				_form.BcpTempDirectory,
				_form.RequestedBy);

			var result = await PartitionArchiveApi.InspectBcpAsync(request);
			if (!result.IsSuccess || result.Value is null)
			{
				Message.Error(result.Error ?? "BCP预检失败，请稍后重试。");
				return false;
			}

			_bcpInspectionResult = result.Value;

			// 显示检查结果
			if (_bcpInspectionResult.BlockingIssues.Any())
			{
				var issuesText = string.Join("\n", _bcpInspectionResult.BlockingIssues.Select(i => $"• {i.Message}"));
				Message.Error($"发现阻塞问题:\n{issuesText}");
			}
			
			if (_bcpInspectionResult.Warnings.Any())
			{
				var warningsText = string.Join("\n", _bcpInspectionResult.Warnings.Select(w => $"• {w.Message}"));
				Message.Warning($"警告信息:\n{warningsText}");
			}

			if (_bcpInspectionResult.CanExecute)
			{
				Message.Success("BCP预检通过,可以执行归档。");
			}

			return true; // 即使有阻塞问题,也返回true进入Step 2展示详情
		}
		catch (Exception ex)
		{
			Logger.LogError(ex, "BCP预检异常");
			Message.Error($"BCP预检异常: {ex.Message}");
			return false;
		}
		finally
		{
			_inspectionLoading = false;
			StateHasChanged();
		}
	}

	/// <summary>
	/// 执行BulkCopy归档预检
	/// </summary>
	private async Task<bool> RunBulkCopyInspectionAsync()
	{
		if (string.IsNullOrWhiteSpace(_form.TargetTable))
		{
			Message.Warning("请填写目标表名称。");
			return false;
		}

		_inspectionLoading = true;
		_bulkCopyInspectionResult = null;

		try
		{
			var request = new BulkCopyArchiveInspectRequest(
				DataSourceId,
				SchemaName,
				TableName,
				"1", // BulkCopy模式暂不支持分区,使用占位值
				_form.TargetTable.Trim(),
				string.IsNullOrWhiteSpace(_form.TargetDatabase) ? null : _form.TargetDatabase?.Trim(),
				_form.RequestedBy);

			var result = await PartitionArchiveApi.InspectBulkCopyAsync(request);
			if (!result.IsSuccess || result.Value is null)
			{
				Message.Error(result.Error ?? "BulkCopy预检失败，请稍后重试。");
				return false;
			}

			_bulkCopyInspectionResult = result.Value;

			// 显示检查结果
			if (_bulkCopyInspectionResult.BlockingIssues.Any())
			{
				var issuesText = string.Join("\n", _bulkCopyInspectionResult.BlockingIssues.Select(i => $"• {i.Message}"));
				Message.Error($"发现阻塞问题:\n{issuesText}");
			}
			
			if (_bulkCopyInspectionResult.Warnings.Any())
			{
				var warningsText = string.Join("\n", _bulkCopyInspectionResult.Warnings.Select(w => $"• {w.Message}"));
				Message.Warning($"警告信息:\n{warningsText}");
			}

			if (_bulkCopyInspectionResult.CanExecute)
			{
				Message.Success("BulkCopy预检通过,可以执行归档。");
			}

			return true; // 即使有阻塞问题,也返回true进入Step 2展示详情
		}
		catch (Exception ex)
		{
			Logger.LogError(ex, "BulkCopy预检异常");
			Message.Error($"BulkCopy预检异常: {ex.Message}");
			return false;
		}
		finally
		{
			_inspectionLoading = false;
			StateHasChanged();
		}
	}

	private async Task ExecuteAutoFixAsync()
	{
		var selectedCodes = _autoFixOptions
			.Where(option => option.IsSelected)
			.Select(option => option.Code)
			.ToList();

		if (!selectedCodes.Any())
		{
			Message.Warning("请至少选择一个自动补齐步骤。");
			return;
		}

		_autoFixExecuting = true;

		try
		{
			var request = new SwitchArchiveAutoFixRequest(
				_form.PartitionConfigurationId,
				DataSourceId,
				SchemaName,
				TableName,
				_form.SourcePartitionKey.Trim(),
				_form.TargetTable.Trim(),
				string.IsNullOrWhiteSpace(_form.TargetDatabase) ? null : _form.TargetDatabase?.Trim(),
				_form.CreateStagingTable,
				_form.RequestedBy,
				selectedCodes);

			Logger.LogInformation("开始执行自动补齐: ConfigId={ConfigId}, Steps={Steps}", 
				_form.PartitionConfigurationId?.ToString() ?? "(未指定)", string.Join(",", selectedCodes));

			var result = await PartitionArchiveApi.AutoFixSwitchAsync(request);
			if (!result.IsSuccess || result.Value is null)
			{
				var errorMsg = result.Error ?? "自动补齐失败。请检查后台日志。";
				Logger.LogError("自动补齐失败: {Error}", errorMsg);
				Message.Error($"自动补齐失败: {errorMsg}");
				return;
			}

			_autoFixResult = result.Value;
			
			// 记录详细的执行结果
			Logger.LogInformation("自动补齐完成: Succeeded={Succeeded}, ExecutedCount={Count}", 
				_autoFixResult.Succeeded, _autoFixResult.Executions.Count);
			
			foreach (var exec in _autoFixResult.Executions)
			{
				Logger.LogInformation("补齐步骤 {Code}: Succeeded={Succeeded}, Message={Message}", 
					exec.Code, exec.Succeeded, exec.Message);
			}

			if (_autoFixResult.Succeeded)
			{
				Message.Success("自动补齐执行成功，请重新预检确认。");
			}
			else
			{
				var failedSteps = _autoFixResult.Executions.Where(e => !e.Succeeded).ToList();
				var failedMessages = string.Join("; ", failedSteps.Select(s => $"{s.Code}: {s.Message}"));
				Logger.LogWarning("部分自动补齐失败: {FailedSteps}", failedMessages);
				Message.Warning($"部分自动补齐失败: {failedMessages}");
			}
		}
		catch (Exception ex)
		{
			Logger.LogError(ex, "执行自动补齐时发生异常");
			Message.Error($"执行自动补齐时发生异常: {ex.Message}");
		}
		finally
		{
			_autoFixExecuting = false;
			StateHasChanged();
		}
	}

	/// <summary>
	/// 执行 BCP/BulkCopy 归档的自动修复。
	/// </summary>
	private async Task OnBcpAutoFixClickedAsync()
	{
		// 根据当前模式选择正确的检查结果
		var inspectionResult = _selectedMode == ArchiveMode.BulkCopy 
			? _bulkCopyInspectionResult 
			: _bcpInspectionResult;

		if (inspectionResult?.AutoFixSteps == null || !inspectionResult.AutoFixSteps.Any())
		{
			Message.Warning("没有可自动修复的步骤。");
			return;
		}

		_autoFixExecuting = true;

		try
		{
			// 目前只支持 CREATE_TARGET_TABLE 修复
			var createTableStep = inspectionResult.AutoFixSteps
				.FirstOrDefault(s => s.Code == "CREATE_TARGET_TABLE");

			if (createTableStep == null)
			{
				Message.Warning("没有找到 CREATE_TARGET_TABLE 修复步骤。");
				return;
			}

			var request = new ArchiveAutoFixRequest(
				DataSourceId,
				SchemaName,
				TableName,
				_form.TargetTable.Trim(),
				_form.TargetDatabase, // 传递目标数据库
				"CREATE_TARGET_TABLE",
				_form.RequestedBy);

			var modeText = _selectedMode == ArchiveMode.BulkCopy ? "BulkCopy" : "BCP";
			Logger.LogInformation("开始执行 {Mode} 自动修复: TargetTable={TargetTable}, TargetDatabase={TargetDatabase}, FixCode={FixCode}", 
				modeText, _form.TargetTable, _form.TargetDatabase, "CREATE_TARGET_TABLE");

			var result = await PartitionArchiveApi.ExecuteAutoFixAsync(request);
			if (!result.IsSuccess || string.IsNullOrEmpty(result.Value))
			{
				var errorMsg = result.Error ?? "自动修复失败。请检查后台日志。";
				Logger.LogError("自动修复失败: {Error}", errorMsg);
				Message.Error($"自动修复失败: {errorMsg}");
				return;
			}

			Logger.LogInformation("自动修复完成: {Message}", result.Value);
			Message.Success("目标表已成功创建,正在重新预检...");
			
			// 根据当前模式自动重新预检
			if (_selectedMode == ArchiveMode.BulkCopy)
			{
				await RunBulkCopyInspectionAsync();
			}
			else
			{
				await RunBcpInspectionAsync();
			}
		}
		catch (Exception ex)
		{
			Logger.LogError(ex, "执行 BCP 自动修复时发生异常");
			Message.Error($"执行自动修复时发生异常: {ex.Message}");
		}
		finally
		{
			_autoFixExecuting = false;
			StateHasChanged();
		}
	}

	private async Task SubmitAsync()
	{
		if (!_form.BackupConfirmed)
		{
			Message.Warning("请确认已完成备份。");
			return;
		}

		_submitting = true;
		_batchResults.Clear();
		_batchSuccessCount = 0;
		_batchFailureCount = 0;

		try
		{
			// 自动保存归档配置(BCP/BulkCopy模式)
			if (_selectedMode == ArchiveMode.Bcp || _selectedMode == ArchiveMode.BulkCopy)
			{
				await SaveArchiveConfigurationAsync();
			}
			if (_isBatchMode)
			{
				Logger.LogInformation("开始批量执行分区切换: Count={Count}", _partitionKeys.Count);
				
				for (var i = 0; i < _partitionKeys.Count; i++)
				{
					_batchCurrentIndex = i;
					var partitionKey = _partitionKeys[i];
					
					Logger.LogInformation("执行分区切换 [{Index}/{Total}]: PartitionKey={Key}", 
						i + 1, _partitionKeys.Count, partitionKey);
					
					StateHasChanged(); // 更新UI显示当前进度

					try
					{
						var request = new SwitchArchiveExecuteRequest(
							_form.PartitionConfigurationId,
							DataSourceId,
							SchemaName,
							TableName,
							partitionKey.Trim(),
							_form.TargetTable.Trim(),
							string.IsNullOrWhiteSpace(_form.TargetDatabase) ? null : _form.TargetDatabase?.Trim(),
							_form.CreateStagingTable,
							_form.BackupConfirmed,
							_form.RequestedBy);

						var result = await PartitionArchiveApi.ArchiveBySwitchAsync(request);
						
						if (result.IsSuccess && result.Value != Guid.Empty)
						{
							_batchSuccessCount++;
							_batchResults.Add(new BatchExecutionResult(partitionKey, true, "成功", result.Value));
							Logger.LogInformation("分区 {Key} 切换成功, TaskId={TaskId}", partitionKey, result.Value);
						}
						else
						{
							_batchFailureCount++;
							var errorMsg = result.Error ?? "未知错误";
							_batchResults.Add(new BatchExecutionResult(partitionKey, false, errorMsg, null));
							Logger.LogWarning("分区 {Key} 切换失败: {Error}", partitionKey, errorMsg);
						}
					}
					catch (Exception ex)
					{
						_batchFailureCount++;
						_batchResults.Add(new BatchExecutionResult(partitionKey, false, $"异常: {ex.Message}", null));
						Logger.LogError(ex, "分区 {Key} 切换异常", partitionKey);
					}
					
					// 短暂延迟,避免压垮数据库
					if (i < _partitionKeys.Count - 1)
					{
						await Task.Delay(500);
					}
				}
				
				var summary = $"批量执行完成: 成功 {_batchSuccessCount} 个, 失败 {_batchFailureCount} 个";
				Logger.LogInformation(summary);
				
				if (_batchFailureCount == 0)
				{
					Message.Success(summary);
				}
				else if (_batchSuccessCount > 0)
				{
					Message.Warning(summary);
				}
				else
				{
					Message.Error(summary);
				}
				
				// 保存第一个成功任务的ID用于跳转
				var firstSuccess = _batchResults.FirstOrDefault(r => r.Success);
				_lastSubmittedTaskId = firstSuccess?.TaskId;
			}
			else
			{
				// 单个分区执行
				if (_selectedMode == ArchiveMode.Bcp)
				{
					// BCP 模式
					var bcpRequest = new Application.Partitions.BcpArchiveExecuteRequest(
						DataSourceId,
						SchemaName,
						TableName,
						_form.SourcePartitionKey.Trim(), // 使用用户选择的分区号
						_form.TargetTable.Trim(),
						string.IsNullOrWhiteSpace(_form.TargetDatabase) ? null : _form.TargetDatabase?.Trim(),
						_form.BcpTempDirectory,
						_form.BcpBatchSize,
						_form.BcpUseNativeFormat,
						_form.BcpMaxErrors,
						_form.BcpTimeoutSeconds,
						_form.BackupConfirmed,
						_form.RequestedBy);

					var result = await PartitionArchiveApi.ArchiveByBcpAsync(bcpRequest);
					
					if (!result.IsSuccess)
					{
						Message.Error(result.Error ?? "提交 BCP 归档失败。");
						return;
					}

					_lastSubmittedTaskId = result.Value;
					Message.Success("BCP 归档已提交到后台任务。请前往任务中心查看进度。");
				}
				else if (_selectedMode == ArchiveMode.BulkCopy)
				{
					// BulkCopy 模式
					var bulkCopyRequest = new Application.Partitions.BulkCopyArchiveExecuteRequest(
						DataSourceId,
						SchemaName,
						TableName,
						_form.SourcePartitionKey.Trim(), // 使用用户选择的分区号
						_form.TargetTable.Trim(),
						string.IsNullOrWhiteSpace(_form.TargetDatabase) ? null : _form.TargetDatabase?.Trim(),
						_form.BulkCopyBatchSize,
						_form.BulkCopyNotifyAfterRows,
						_form.BulkCopyTimeoutSeconds,
						_form.BulkCopyEnableStreaming,
						_form.BackupConfirmed,
						_form.RequestedBy);

					var result = await PartitionArchiveApi.ArchiveByBulkCopyAsync(bulkCopyRequest);
					
					if (!result.IsSuccess)
					{
						Message.Error(result.Error ?? "提交 BulkCopy 归档失败。");
						return;
					}

					_lastSubmittedTaskId = result.Value;
					Message.Success("BulkCopy 归档已提交到后台任务。请前往任务中心查看进度。");
				}
				else
				{
					// Switch 模式 - 原有逻辑
					var request = new SwitchArchiveExecuteRequest(
						_form.PartitionConfigurationId,
						DataSourceId,
						SchemaName,
						TableName,
						_form.SourcePartitionKey.Trim(),
						_form.TargetTable.Trim(),
						string.IsNullOrWhiteSpace(_form.TargetDatabase) ? null : _form.TargetDatabase?.Trim(),
						_form.CreateStagingTable,
						_form.BackupConfirmed,
						_form.RequestedBy);

					var result = await PartitionArchiveApi.ArchiveBySwitchAsync(request);
					
					if (!result.IsSuccess)
					{
						Message.Error(result.Error ?? "提交分区切换失败。");
						return;
					}

					_lastSubmittedTaskId = result.Value;
					Message.Success("分区切换已提交到后台任务。请前往任务中心查看进度。");
				}
			}

			if (_lastSubmittedTaskId.HasValue && OnSuccess.HasDelegate)
			{
				await OnSuccess.InvokeAsync(_lastSubmittedTaskId.Value);
			}

			await CloseAsync();
		}
		finally
		{
			_submitting = false;
		}
	}

	private async Task CloseAsync()
	{
		if (VisibleChanged.HasDelegate)
		{
			await VisibleChanged.InvokeAsync(false);
		}

		_wasVisible = false;
		_configurationOptions.Clear();
		_inspectionResult = null;
		_autoFixResult = null;
		_autoFixOptions.Clear();
		_form.PartitionConfigurationId = null;
		_form.SourcePartitionKey = string.Empty;
		_form.TargetDatabase = null;
		_form.TargetTable = string.Empty;
		_form.CreateStagingTable = true;
		_form.BackupConfirmed = false;
		_selectedConfigurationDetail = null;
		_executionPreviewMarkdown = null;
		_lastSubmittedTaskId = null;
	}

	private void ResetAutoFixSelections(IReadOnlyList<PartitionSwitchAutoFixStepDto> steps)
	{
		_autoFixOptions.Clear();
		foreach (var step in steps)
		{
			_autoFixOptions.Add(new AutoFixOptionState(step.Code, step.Description, step.Recommendation, true));
		}
	}

	private void UpdateExecutionPreviewMarkdown()
	{
		if (_inspectionResult is null)
		{
			_executionPreviewMarkdown = null;
			return;
		}

		var sb = new StringBuilder();
		sb.AppendLine("### 分区切换任务概览");
		sb.AppendLine();
		sb.AppendLine($"- 源表：`{FormatTableName()}`");
		sb.AppendLine($"- 目标表：`{_form.TargetTable}`");
		sb.AppendLine($"- 目标数据库：`{_form.TargetDatabase ?? "(沿用配置)"}`");
		sb.AppendLine($"- 源分区编号：`{_form.SourcePartitionKey}`");
		sb.AppendLine($"- 创建临时表：{(_form.CreateStagingTable ? "是" : "否")}");

		if (_inspectionResult.Plan is not null)
		{
			sb.AppendLine();
			sb.AppendLine("#### 自动补齐脚本预览");
			if (_inspectionResult.Plan.AutoFixes.Any())
			{
				foreach (var fix in _inspectionResult.Plan.AutoFixes)
				{
					sb.AppendLine($"- **{fix.Title}** ({fix.Code})");
					if (!string.IsNullOrWhiteSpace(fix.Prerequisite))
					{
						sb.AppendLine($"  - 前置条件：{fix.Prerequisite}");
					}
					if (fix.Commands.Any())
					{
						foreach (var command in fix.Commands)
						{
							var commandDescription = string.IsNullOrWhiteSpace(command.Description) ? "自动补齐脚本" : command.Description;
							sb.AppendLine();
							sb.AppendLine("```sql");
							sb.AppendLine($"-- {commandDescription}");
							sb.AppendLine(command.CommandText);
							sb.AppendLine("```");
						}
					}
				}
			}
			else
			{
				sb.AppendLine("- 无需自动补齐，所有条件已满足。");
			}
		}

		var switchScript = BuildSwitchScript();
		if (!string.IsNullOrWhiteSpace(switchScript))
		{
			sb.AppendLine();
			sb.AppendLine("#### 分区切换 SQL 预览");
			sb.AppendLine();
			sb.AppendLine("```sql");
			sb.AppendLine(switchScript);
			sb.AppendLine("```");
		}

		_executionPreviewMarkdown = sb.ToString();
	}

	private string BuildSwitchScript()
	{
		if (string.IsNullOrWhiteSpace(_form.SourcePartitionKey))
		{
			return string.Empty;
		}

		var sourceTable = BuildQualifiedIdentifier(SchemaName, TableName, null);
		var targetTable = BuildTargetTableIdentifier();

		var builder = new StringBuilder();
		builder.AppendLine("SET XACT_ABORT ON;");
		builder.AppendLine("BEGIN TRANSACTION;");
		builder.AppendLine($"ALTER TABLE {sourceTable} SWITCH PARTITION {_form.SourcePartitionKey.Trim()} TO {targetTable};");
		builder.AppendLine("COMMIT;");
		return builder.ToString();
	}

	private string BuildTargetTableIdentifier()
	{
		var targetTableRaw = _form.TargetTable?.Trim();
		if (string.IsNullOrWhiteSpace(targetTableRaw))
		{
			return BuildQualifiedIdentifier(SchemaName, TableName, _form.TargetDatabase);
		}

		var parts = targetTableRaw.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
		var schema = SchemaName;
		var table = TableName;
		var database = string.IsNullOrWhiteSpace(_form.TargetDatabase) ? null : _form.TargetDatabase?.Trim();

		if (parts.Length == 1)
		{
			table = parts[0];
		}
		else if (parts.Length == 2)
		{
			schema = parts[0];
			table = parts[1];
		}
		else if (parts.Length >= 3)
		{
			database = parts[0];
			schema = parts[1];
			table = parts[2];
		}

		return BuildQualifiedIdentifier(schema, table, database);
	}

	private static string BuildQualifiedIdentifier(string schema, string table, string? database)
	{
		var items = new List<string>();
		if (!string.IsNullOrWhiteSpace(database))
		{
			items.Add(WrapSqlIdentifier(database));
		}
		items.Add(WrapSqlIdentifier(schema));
		items.Add(WrapSqlIdentifier(table));
		return string.Join('.', items);
	}

	private static string WrapSqlIdentifier(string identifier)
	{
		var trimmed = identifier.Trim();
		if (trimmed.Length == 0)
		{
			return "";
		}

		if (trimmed.StartsWith('[') && trimmed.EndsWith(']'))
		{
			return trimmed;
		}

		return $"[{trimmed.Replace("]", "]]")}]";
	}

	/// <summary>
	/// 获取格式化的源服务器地址
	/// </summary>
	private string GetSourceServerDisplay()
	{
		if (_currentDataSource == null) return "(未知)";
		
		var port = _currentDataSource.ServerPort == 1433 ? "" : $":{_currentDataSource.ServerPort}";
		return $"{_currentDataSource.ServerAddress}{port}";
	}

	/// <summary>
	/// 获取格式化的目标服务器地址
	/// </summary>
	private string GetTargetServerDisplay()
	{
		if (_currentDataSource == null) return "(未知)";
		
		if (_currentDataSource.UseSourceAsTarget)
		{
			var port = _currentDataSource.ServerPort == 1433 ? "" : $":{_currentDataSource.ServerPort}";
			return $"{_currentDataSource.ServerAddress}{port} (与源服务器相同)";
		}
		else
		{
			var targetServer = _currentDataSource.TargetServerAddress ?? _currentDataSource.ServerAddress;
			var port = _currentDataSource.TargetServerPort == 1433 ? "" : $":{_currentDataSource.TargetServerPort}";
			return $"{targetServer}{port}";
		}
	}

	/// <summary>
	/// 获取源数据库认证方式
	/// </summary>
	private string GetSourceAuthDisplay()
	{
		if (_currentDataSource == null) return "(未知)";
		return _currentDataSource.UseIntegratedSecurity 
			? "Windows 集成身份验证" 
			: $"SQL Server 身份验证 ({_currentDataSource.UserName})";
	}

	/// <summary>
	/// 获取目标数据库认证方式
	/// </summary>
	private string GetTargetAuthDisplay()
	{
		if (_currentDataSource == null) return "(未知)";
		
		if (_currentDataSource.UseSourceAsTarget)
		{
			return _currentDataSource.UseIntegratedSecurity 
				? "Windows 集成身份验证" 
				: $"SQL Server 身份验证 ({_currentDataSource.UserName})";
		}
		else
		{
			return _currentDataSource.TargetUseIntegratedSecurity 
				? "Windows 集成身份验证" 
				: $"SQL Server 身份验证 ({_currentDataSource.TargetUserName})";
		}
	}

	/// <summary>
	/// 获取目标数据库显示名称
	/// </summary>
	private string GetTargetDatabaseDisplay()
	{
		if (_currentDataSource == null) return "(未知)";
		
		// 如果用户在表单中输入了目标数据库，优先使用
		if (!string.IsNullOrWhiteSpace(_form.TargetDatabase))
		{
			return _form.TargetDatabase;
		}
		
		// 同服务器归档：使用源数据库名称
		if (_currentDataSource.UseSourceAsTarget)
		{
			return _currentDataSource.DatabaseName;
		}
		
		// 跨服务器归档：使用目标数据库配置，如果没有配置则显示提示
		if (!string.IsNullOrWhiteSpace(_currentDataSource.TargetDatabaseName))
		{
			return _currentDataSource.TargetDatabaseName;
		}
		
		return "(未配置)";
	}

	private enum ArchiveMode
	{
		Switch,
		Bcp,
		BulkCopy
	}

	/// <summary>
	/// 获取归档模式的显示名称
	/// </summary>
	private string GetArchiveModeDisplayName()
	{
		return _selectedMode switch
		{
			ArchiveMode.Bcp => "BCP 归档",
			ArchiveMode.BulkCopy => "BulkCopy 归档",
			ArchiveMode.Switch => "分区切换",
			_ => "分区切换"
		};
	}

	/// <summary>
	/// 将UI的ArchiveMode转换为Domain的ArchiveMethod
	/// </summary>
	private static ArchiveMethod ToArchiveMethod(ArchiveMode mode)
	{
		return mode switch
		{
			ArchiveMode.Bcp => ArchiveMethod.Bcp,
			ArchiveMode.BulkCopy => ArchiveMethod.BulkCopy,
			ArchiveMode.Switch => ArchiveMethod.PartitionSwitch,
			_ => ArchiveMethod.PartitionSwitch
		};
	}

	private sealed class AutoFixOptionState
	{
		public AutoFixOptionState(string code, string description, string? recommendation, bool isSelected)
		{
			Code = code;
			Description = description;
			Recommendation = recommendation;
			IsSelected = isSelected;
		}

		public string Code { get; }
		public string Description { get; }
		public string? Recommendation { get; }
		public bool IsSelected { get; set; }
	}

	/// <summary>
	/// 归档表单模型(支持Switch/BCP/BulkCopy三种模式)
	/// </summary>
	private sealed class SwitchArchiveFormModel
	{
		// === 通用字段 ===
		public Guid? PartitionConfigurationId { get; set; }
		public string SourcePartitionKey { get; set; } = string.Empty;
		public string? TargetDatabase { get; set; }
		public string TargetTable { get; set; } = string.Empty;
		public bool CreateStagingTable { get; set; } = true;
		public bool BackupConfirmed { get; set; }
		public string RequestedBy { get; set; } = string.Empty;

		// === BCP模式专用字段 ===
		/// <summary>
		/// 临时文件目录（初始化时从 localStorage 加载）
		/// </summary>
		public string BcpTempDirectory { get; set; } = string.Empty;

		/// <summary>
		/// 使用原生格式(native format)
		/// </summary>
		public bool BcpUseNativeFormat { get; set; } = true;

		/// <summary>
		/// 最大错误数(超过此值则中止)
		/// </summary>
		public int BcpMaxErrors { get; set; } = 10;

		/// <summary>
		/// 批次大小
		/// </summary>
		public int BcpBatchSize { get; set; } = 10000;

		/// <summary>
		/// 超时时间(秒)
		/// </summary>
		public int BcpTimeoutSeconds { get; set; } = 3600;

		// === BulkCopy模式专用字段 ===
		/// <summary>
		/// 批次大小
		/// </summary>
		public int BulkCopyBatchSize { get; set; } = 10000;

		/// <summary>
		/// 通知行数(每N行触发进度通知)
		/// </summary>
		public int BulkCopyNotifyAfterRows { get; set; } = 50000;

		/// <summary>
		/// 超时时间(秒)
		/// </summary>
		public int BulkCopyTimeoutSeconds { get; set; } = 3600;

		/// <summary>
		/// 启用流式读取
		/// </summary>
		public bool BulkCopyEnableStreaming { get; set; } = true;
	}

	private sealed class PartitionConfigurationOption
	{
		public PartitionConfigurationOption(PartitionConfigurationSummaryModel model)
		{
			Id = model.Id;
			Display = $"{model.SchemaName}.{model.TableName} / {model.PartitionFunctionName}";
			TargetDisplay = string.IsNullOrWhiteSpace(model.TargetTableName)
				? "(未设置目标表)"
				: model.TargetTableName;
			DefaultTargetTable = string.IsNullOrWhiteSpace(model.TargetTableName)
				? $"{model.SchemaName}.{model.TableName}_arch"
				: model.TargetTableName;
			DefaultTargetDatabase = null;
			IsCommitted = model.IsCommitted;
		}

		public Guid Id { get; }
		public string Display { get; }
		public string TargetDisplay { get; }
		public string DefaultTargetTable { get; }
		public string? DefaultTargetDatabase { get; }
		public bool IsCommitted { get; }
	}
}
