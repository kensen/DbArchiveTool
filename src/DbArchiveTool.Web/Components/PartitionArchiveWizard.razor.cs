using AntDesign;
using DbArchiveTool.Application.Partitions;
using DbArchiveTool.Web.Core;
using DbArchiveTool.Web.Services;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.Logging;
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
	[Inject] private MessageService Message { get; set; } = default!;
	[Inject] private AdminSessionState AdminSession { get; set; } = default!;
	[Inject] private ILogger<PartitionArchiveWizard> Logger { get; set; } = default!;

	private readonly SwitchArchiveFormModel _form = new();
	private readonly List<PartitionConfigurationOption> _configurationOptions = new();
	private PartitionConfigurationOption? _selectedConfiguration;
	private PartitionConfigurationDetailModel? _selectedConfigurationDetail;
	private bool _initializing;
	private bool _loadingConfigurations;
	private bool _inspectionLoading;
	private bool _autoFixExecuting;
	private bool _submitting;
	private bool _wasVisible;
	private int _currentStep;
	private ArchiveMode _selectedMode = ArchiveMode.Switch;
	private PartitionSwitchInspectionResultDto? _inspectionResult;
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
				var dataSourceResult = await DataSourceApi.GetAsync();
				if (dataSourceResult.IsSuccess && dataSourceResult.Value != null)
				{
					var dataSource = dataSourceResult.Value.FirstOrDefault(ds => ds.Id == DataSourceId);
					if (dataSource != null)
					{
						var targetDatabase = dataSource.UseSourceAsTarget
							? dataSource.DatabaseName
							: (dataSource.TargetDatabaseName ?? dataSource.DatabaseName);
						
						_form.TargetDatabase = targetDatabase;
						Logger.LogInformation("无配置场景: 设置目标数据库={TargetDatabase}", targetDatabase);
					}
				}
				
				// 设置默认目标表名为 源表_bak
				_form.TargetTable = $"{SchemaName}.{TableName}_bak";
			}

			_form.RequestedBy = ResolveOperator();
			_form.BackupConfirmed = false;
		}
		finally
		{
			_initializing = false;
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

		StateHasChanged();
	}

	private string FormatTableName() => string.IsNullOrWhiteSpace(SchemaName) || string.IsNullOrWhiteSpace(TableName)
		? "(未指定)"
		: $"{SchemaName}.{TableName}";

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
			0 => _selectedMode == ArchiveMode.Switch,
			1 => !string.IsNullOrWhiteSpace(_form.SourcePartitionKey) && !string.IsNullOrWhiteSpace(_form.TargetTable),
			2 => _inspectionResult is not null && _inspectionResult.CanExecute,
			_ => true
		};
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

		if (_currentStep == 1)
		{
			if (!await RunInspectionAsync())
			{
				return;
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
				// 单个分区执行(原有逻辑)
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

	private enum ArchiveMode
	{
		Switch,
		Bcp,
		BulkCopy
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

	private sealed class SwitchArchiveFormModel
	{
		public Guid? PartitionConfigurationId { get; set; }
		public string SourcePartitionKey { get; set; } = string.Empty;
		public string? TargetDatabase { get; set; }
		public string TargetTable { get; set; } = string.Empty;
		public bool CreateStagingTable { get; set; } = true;
		public bool BackupConfirmed { get; set; }
		public string RequestedBy { get; set; } = string.Empty;
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
