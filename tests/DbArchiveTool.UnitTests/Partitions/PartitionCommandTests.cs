using DbArchiveTool.Application.Partitions;
using DbArchiveTool.Domain.Partitions;
using Microsoft.Extensions.Logging;
using Moq;

namespace DbArchiveTool.UnitTests.Partitions;

public class PartitionCommandTests
{
	private readonly Mock<IPartitionMetadataRepository> metadataRepository = new();
	private readonly Mock<IPartitionCommandRepository> commandRepository = new();
	private readonly Mock<IPartitionCommandScriptGenerator> scriptGenerator = new();
	private readonly PartitionValueParser parser = new();
	private readonly Mock<ILogger<PartitionCommandAppService>> logger = new();

	[Fact]
	public async Task PreviewSplitAsync_Should_Fail_When_DataSourceId_Is_Empty()
	{
		var service = CreateService();
	var request = new SplitPartitionRequest(Guid.Empty, "dbo", "Orders", new[] { "100" }, true, "tester");

		var result = await service.PreviewSplitAsync(request);

		Assert.False(result.IsSuccess);
		Assert.Equal("数据源标识不能为空。", result.Error);
		metadataRepository.VerifyNoOtherCalls();
		scriptGenerator.VerifyNoOtherCalls();
	}

	[Fact]
	public async Task ExecuteSplitAsync_Should_Fail_When_Backup_Not_Confirmed()
	{
		var service = CreateService();
	var request = new SplitPartitionRequest(Guid.NewGuid(), "dbo", "Orders", new[] { "2024-01-01" }, false, "tester");

		var result = await service.ExecuteSplitAsync(request);

		Assert.False(result.IsSuccess);
		Assert.Equal("执行拆分前需要确认已有备份或快照。", result.Error);
		metadataRepository.VerifyNoOtherCalls();
		scriptGenerator.VerifyNoOtherCalls();
		commandRepository.VerifyNoOtherCalls();
	}

	private PartitionCommandAppService CreateService()
		=> new(metadataRepository.Object, commandRepository.Object, scriptGenerator.Object, parser, logger.Object);
}
