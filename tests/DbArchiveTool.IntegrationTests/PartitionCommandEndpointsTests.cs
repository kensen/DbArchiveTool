using System.Net;
using System.Net.Http.Json;
using DbArchiveTool.Api.Models;
using Microsoft.AspNetCore.Mvc;

namespace DbArchiveTool.IntegrationTests;

public class PartitionCommandEndpointsTests : IClassFixture<ApiWebApplicationFactory>
{
    private readonly ApiWebApplicationFactory factory;

    public PartitionCommandEndpointsTests(ApiWebApplicationFactory factory)
    {
        this.factory = factory;
    }

    [Fact(Skip = "Partition command endpoints pending implementation")]
    public async Task PreviewSplit_ShouldRequireValidInput()
    {
        var client = factory.CreateClient();
        var dto = new SplitPartitionDto("dbo", "Orders", new[] { "2024-01-01" }, true, "tester");

        var response = await client.PostAsJsonAsync($"api/v1/archive-data-sources/{Guid.Empty}/partition-commands/preview", dto);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact(Skip = "Partition command endpoints pending implementation")]
    public async Task ExecuteSplit_ShouldValidateBackup()
    {
        var client = factory.CreateClient();
        var dto = new SplitPartitionDto("dbo", "Orders", new[] { "2024-01-01" }, false, "tester");

        var response = await client.PostAsJsonAsync($"api/v1/archive-data-sources/{Guid.NewGuid()}/partition-commands/split", dto);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var problem = await response.Content.ReadFromJsonAsync<ProblemDetails>();
        Assert.Contains("备份", problem!.Detail);
    }
}
