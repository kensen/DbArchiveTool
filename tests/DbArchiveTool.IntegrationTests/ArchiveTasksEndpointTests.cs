using DbArchiveTool.Api;
using Microsoft.AspNetCore.Mvc.Testing;
using System.Net;

namespace DbArchiveTool.IntegrationTests;

public class ArchiveTasksEndpointTests : IClassFixture<WebApplicationFactory<Program>>
{
    private readonly HttpClient _client;

    public ArchiveTasksEndpointTests(WebApplicationFactory<Program> factory)
    {
        _client = factory.CreateClient();
    }

    [Fact(Skip = "API skeleton does not have persistent storage yet.")]
    public async Task Get_ShouldReturnSuccess()
    {
        var response = await _client.GetAsync("/api/v1/archive-tasks");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }
}
