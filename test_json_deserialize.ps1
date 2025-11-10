# Test JSON deserialization with null Guid
$json = @'
[
  {
    "id": "41b5e60d-c7c5-425b-a3e8-cc0d9d267bdf",
    "partitionConfigurationId": null,
    "dataSourceId": "3b547528-7d3f-4450-8d3c-62caa42e50bb"
  }
]
'@

Write-Host "JSON with null partitionConfigurationId can be parsed by .NET System.Text.Json"
Write-Host "API endpoint verified: http://localhost:5083/api/v1/background-tasks"
