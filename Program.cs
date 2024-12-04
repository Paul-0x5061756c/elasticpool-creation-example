using ElasticPoolDemo;
using Microsoft.Extensions.Configuration;


IConfigurationRoot configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
    .AddJsonFile("appsettings.development.json", optional: true, reloadOnChange: true)
    .Build();

string? subscriptionId = configuration["SubscriptionId"];
string? resourceGroupName = configuration["ResourceGroupName"];
string? serverName = configuration["ServerName"];

if (subscriptionId is null || resourceGroupName is null || serverName is null)
{
    throw new Exception("SubscriptionId, ResourceGroupName, and ServerName must be provided in the configuration file");
}

var elasticPoolManager = new ElasticPoolManager(configuration);

elasticPoolManager.CreateDatabaseAsync(10, "Mister Suits").Wait();
