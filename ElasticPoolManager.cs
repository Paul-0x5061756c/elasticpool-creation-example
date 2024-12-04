using Azure;
using Azure.Core;
using Azure.Identity;
using Azure.ResourceManager;
using Azure.ResourceManager.Resources;
using Azure.ResourceManager.Sql;
using Azure.ResourceManager.Sql.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

namespace ElasticPoolDemo;

public class ElasticPoolManager
{
    private int? _maxDatabasesPerPool;
    private string? _subscriptionId;
    private string? _resourceGroupName;
    private string? _sqlServerName;
    private SqlSku? _sqlSku;
    private ElasticPoolPerDatabaseSettings? _perDatabaseSettings;
    private string? _connectionString;
    
    
    public ElasticPoolManager(IConfiguration configuration)
    {
        CreateMaxDatabasesPerPool(configuration);
        CreateAzureConfiguration(configuration);
        CreateSku(configuration);
        CreatePerDatabaseSettings(configuration);
        CreateConnectionString(configuration);
    }
    
    private void CreateConnectionString(IConfiguration configuration)
    {
        string? connectionString = configuration["ConnectionString"];
        
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new Exception("ConnectionString must be provided in the configuration file");
        }
        
        _connectionString = connectionString;
    }

    private void CreateMaxDatabasesPerPool(IConfiguration configuration)
    {
        string? maxDatabasesPerPool = configuration["ElasticPoolSettings:MaxDatabasesPerPool"];
        
        if (string.IsNullOrWhiteSpace(maxDatabasesPerPool))
        {
            throw new Exception("MaxDatabasesPerPool must be provided in the configuration file");
        }
        
        _maxDatabasesPerPool = Convert.ToInt32(maxDatabasesPerPool);
    }
    
    private void CreateAzureConfiguration(IConfiguration configuration)
    {
        _subscriptionId = configuration["SubscriptionId"];
        _resourceGroupName = configuration["ResourceGroupName"];
        _sqlServerName = configuration["ServerName"];
        
        if (string.IsNullOrWhiteSpace(_subscriptionId) || string.IsNullOrWhiteSpace(_resourceGroupName) || string.IsNullOrWhiteSpace(_sqlServerName))
        {
            throw new Exception("SubscriptionId, ResourceGroupName, and ServerName must be provided in the configuration file");
        }
    }
    
    private void CreateSku(IConfiguration configuration)
    {
        string? capacity = configuration["ElasticPoolSettings:Sku:Capacity"];
        string? tier = configuration["ElasticPoolSettings:Sku:Tier"];
        string? name = configuration["ElasticPoolSettings:Sku:Name"];
        
        if (string.IsNullOrWhiteSpace(capacity) || string.IsNullOrWhiteSpace(tier) || string.IsNullOrWhiteSpace(name))
        {
            throw new Exception("Capacity, Tier and Name must be provided for the Sku in the configuration file");
        }
        _sqlSku = new SqlSku(name)
        {
            Capacity = Convert.ToInt32(capacity),
            Tier = tier
        };
    }
    
    private void CreatePerDatabaseSettings(IConfiguration configuration)
    {
        string? maxCapacity = configuration["ElasticPoolSettings:PerDatabaseSettings:MaxCapacity"];
        string? minCapacity = configuration["ElasticPoolSettings:PerDatabaseSettings:MinCapacity"];

        if (string.IsNullOrWhiteSpace(maxCapacity) || string.IsNullOrWhiteSpace(minCapacity))
        {
            throw new Exception("MaxCapacity and MinCapacity must be provided for the PerDataBaseSettings in the configuration file");
        }
        
        _perDatabaseSettings = new ElasticPoolPerDatabaseSettings
        {
            MaxCapacity = Convert.ToInt32(maxCapacity),
            MinCapacity = Convert.ToInt32(minCapacity) 
        };
    }
    
    private async Task<SubscriptionResource> GetSubscriptionResource()
    {
        var subscription = await new ArmClient(new DefaultAzureCredential()).GetSubscriptionResource(new ResourceIdentifier($"/subscriptions/{_subscriptionId}")).GetAsync();
        
        if(!subscription.HasValue)
        {
            throw new Exception("Subscription not found");
        }
        
        return subscription.Value;
    }
    
    private async Task<ResourceGroupResource> GetResourceGroupResource(SubscriptionResource subscription)
    {
        var resourceGroup = await subscription.GetResourceGroups().GetAsync(_resourceGroupName);
        
        if(!resourceGroup.HasValue)
        {
            throw new Exception("Resource group not found");
        }

        return resourceGroup.Value;
    }
    
    private async Task<string> CreateUserForDatabase(string databaseName)
    {
        var userName = $"User_{Guid.NewGuid()}".Replace("-", "_");
        var userPassword = Guid.NewGuid().ToString().Replace("-", "_");
        
        await using var masterConnection = new SqlConnection(_connectionString);
        await masterConnection.OpenAsync();

        var createLoginQuery = $"""
                                    CREATE LOGIN {userName} WITH PASSWORD = '{userPassword}';
                                """;

        await using var createLoginCommand = new SqlCommand(createLoginQuery, masterConnection);
        await createLoginCommand.ExecuteNonQueryAsync();

        string? dbConnectionString = _connectionString?.Replace("Initial Catalog=master", $"Initial Catalog={databaseName}");
        await using var dbConnection = new SqlConnection(dbConnectionString);
        await dbConnection.OpenAsync();

        string createUserQuery = $"""
                                      CREATE USER {userName} FOR LOGIN {userName};
                                      ALTER ROLE db_datareader ADD MEMBER {userName};
                                      ALTER ROLE db_datawriter ADD MEMBER {userName};
                                  """;

        await using var createUserCommand = new SqlCommand(createUserQuery, dbConnection);
        await createUserCommand.ExecuteNonQueryAsync();

        return $"Server=tcp:{dbConnection.DataSource},1433;Initial Catalog={databaseName};Persist Security Info=False;User ID={userName};Password={userPassword};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
    }

    private async Task<SqlServerResource> GetSqlServerResource(ResourceGroupResource resourceGroup)
    {
        var sqlServer = await resourceGroup.GetSqlServers().GetAsync(_sqlServerName);
        
        if(!sqlServer.HasValue)
        {
            throw new Exception("SQL server not found");
        }

        return sqlServer.Value;
    }
    
    private async Task<ElasticPoolResource> GetTargetElasticPool(SqlServerResource sqlServer)
    {
        ElasticPoolCollection? elasticPools = sqlServer.GetElasticPools();
        
        ElasticPoolResource? targetPool = null;

        await foreach (ElasticPoolResource? elasticPool in elasticPools)
        {
            var databases = elasticPool.GetDatabases();
            if (databases.Count() >= _maxDatabasesPerPool) continue;
            targetPool = elasticPool;
            break;
        }

        if (targetPool is null)
        {
            var newPoolName = $"ElasticPool-{Guid.NewGuid()}";
            
            Console.WriteLine($"Creating a new elastic pool: {newPoolName}");
            
            await sqlServer.GetElasticPools().CreateOrUpdateAsync(WaitUntil.Completed, newPoolName, new ElasticPoolData(AzureLocation.UKSouth)
            {
                PerDatabaseSettings = _perDatabaseSettings,
                Sku = _sqlSku
            });
            
            Console.WriteLine($"Elastic pool '{newPoolName}' created successfully.");
            
            targetPool = await sqlServer.GetElasticPools().GetAsync(newPoolName);
        }
        if (targetPool is null) throw new Exception("Failed to create a new elastic pool");
        
        return targetPool;
    }
    
    public async Task CreateDatabaseAsync(int portalId, string portalName)
    {
        SubscriptionResource subscription = await GetSubscriptionResource();
        
        ResourceGroupResource resourceGroup = await GetResourceGroupResource(subscription);
        
        SqlServerResource sqlServer = await GetSqlServerResource(resourceGroup);
        
        ElasticPoolResource targetPool = await GetTargetElasticPool(sqlServer);
        
        var databaseName = $"DB_{portalId}_{portalName.Trim().Replace(" ", "_")}";
        
        Console.WriteLine($"Creating database '{databaseName}' in pool '{targetPool.Data.Name}'");
        
        var existingDatabase = await sqlServer.GetSqlDatabases().GetIfExistsAsync(databaseName);
        
        if (existingDatabase.HasValue)
        {
            Console.WriteLine($"Database '{databaseName}' already exists.");
            return;
        }
        
        await sqlServer.GetSqlDatabases().CreateOrUpdateAsync(WaitUntil.Completed, databaseName, new SqlDatabaseData(AzureLocation.UKSouth)
        {
            ElasticPoolId = targetPool.Id
        });
        
        string connectionString = await CreateUserForDatabase(databaseName);

        Console.WriteLine($"Database '{databaseName}' created successfully. Connection string: {connectionString}");
    }
}