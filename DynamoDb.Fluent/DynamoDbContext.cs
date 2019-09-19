using System;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;

namespace DynamoDb.Fluent
{
    public class DynamoDbContext
    {
        
        private readonly AmazonDynamoDBClient client;
        public DynamoDbContext(DynamoDbConfiguration configuration)
        {
            if (!string.IsNullOrWhiteSpace(configuration.LocalUrl))
            {
                var clientConfig = new AmazonDynamoDBConfig {ServiceURL = configuration.LocalUrl};
                this.client = new AmazonDynamoDBClient(clientConfig);
            }
            else
            {
                var region = string.IsNullOrWhiteSpace(configuration.Region) ? null : RegionEndpoint.GetBySystemName(configuration.Region);
                this.client = configuration.Credentials == null
                    ? (region == null
                        ? new AmazonDynamoDBClient()
                        : new AmazonDynamoDBClient(region))
                    : (region == null
                        ? new AmazonDynamoDBClient(configuration.Credentials.AccessKey, configuration.Credentials.SecretKey)
                        : new AmazonDynamoDBClient(configuration.Credentials.AccessKey, configuration.Credentials.SecretKey, region));
            }
        }
        
        public ITable<T> GetTable<T>(string tableName)
        {
            return new DynamoDbTable<T>(client, tableName);
        }
    }
}