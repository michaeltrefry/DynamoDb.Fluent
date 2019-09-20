using DynamoDb.Fluent.Dynamo;
using DynamoDb.Fluent.Tests.Entities;

namespace DynamoDb.Fluent.Tests.Contexts
{
    public class MyDynamoContext : DynamoDbContext, IMyDynamoContext
    {
        public MyDynamoContext(DynamoDbConfiguration configuration) : base(configuration)
        {
        }
        
        public ITable<User> Users => this.GetTable<User>("Users");
        public ITable<UserEntity> UserEntities  => this.GetTable<UserEntity>("UserEntities");
    }
}