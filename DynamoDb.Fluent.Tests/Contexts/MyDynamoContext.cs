using DynamoDb.Fluent.Dynamo;
using DynamoDb.Fluent.Tests.Entities;

namespace DynamoDb.Fluent.Tests.Contexts
{
    public class MyDynamoContext : DynamoDbContext, IMyDynamoContext
    {
        public MyDynamoContext() : base(new DynamoDbConfiguration() { Region = "us-east-1"})
        {
        }
        
        public ITable<User> Users => this.GetTable<User>("UnitTests");
        public ITable<OwnedEntity> OwnedEntities  => this.GetTable<OwnedEntity>("UnitTests");
        public ITable<T> Entities<T>() where T : class, new()
        {
            return this.GetTable<T>("UnitTests");
        }
    }
}