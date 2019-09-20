

using DynamoDb.Fluent.Tests.Entities;

namespace DynamoDb.Fluent.Tests.Contexts
{
    public interface IMyDynamoContext : IDynamoDbContext
    {
        ITable<User> Users { get; }
        ITable<UserEntity> UserEntities { get; }
    }
}