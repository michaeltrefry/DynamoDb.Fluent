

using DynamoDb.Fluent.Tests.Entities;

namespace DynamoDb.Fluent.Tests.Contexts
{
    public interface IMyDynamoContext : IDynamoDbContext
    {
        ITable<User> Users { get; }
        ITable<OwnedEntity> OwnedEntities { get; }

        ITable<T> Entities<T>() where T : class, new();
    }
}