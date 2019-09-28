namespace DynamoDb.Fluent
{
    public interface IDynamoDbContext
    {
        ITable<T> GetTable<T>(string tableName) where T : class, new();
    }
}