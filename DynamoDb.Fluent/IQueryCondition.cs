namespace DynamoDb.Fluent
{
    public interface IQueryCondition<T> where T : class, new()
    {
        IObjectQuery<T> Equal(object value);
        IObjectQuery<T> LessThanOrEqual(object value);
        IObjectQuery<T> LessThan(object value);
        IObjectQuery<T> GreaterThanOrEqual(object value);
        IObjectQuery<T> GreaterThan(object value);
        IObjectQuery<T> BeginsWith(string value);
        IObjectQuery<T> Between(object value1, object value2);
    }
}