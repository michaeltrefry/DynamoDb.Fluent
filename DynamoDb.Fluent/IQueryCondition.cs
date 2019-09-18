using System.Collections.Generic;

namespace DynamoDb.Fluent
{
    public interface IQueryCondition<T>
    {
        IObjectQuery<T> Equal(object value);
        IObjectQuery<T> LessThanOrEqual(object value);
        IObjectQuery<T> LessThan(object value);
        IObjectQuery<T> GreaterThanOrEqual(object value);
        IObjectQuery<T> GreaterThan(object value);
        IObjectQuery<T> BeginsWith(string value);
        IObjectQuery<T> Between(object value1, object value2);
    }

    public interface IScanCondition<T> : IQueryCondition<T>
    {
        IObjectQuery<T> NotEqual(object value);
        IObjectQuery<T> IsNotNull();
        IObjectQuery<T> IsNull();
        IObjectQuery<T> Contains(object value);
        IObjectQuery<T> NotContains(object value);
        IObjectQuery<T> In(IEnumerable<object> values);
    }
}