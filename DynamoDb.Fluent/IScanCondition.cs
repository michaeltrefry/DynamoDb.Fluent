using System.Collections.Generic;

namespace DynamoDb.Fluent
{
    public interface IScanCondition<T> : IQueryCondition<T> where T : class, new()
    {
        IObjectQuery<T> NotEqual(object value);
        IObjectQuery<T> IsNotNull();
        IObjectQuery<T> IsNull();
        IObjectQuery<T> Contains(object value);
        IObjectQuery<T> NotContains(object value);
        IObjectQuery<T> In(IEnumerable<object> values);
    }
}