namespace DynamoDb.Fluent.Tests
{
    public class MyDynamoContext : DynamoDbContext
    {
        public MyDynamoContext(DynamoDbConfiguration configuration) : base(configuration)
        {
        }
        
        public ITable<User> Users => this.GetTable<User>("Galleries");
    }
}