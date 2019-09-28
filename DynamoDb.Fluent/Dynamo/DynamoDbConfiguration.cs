namespace DynamoDb.Fluent.Dynamo
{
    public class DynamoDbConfiguration
    {
        public string Region { get; set; }
        public AmazonCredentials Credentials { get; set; }
        public string LocalUrl { get; set; }
    }
    
    public class AmazonCredentials
    {
        public string AccessKey { get; set; }
        public string SecretKey { get; set; }
    }
}