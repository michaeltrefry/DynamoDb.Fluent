using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Threading.Tasks;

namespace DynamoDb.Fluent.Tests
{
    [TestClass]
    public class DynamoTests
    {
        private DynamoDbConfiguration _dynamoConfig;
        
        [TestInitialize]
        public void Initialize()
        {
            _dynamoConfig = new DynamoDbConfiguration()
            {
                Region = "us-east-1"
            };
        }
        
        [TestMethod]
        public async Task TestMethod1()
        {
            var context = new MyDynamoContext(_dynamoConfig);

            var item = await context.Users.Find("V4oLCBgZUsPod3Iluscw","User");

            Assert.IsNotNull(item);
        }
        
    }
}