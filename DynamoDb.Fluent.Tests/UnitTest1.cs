using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DynamoDb.Fluent.Tests
{
    [TestClass]
    public class UnitTest1
    {
        [TestInitialize]
        public void Initialize()
        {
            var config = new DynamoDbConfiguration()
            {
                
            };
        }
        [TestMethod]
        public void TestMethod1()
        {
        }
    }
}