using System;
using System.Text.RegularExpressions;

namespace DynamoDb.Fluent.Tests.Utility
{
    public class ShortGuid
    {
        public ShortGuid()
        {
        }

        private ShortGuid(string value)
        {
            Value = Regex.Replace(value, "[/+=]", "");
        }

        private string Value { get; }

        public static ShortGuid NewId()
        {
            return new ShortGuid(Convert.ToBase64String(Guid.NewGuid().ToByteArray()));
        }

        public static implicit operator string(ShortGuid guid)
        {
            return guid.Value;
        }

        public static implicit operator ShortGuid(string value)
        {
            return new ShortGuid(value);
        }
    }
}