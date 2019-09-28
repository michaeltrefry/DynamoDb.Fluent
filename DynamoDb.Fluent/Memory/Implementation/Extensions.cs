using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.DocumentModel;
using DynamoDb.Fluent.Memory.Definitions;
using Newtonsoft.Json.Linq;

namespace DynamoDb.Fluent.Memory.Implementation
{
    public static class Extensions
    {
        public static IEnumerable<JToken> FilterByConditions(this QueryOperation query, IEnumerable<JToken> tokens)
        {
            if (query.Conditions == null || !query.Conditions.Any()) return tokens;
            
            var newResults = new List<JToken>();
            foreach (var result in tokens)
            {
                var passes = true;
                foreach (var condition in query.Conditions)
                {
                    passes &= condition.Passes(result[condition.FieldName]?.Value<string>());
                }
                if (passes) newResults.Add(result);
            }
            return newResults;
        }
        
        public static bool Passes(this Condition condition, string value)
        {
            switch (condition.Operator)
            {
                case ScanOperator.Equal:
                    return value == condition.Value;
                case ScanOperator.NotEqual:
                    return value != condition.Value;
                case ScanOperator.BeginsWith:
                    return value.StartsWith(condition.Value);
                case ScanOperator.GreaterThan:
                    if (int.TryParse(value, out var val)
                        && int.TryParse(condition.Value, out var val1))
                        return val > val1;
                    return string.Compare(value, condition.Value, StringComparison.InvariantCultureIgnoreCase) > 0;
                case ScanOperator.LessThan:
                    if (int.TryParse(value, out val)
                        && int.TryParse(condition.Value, out val1))
                        return val < val1;
                    return string.Compare(value, condition.Value, StringComparison.InvariantCultureIgnoreCase) < 0;
                case ScanOperator.GreaterThanOrEqual:
                    if (int.TryParse(value, out val)
                        && int.TryParse(condition.Value, out val1))
                        return val >= val1;
                    return string.Compare(value, condition.Value, StringComparison.InvariantCultureIgnoreCase) >= 0;
                case ScanOperator.LessThanOrEqual:
                    if (int.TryParse(value, out val)
                        && int.TryParse(condition.Value, out val1))
                        return val <= val1;
                    return string.Compare(value, condition.Value, StringComparison.InvariantCultureIgnoreCase) <= 0;
                case ScanOperator.Between:
                    if (int.TryParse(value, out val)
                    && int.TryParse(condition.Value, out val1)
                    && int.TryParse(condition.Value2, out var val2))
                        return val > val1 && val < val2;
                    return string.Compare(value, condition.Value, StringComparison.InvariantCultureIgnoreCase) > 0
                           && string.Compare(value, condition.Value2, StringComparison.InvariantCultureIgnoreCase) < 0;
                case ScanOperator.In:
                    return ((IList) condition.Values).Contains(value);
                case ScanOperator.IsNull:
                    return value == null;
                case ScanOperator.IsNotNull:
                    return value != null;
                case ScanOperator.Contains:
                    throw new NotImplementedException();
                case ScanOperator.NotContains:
                    throw new NotImplementedException();
            }

            return false;
        }

        public static ItemPointer GetPointer(this TableDefinition definition, JToken token)
        {
            var hashValue = token[definition.HashKey.Name];
            if (hashValue == null)
                return null;
            
            var hashKey =  hashValue.Value<string>();
            string sortKey = null;
            
            if (definition.SortKey != null)
            {
                var sortValue = token[definition.SortKey.Name];
                if (sortValue != null)
                    sortKey = sortValue.Value<string>();
            }
            
            return new ItemPointer()
            {
                HashKey = hashKey,
                SortKey = sortKey
            };
        }
    }
}