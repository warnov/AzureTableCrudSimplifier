using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;

namespace WarNov.AzureTableCrudSimplifier
{
    public class TableOperator
    {
        public TableOperator(string azureStorageConnectionString)
        {
            StorageAccount = CloudStorageAccount.Parse(azureStorageConnectionString);
        }

        public CloudStorageAccount StorageAccount { get;  }

        /// <summary>
        /// Gets the cloud table with the specified name
        /// </summary>
        /// <param name="tableName">Table Name</param>
        /// <returns>The table</returns>
        public CloudTable GetTable(string tableName)
        {
            var tableClient = StorageAccount.CreateCloudTableClient();
            return tableClient.GetTableReference(tableName);
        }


        /// <summary>
        /// Returns a unique register from the specified Azure Table, given the PK, and RK. This method is generic and brings a typed registry
        /// </summary>
        /// <typeparam name="T">The type of the registry to bring from Azure</typeparam>
        /// <param name="tableName">The table with the registry</param>
        /// <param name="pk">Registry's PK</param>
        /// <param name="rk">Registry's RK</param>
        /// <returns>The registry, or default value for the class if it doesn't exist</returns>
        public T UniqueRecordFromTable<T>(string tableName, string pk, string rk) where T : TableEntity, new()
        {
            var table = GetTable(tableName);
            var pkCondition = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk);
            var rkCondition = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rk);
            var combinedFilters = TableQuery.CombineFilters(
                pkCondition,
                TableOperators.And,
                rkCondition);
            var query = new TableQuery<T>().Where(combinedFilters);
            var conToken = new TableContinuationToken();
            IEnumerable<T> virtualResults =
                       table.ExecuteQuerySegmentedAsync<T>(query, conToken).Result.ToList();
            return virtualResults.ToList().FirstOrDefault();
        }

        /// <summary>
        /// Inserts or merge an entity into the specified table.
        /// </summary>
        /// <typeparam name="T">The type of the entity to insert (must derive from TableEntity)</typeparam>
        /// <param name="tableName">The name of the table</param>
        /// <param name="entity">The entity to be inserted</param>
        /// <returns>True if the insert or merge opertation can be done.</returns>
        public bool InsertOrMergeEntity<T>(string tableName, T entity) where T : TableEntity, new()
        {
            var table = GetTable(tableName);
            TableOperation tableOperation = TableOperation.InsertOrMerge(entity);
            var result = table.ExecuteAsync(tableOperation).Result;
            return result.HttpStatusCode == (int)HttpStatusCode.NoContent;
        }

        /// <summary>
        /// Inserts or merge a set of entities into the specified table within a single tyransaction (if all the entities to be inserted have the same PK).
        /// </summary>
        /// <typeparam name="T">The type of entities to be inserted</typeparam>
        /// <param name="tableName">The name of the table in which the entities will beinserted</param>
        /// <param name="entities">The entities to be inserted</param>
        /// <returns>True if all the entities were succesfully inserted</returns>
        public bool InsertOrMergeBatchEntities<T>(string tableName, List<T> entities) where T : TableEntity, new()
        {
            try
            {
                TableBatchOperation batchOperation = new TableBatchOperation();
                var table = GetTable(tableName);
                foreach (var entity in entities)
                {
                    batchOperation.InsertOrMerge(entity);
                }
                var results = table.ExecuteBatchAsync(batchOperation).Result;
                return results[0].HttpStatusCode == (int)HttpStatusCode.NoContent;
            }
            catch
            {
                return false;
            }
        }
    }
}
