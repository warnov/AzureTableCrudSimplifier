using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using System.Linq;
using System.Net;

namespace WarNov.AzureTableCrudSimplifier
{
    public class TableOperator
    {
        readonly CloudStorageAccount _account;
        readonly CloudTableClient _tableClient;
        CloudTable _table;
        string _tableName;


        /// <summary>
        /// The Name of the table the TableOperator will be working with
        /// </summary>
        public string TableName
        {
            get
            {
                return _tableName;
            }
            set
            {
                _tableName = value;
                _table = _tableClient.GetTableReference(TableName);
            }
        }

        /// <summary>
        /// Initialize the Table Operator
        /// </summary>
        /// <param name="azureStorageConnectionString"></param>
        /// <param name="tableName"></param>
        public TableOperator(string azureStorageConnectionString, string tableName = "")
        {
            _account = CloudStorageAccount.Parse(azureStorageConnectionString);
            _tableClient = _account.CreateCloudTableClient();
            _tableName = tableName;
            _table = _tableClient.GetTableReference(_tableName);
        }

        /// <summary>
        /// Returns a unique register from the specified Azure Table, given the PK, and RK. This method is generic and brings a typed registry
        /// </summary>        /// <typeparam name="T">The type of the registry to bring from Azure</typeparam>      
        /// <param name="pk">Registry's PK</param>
        /// <param name="rk">Registry's RK</param>
        /// <returns>The registry, or default value for the class if it doesn't exist</returns>
        public T UniqueRecordFromTable<T>(string pk, string rk) where T : TableEntity, new()
        {
            var pkCondition = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk);
            var rkCondition = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rk);
            var combinedFilters = TableQuery.CombineFilters(
                pkCondition,
                TableOperators.And,
                rkCondition);
            var query = new TableQuery<T>().Where(combinedFilters);
            var conToken = new TableContinuationToken();
            IEnumerable<T> virtualResults =
                       _table.ExecuteQuerySegmentedAsync<T>(query, conToken).Result.ToList();
            return virtualResults.ToList().FirstOrDefault();
        }

        /// <summary>
        /// Inserts or merge an entity into the specified table.
        /// </summary>
        /// <typeparam name="T">The type of the entity to insert (must derive from TableEntity)</typeparam>       
        /// <param name="entity">The entity to be inserted</param>
        /// <returns>True if the insert or merge operation can be done.</returns>
        public bool InsertOrMergeEntity<T>(T entity) where T : TableEntity, new()
        {
            TableOperation tableOperation = TableOperation.InsertOrMerge(entity);
            var result = _table.ExecuteAsync(tableOperation).Result;
            return result.HttpStatusCode == (int)HttpStatusCode.NoContent;
        }

        /// <summary>
        /// Inserts or merge a set of entities into the specified table within a single tyransaction (if all the entities to be inserted have the same PK).
        /// </summary>
        /// <typeparam name="T">The type of entities to be inserted</typeparam>    
        /// <param name="entities">The entities to be inserted</param>
        /// <returns>True if all the entities were succesfully inserted</returns>
        public bool InsertOrMergeBatchEntities<T>(List<T> entities) where T : TableEntity, new()
        {
            try
            {
                TableBatchOperation batchOperation = new TableBatchOperation();
                foreach (var entity in entities)
                {
                    batchOperation.InsertOrMerge(entity);
                }
                var results = _table.ExecuteBatchAsync(batchOperation).Result;
                return results[0].HttpStatusCode == (int)HttpStatusCode.NoContent;
            }
            catch
            {
                return false;
            }
        }
    }
}
