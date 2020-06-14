// --------------------------------------------------------------------------------------------------------------------
// <copyright file="Program.cs" company="Madeira Ltd.">
//   Copyright (c) Madeira Data Solutions. All rights reserved.
// </copyright>
// --------------------------------------------------------------------------------------------------------------------

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

namespace Microsoft.Cyber.CyberSerialization
{
    class Program
    {
        public static IConfigurationRoot configuration;
        static DataTable CreateTable(string[] fieldsList)
        {
            // TODO: Make column list dynamic based on fieldsToCopy
            DataTable dt = new DataTable();
            foreach (string fieldName in fieldsList)
            {
                dt.Columns.Add(fieldName, typeof(string));
            }
            return dt;
        }

        static void Main(string[] args)
        {
            Console.Clear();
            MainAsync(args).Wait();
        }

        static async Task MainAsync(string[] args)
        {
            // Create service collection
            ServiceCollection serviceCollection = new ServiceCollection();
            ConfigureServices(serviceCollection);

            // Create service provider
            IServiceProvider serviceProvider = serviceCollection.BuildServiceProvider();

            string sourceEndPoint = configuration["CosmosDB:sourceEndPoint"]; // "https://<cosmosdbaccount>.documents.azure.com:443/";
            string sourceAuthKey = configuration["CosmosDB:sourceAuthKey"]; // "AccessKey";
            string sourceDatabase = configuration["CosmosDB:sourceDatabase"]; // "CosmosDbDatabase";
            string sourceCollection = configuration["CosmosDB:sourceCollection"]; // "CosmosDbCollection";
            string sourceQuery = configuration["CosmosDB:sourceQuery"]; // "SELECT c.Field1, c.Field2, c.Filed3, ... FROM c";
            int rowsPerFetch = int.Parse(configuration["CosmosDB:sourceMaxCountPerFetch"]); // 1000;

            string targetHost = configuration["SQLServer:targetHost"]; // "<SqlServerName>.database.windows.net";
            string targetDatabase = configuration["SQLServer:targetDatabase"]; //"SqlDatabaseName";
            string targetUsername = configuration["SQLServer:targetUsername"]; //"SqlUsername";
            string targetPassword = configuration["SQLServer:targetPassword"]; //"SqlPassword";
            string? targetTable = configuration["SQLServer:targetTable"]; // "SqlTargetStagingTable";
            bool truncateTargetTable = bool.Parse(configuration["SQLServer:truncateTargetTable"]); // true
            string? targetProcedure = configuration["SQLServer:targetProcedureTVP"]; // "SqlStoredProcedureWithTVP";
            string? mergeProcedure = configuration["SQLServer:mergeProcedure"]; // "SqlStoredProcedureForMerge";
            int rowsPerChunk = int.Parse(configuration["SQLServer:rowsPerChunk"]); // 5000;
            bool useBulkCopy = bool.Parse(configuration["SQLServer:useBulkCopy"]); // true;
            int maxRetries = int.Parse(configuration["SQLServer:maxRetries"]); // 10;
            int delaySecondsBetweenRetries = int.Parse(configuration["SQLServer:delaySecondsBetweenRetries"]); // 30;
            string[] fieldsToCopy = configuration.GetSection("FieldsToCopy").Get<string[]>();

            #region Settings Validation
            
            if (String.IsNullOrEmpty(targetProcedure) && !useBulkCopy)
            {
                throw new Exception("targetProcedure must be specified when BulkCopy is not used");
            }

            if (String.IsNullOrEmpty(targetTable) && useBulkCopy)
            {
                throw new Exception("targetTable must be specified when using BulkCopy");
            }

            #endregion

            #region Connect to CosmosDB

            DocumentClient client = new DocumentClient(
                new Uri(sourceEndPoint),
                sourceAuthKey,
                new ConnectionPolicy()
                {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp,
                    RetryOptions = new RetryOptions()
                    {
                        MaxRetryAttemptsOnThrottledRequests = 10,
                        MaxRetryWaitTimeInSeconds = 30
                    }
                });

            Uri sourceDatabaseUri = UriFactory.CreateDatabaseUri(sourceDatabase);

            DocumentCollection customSourceContainer = client.CreateDocumentCollectionQuery(sourceDatabaseUri)
                .Where(c => c.Id == sourceCollection).AsEnumerable().FirstOrDefault();

            FeedOptions option = new FeedOptions
            {
                EnableCrossPartitionQuery = true,
                MaxItemCount = rowsPerFetch,
                MaxBufferedItemCount = rowsPerChunk,
                EnableScanInQuery = true
            };

            #endregion

            #region Migration Process

            DataTable dataTable = CreateTable(fieldsToCopy);
            int i = 0;
            int errorsCount = 0;
            using (var queryable = client.CreateDocumentQuery(customSourceContainer.SelfLink, sourceQuery, option).AsDocumentQuery())
            {

                #region Connect to Target SQL Server

                SqlConnection sqlConnection = new SqlConnection(
                    string.Format("Data Source={0};Initial Catalog={1};User ID={2};Password={3}",
                    targetHost, targetDatabase, targetUsername, targetPassword));
                sqlConnection.Open();
                sqlConnection.InfoMessage += SqlConnection_InfoMessage;

                // Truncate staging table
                if (truncateTargetTable && !String.IsNullOrEmpty(targetTable))
                {
                    using (SqlCommand cmd = sqlConnection.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = String.Format("TRUNCATE TABLE {0}", targetTable);

                        Console.WriteLine("{1} {2} Truncating {0}...", targetTable, DateTime.Now.ToShortDateString(), DateTime.Now.ToLongTimeString());

                        cmd.ExecuteNonQuery();
                    }
                }

                #endregion

                Console.WriteLine("{2} {3} Starting process (chunk size: {0}, fetch size: {1})", rowsPerChunk, rowsPerFetch, DateTime.Now.ToShortDateString(), DateTime.Now.ToLongTimeString());

                while (queryable.HasMoreResults)
                {
                    Console.WriteLine("{1} {2} Buffered: {0} (decompressing)", dataTable.Rows.Count, DateTime.Now.ToShortDateString(), DateTime.Now.ToLongTimeString());

                    foreach (dynamic item in await queryable.ExecuteNextAsync())
                    {
                        // Iterate through items
                        i++;
                        insertion_start:

                        // Retry connection if necessary
                        if (sqlConnection == null || sqlConnection.State != ConnectionState.Open)
                        {
                            if (sqlConnection != null) sqlConnection.Dispose();

                            Thread.Sleep(delaySecondsBetweenRetries * 1000);

                            sqlConnection = new SqlConnection(
                                                string.Format("Data Source={0};Initial Catalog={1};User ID={2};Password={3}",
                                                targetHost, targetDatabase, targetUsername, targetPassword));
                            sqlConnection.Open();
                            sqlConnection.InfoMessage += SqlConnection_InfoMessage;
                        }

                        try
                        {
                            // Convert Dynamic Object into a Dictionary
                            IDictionary<string, object> d = (IDictionary<string, object>)item;

                            // Construct a new DataRow
                            DataRow newRow = dataTable.NewRow();

                            // If fields list was provided, use that
                            if (fieldsToCopy.Length > 0)
                            {
                                foreach (string field in fieldsToCopy)
                                {
                                    if (d.ContainsKey(field) && d[field] != null)
                                    {
                                        newRow.SetField<string>(field, d[field].ToString());
                                    }
                                    else
                                    {
                                        newRow.SetField<string>(field, String.Empty);
                                    }
                                }
                            }
                            // Otherwise, try to generate row fields based on query fields returned
                            else
                            {
                                foreach (string field in d.Keys)
                                {
                                    newRow.SetField<string>(field, d[field].ToString());
                                }
                            }

                            // Add the new row in the DataTable
                            dataTable.Rows.Add(newRow);

                            // Check if should flush staging data using SqlBulkCopy
                            if (useBulkCopy && dataTable.Rows.Count >= rowsPerChunk)
                            {
                                insertBulk(sqlConnection, targetTable, dataTable);
                                dataTable.Clear();

                                mergeStaging(sqlConnection, mergeProcedure);
                                i = 0;
                            }
                            else if (!useBulkCopy)
                            {
                                // Check if should flush staging data using Table-Valued-Parameters
                                // TVP contents, as best practice, should not exceed 1000 rows
                                if ((dataTable.Rows.Count % 1000) == 0 || dataTable.Rows.Count >= rowsPerChunk)
                                {
                                    insertChunk(sqlConnection, targetProcedure, dataTable);

                                    if (i >= rowsPerChunk)
                                    {
                                        mergeStaging(sqlConnection, mergeProcedure);
                                        i = 0;
                                    }

                                    dataTable.Clear();
                                }
                            }

                            if (errorsCount > 0)
                            {
                                errorsCount = 0;
                            }
                        }
                        catch (Exception e)
                        {
                            if (errorsCount < maxRetries)
                            {
                                Console.WriteLine("{3} {4} ERROR (attempt {0} out of {1}): {2}", errorsCount, maxRetries, e.Message, DateTime.Now.ToShortDateString(), DateTime.Now.ToLongTimeString());
                                errorsCount++;

                                if (sqlConnection.State == ConnectionState.Open)
                                {
                                    sqlConnection.Close();
                                }

                                goto insertion_start;
                            }
                            else
                            {
                                Console.WriteLine();
                                Console.WriteLine("Last item:");
                                Console.WriteLine("===============================================");
                                Console.WriteLine(item);
                                Console.WriteLine("===============================================");
                                Console.WriteLine();
                                throw e;
                            }
                        }
                    }
                }

                // If more data remained in buffer object

                Console.WriteLine("{1} {2} Buffered: {0} (finalizing)", dataTable.Rows.Count, DateTime.Now.ToShortDateString(), DateTime.Now.ToLongTimeString());

                // Check if should flush staging data using SqlBulkCopy
                if (useBulkCopy && dataTable.Rows.Count > 0)
                {
                    insertBulk(sqlConnection, targetTable, dataTable);
                }
                // Check if should flush staging data using Table-Valued-Parameters
                else if (dataTable.Rows.Count > 0)
                {
                    insertChunk(sqlConnection, targetProcedure, dataTable);
                }

                dataTable.Clear();
                mergeStaging(sqlConnection, mergeProcedure);

                Console.WriteLine("{0} {1} Migration is complete!", DateTime.Now.ToShortDateString(), DateTime.Now.ToLongTimeString());
                sqlConnection.Close();
            }

            #endregion
        }

        private static void SqlConnection_InfoMessage(object sender, SqlInfoMessageEventArgs e)
        {
            foreach (SqlError item in e.Errors)
            {
                if (item.Class >= 1 && item.Class <= 9)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("{5} {6} Warning {0} Severity {1} Line {2} ({3}): {4}", item.Number, item.Class, item.LineNumber, item.Procedure, item.Message, DateTime.Now.ToShortDateString(), DateTime.Now.ToLongTimeString());
                    Console.ResetColor();
                }
                else if (item.Class > 10)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("{5} {6} Error {0} Severity {1} Line {2} ({3}): {4}", item.Number, item.Class, item.LineNumber, item.Procedure, item.Message, DateTime.Now.ToShortDateString(), DateTime.Now.ToLongTimeString());
                    Console.ResetColor();
                }
                else
                {
                    Console.WriteLine("{1} {2} {0}", item.Message, DateTime.Now.ToShortDateString(), DateTime.Now.ToLongTimeString());
                }
            }
        }

        private static void insertChunk(SqlConnection sqlConnection, string targetProcedure, DataTable dataTable)
        {
            Console.WriteLine("{1} {2} Flushing {0} row(s) using TVP...", dataTable.Rows.Count, DateTime.Now.ToShortDateString(), DateTime.Now.ToLongTimeString());
            using (SqlCommand cmd = sqlConnection.CreateCommand())
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = targetProcedure;
                cmd.CommandTimeout = 3600000;
                System.Data.SqlClient.SqlParameter sqlParam = cmd.Parameters.AddWithValue("@Profiles", dataTable);
                sqlParam.SqlDbType = SqlDbType.Structured;
                cmd.ExecuteNonQuery();
            }
        }

        private static void mergeStaging(SqlConnection sqlConnection, string? mergeProcedure)
        {
            if (!String.IsNullOrEmpty(mergeProcedure))
            {
                Console.WriteLine("{0} {1} Merging...", DateTime.Now.ToShortDateString(), DateTime.Now.ToLongTimeString());
                using (SqlCommand cmd = sqlConnection.CreateCommand())
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = mergeProcedure;
                    cmd.CommandTimeout = 3600000;
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private static void insertBulk(SqlConnection sqlConnection, string targetTable, DataTable dataTable)
        {
            Console.WriteLine("{1} {2} Flushing {0} row(s) using BulkCopy...", dataTable.Rows.Count, DateTime.Now.ToShortDateString(), DateTime.Now.ToLongTimeString());

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConnection))
            {
                bulkCopy.DestinationTableName = targetTable;
                bulkCopy.BulkCopyTimeout = 3600000;
                bulkCopy.BatchSize = 1000;

                try
                {
                    // Write from the source to the destination.
                    bulkCopy.WriteToServer(dataTable);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

        private static void ConfigureServices(IServiceCollection serviceCollection)
        {

            // Build configuration
            configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", false)
                .Build();

            // Add access to generic IConfigurationRoot
            serviceCollection.AddSingleton<IConfigurationRoot>(configuration);

            // Add app
            serviceCollection.AddTransient<Program>();
        }
    }
}
