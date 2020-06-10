# CosmosDb-to-MSSQL-Migration

This is a simple command-line program that migrates data from Azure CosmosDB to a Microsoft SQL Server database.

In this page:

- [Arguments](#arguments)
  - [CosmosDB](#cosmosdb)
  - [SQLServer](#sqlserver)
  - [FieldsToCopy](#fieldstocopy)
- [Copying using Bulk Copy](#copying-using-bulk-copy)
- [Copying using Table-Valued-Parameters](#copying-using-table-valued-parameters)
- [Permissions](#permissions)
- [Remarks](#remarks)
- [See Also](#see-also)

# Arguments

Edit the `appsettings.json` file to specify the necessary parameters, as described below.

## CosmosDB

The `CosmosDB` section in the settings affects the retreival of data from your Cosmos DB source.

#### sourceEndPoint

`sourceEndPoint` is the end-point URI of your Cosmos DB account.
It can be found in the **Overview** page of your Cosmos DB account in the Azure portal.
It can also be found in the **Keys** page of your Cosmos DB account.

Example: `https://my-cosmosdb-account.documents.azure.com:443/`

#### sourceAuthKey

`sourceAuthKey` is either the **Primary Key** or **Secondary Key** of your Cosmos DB account.
You can find your keys in the **Keys** page of your Cosmos DB account in the Azure portal.

Example: `ASdiodhasjda2...Ajddndf==`

#### sourceDatabase

`sourceDatabase` is the source database name of your Cosmos DB account.

Your Cosmos DB databases can be found in the **Data Explorer** or **Browse** pages of your Cosmos DB account in the Azure portal.

#### sourceCollection

`sourceCollection` is the source collection name of your Cosmos DB account.

Your Cosmos DB collections can be found under each database in the **Data Explorer** or **Browse** pages of your Cosmos DB account in the Azure portal.

#### sourceQuery

`sourceQuery` is the SQL Query to be used for retrieving the relevant data from your Cosmos DB collection.

For example: `SELECT c.Field1, c.Field2, c.Field3 FROM c`

#### sourceMaxCountPerFetch

`sourceMaxCountPerFetch` is the maximum number of rows to fetch per each iteration from your Cosmos DB account.

It affects the number of maximum buffered items, and the maximum fetched items.

Choosing the right configuration number in this setting should be affected by your Cosmos DB configured throughput (based on RUs).

This can be affected by your Cosmos DB account scale, found in the **Scale** page of your Cosmos DB account in the Azure portal.

[Click to learn more](https://docs.microsoft.com/en-us/azure/cosmos-db/scaling-throughput) about Cosmos DB Scaling throughput.

## SQLServer

The `SQLServer` section in the settings affects the insertion of data into your target SQL Server database.

#### targetHost

`targetHost` is the host address of your SQL Server instance destination.

For example: `my-azure-sql-db.database.windows.net`

You can also specify an IP address. For example: `168.192.10.11`

If your SQL Server instance uses a TCP port other than 1433, it should be specified after a comma.

For example: `my-sql-server-host.acme.com,12345`

Specifying a named instance: `my-sql-server-host\myNamedInstance`

#### targetDatabase

`targetDatabase` is the name of your SQL Server database destination.

To find your list of databases in your SQL Server instance, you can run the following SQL query:

`SELECT name FROM sys.databases`

#### targetUsername

`targetUsername` is the SQL Authentication login name to be used for authenticating with your SQL Server.

#### targetPassword

`targetPassword` is the password of your SQL Authentication login name to be used for authenticating with your SQL Server.

#### rowsPerChunk

`rowsPerChunk` specifies the number of rows to fetch from the Cosmos DB source before inserting them as a chunk into the SQL Server destination.

#### mergeProcedure

`mergeProcedure` is a stored procedure to run after the insertion of each chunk of data into the SQL Server destination.

This parameter is **optional**. If you do not want such a procedure to be executed, please specify **null** as its value.

#### useBulkCopy

`useBulkCopy` is a **boolean** parameter specifying whether to use *SqlBulkCopy* or not.

If **true**, you must specify the `targetTable` parameter must be specified.

If **false**, will use a stored procedure with a Table-Valued-Parameter instead.
In such case, the `targetProcedureTVP` parameter must be specified.

#### targetTable

`targetTable` is the name of the table into which you want to save the data using SqlBulkCopy.

If `useBulkCopy` is **false**, the `targetTable` parameter is **optional**.

#### truncateTargetTable

`truncateTargetTable` is a **boolean** parameter specifying whether to truncate the `targetTable` staging table before beginning the migration process.

If `useBulkCopy` is **false**, the `truncateTargetTable` parameter is **optional**.

#### targetProcedureTVP

`targetProcedureTVP` is the name of a stored procedure that accepts a Table-Valued-Parameter as input.

This procedure must accept a table-valued-parameter with the fields matching the fields specified in the `FieldsToCopy` section.

If `useBulkCopy` is **true**, the `targetProcedureTVP` parameter is **optional**.

## FieldsToCopy

The `FieldsToCopy` parameter is a string array used for specifying the list of fields to copy from Cosmos DB to SQL Server.

If a specified field is not returned in the Cosmos DB results, it will be sent as an empty string instead.

Example:

```
  "FieldsToCopy": [
    "Field1",
    "Field2",
    "Field3"
  ]
```

# Copying using Bulk Copy

TBA

# Copying using Table-Valued-Parameters

TBA

# Permissions

TBA

# Remarks

TBA

# See Also

TBA