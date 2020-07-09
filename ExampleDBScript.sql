
CREATE TABLE dbo.SqlTargetTable (
    Field1      VARCHAR (100) NOT NULL PRIMARY KEY CLUSTERED,
    Field2	VARCHAR (MAX),
    Field3	VARCHAR (MAX)
);
GO

CREATE TABLE dbo.SqlTargetStagingTable (
    Field1      VARCHAR (100),
    Field2	VARCHAR (MAX),
    Field3	VARCHAR (MAX)
);
GO
CREATE TYPE dbo.tvpBatchData AS TABLE (
    Field1      VARCHAR (100),
    Field2	VARCHAR (MAX),
    Field3	VARCHAR (MAX)
);

GO
CREATE PROCEDURE dbo.SqlStoredProcedureWithTVP
	@Batch AS dbo.tvpBatchData READONLY
AS
BEGIN
SET NOCOUNT ON;

INSERT INTO dbo.SqlTargetStagingTable
SELECT * FROM @Batch;

END
GO
CREATE PROCEDURE dbo.SqlMergeFromStagingTable
(
    @TruncateStagingOnSuccess BIT = 0,
    @Verbose BIT = 0
)
AS
BEGIN
SET NOCOUNT, XACT_ABORT, ARITHABORT, QUOTED_IDENTIFIER ON;
DECLARE @MergedCount INT;

MERGE INTO dbo.SqlTargetTable AS Trgt
USING dbo.SqlTargetStagingTable AS Src
ON Trgt.Field1 = Src.Field1
WHEN NOT MATCHED BY TARGET THEN
	INSERT
	(Field1, Field2, Field3)
	VALUES
	(Field1, Field2, Field3)
WHEN MATCHED THEN
	UPDATE SET
		Field2 = Src.Field2,
		Field3 = Src.Field3
;
SET @MergedCount = @@ROWCOUNT;
IF @Verbose = 1 RAISERROR(N'Affected %d Record(s)',0,1,@MergedCount) WITH NOWAIT;

IF @TruncateStagingOnSuccess = 1
BEGIN
	TRUNCATE TABLE dbo.SqlTargetStagingTable;
	IF @Verbose = 1 PRINT N'Truncated staging table';
END
END
