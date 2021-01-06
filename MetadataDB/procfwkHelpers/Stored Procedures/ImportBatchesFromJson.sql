CREATE PROCEDURE procfwkHelpers.ImportBatchesFromJson
(
  @json NVARCHAR(MAX)
 ,@dropExisting BIT = 0
)
AS
BEGIN
  SET XACT_ABORT, NOCOUNT ON;

  PRINT Replicate(Char(10), 2) + Replicate('-', 180);
  PRINT 'Running proc: ' + (Object_Schema_Name(@@procId) + '.' + Object_Name(@@procId));
  PRINT Replicate('-', 180);

  BEGIN TRANSACTION;
  BEGIN TRY
    IF IsJson(@json) = 0
      RAISERROR('The json is not valid', 16, 1);

    DROP TABLE IF EXISTS #batches;
    DROP TABLE IF EXISTS #outputActions;
    CREATE TABLE #outputActions (mergeAction VARCHAR(20));
    
    SELECT *
    INTO #batches
    FROM
      OpenJson( Json_Query( @json, '$.batches' ) )
      WITH (
        BatchId UNIQUEIDENTIFIER '$.id'
       ,BatchName [varchar] (255) '$.name'
       ,BatchDescription [varchar] (4000) '$.description'
       ,Enabled bit '$.enabled'
      );
    
    -- Merge batches
    MERGE INTO procfwk.Batches AS t
    USING (SELECT *
           FROM #batches
          )AS s
    ON t.BatchId = s.BatchId
    WHEN NOT MATCHED BY TARGET
      THEN INSERT (BatchId, BatchName, BatchDescription, Enabled)
           VALUES (s.BatchId, s.BatchName, s.BatchDescription, s.Enabled)
    WHEN MATCHED
         AND (
               IsNull(t.BatchName, '') <> IsNull(s.BatchName, '')
               OR IsNull(t.BatchDescription, '') <> IsNull(s.BatchDescription, '')
               OR IsNull(t.Enabled, '') <> IsNull(s.Enabled, '')
             )
      THEN UPDATE 
             SET BatchName = s.BatchName
                ,BatchDescription = s.BatchDescription
                ,Enabled = s.Enabled
    WHEN NOT MATCHED BY SOURCE
         AND @dropExisting = 1
      THEN DELETE
    OUTPUT $action INTO #outputActions;

    DECLARE @sourceRows INT = (SELECT Count(*) FROM #batches)
           ,@insertedRows INT = (SELECT Count(*) FROM #outputActions WHERE mergeAction = 'INSERT')
           ,@updatedRows INT = (SELECT Count(*) FROM #outputActions WHERE mergeAction = 'UPDATE')
           ,@deletedRows INT = (SELECT Count(*) FROM #outputActions WHERE mergeAction = 'DELETE');

    PRINT '  Source rows:   ' + Cast(@sourceRows AS VARCHAR(10));
    PRINT '  Inserted rows: ' + Cast(@insertedRows AS VARCHAR(10));
    PRINT '  Updated rows:  ' + Cast(@updatedRows AS VARCHAR(10));
    PRINT '  Deleted rows:  ' + Cast(@deletedRows AS VARCHAR(10))

    COMMIT;
  END TRY

  -- Rollback changes and throw to caller on an error
  BEGIN CATCH
    IF @@tranCount > 0
      ROLLBACK;
    
    THROW;
  END CATCH
END