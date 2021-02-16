CREATE PROCEDURE procfwkHelpers.ImportBatchStageLinkFromJson
(
  @json NVARCHAR(MAX)
 ,@deleteItemsNotInJson BIT = 0
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

    DROP TABLE IF EXISTS #batchStageLink;
    DROP TABLE IF EXISTS #outputActions;
    CREATE TABLE #outputActions (mergeAction VARCHAR(20));

    SELECT b.BatchId
          ,s.Value AS StageName
          ,(s.[Key] + 1) * 10 AS RunOrder
    INTO #batchStageLink
    FROM
      OpenJson( Json_Query( @json, '$.batches' ) )
      WITH (
        BatchId UNIQUEIDENTIFIER '$.id'
       ,stages NVARCHAR(MAX) AS JSON
      ) AS b
    OUTER APPLY OpenJson( Json_Query( b.stages, '$' ) ) AS s
  
    -- Merge BatchStageLink
    MERGE INTO procfwk.BatchStageLink AS t
    USING (SELECT b.BatchId
                 ,s.StageId
                 ,bsl.RunOrder
           FROM #batchStageLink AS bsl
           LEFT JOIN procfwk.Batches AS b
             ON b.BatchId = bsl.BatchId
           LEFT JOIN procfwk.Stages AS s
             ON s.StageName = bsl.StageName
          )AS S
    ON t.BatchId = s.BatchId
       AND t.StageId = s.StageId
    WHEN NOT MATCHED BY TARGET
      THEN INSERT (BatchId, StageId, RunOrder)
           VALUES (s.BatchId, s.StageId, s.RunOrder)
    WHEN MATCHED
         AND IsNull(t.RunOrder, 0) <> IsNull(s.RunOrder, 0)
      THEN UPDATE 
             SET RunOrder = s.RunOrder
    WHEN NOT MATCHED BY SOURCE
         AND @deleteItemsNotInJson = 1
      THEN DELETE
    OUTPUT $action INTO #outputActions;

    DECLARE @sourceRows INT = (SELECT Count(*) FROM #batchStageLink)
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