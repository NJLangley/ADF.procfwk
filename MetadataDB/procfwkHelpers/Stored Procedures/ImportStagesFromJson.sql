CREATE PROCEDURE procfwkHelpers.ImportStagesFromJson
(
  @json NVARCHAR(MAX)
 ,@importIds BIT = 0
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

    DROP TABLE IF EXISTS #stages;
    DROP TABLE IF EXISTS #outputActions;
    CREATE TABLE #outputActions (mergeAction VARCHAR(20));

    SELECT *
    INTO #stages
    FROM
      OpenJson( Json_Query( @json, '$.stages' ) )
      WITH (
        StageName [varchar] (255) '$.name'
       ,StageDescription [varchar] (4000) '$.description'
       ,Enabled bit '$.enabled'
      );
    
    -- Merge stages
    MERGE INTO procfwk.Stages AS t
    USING (SELECT *
            FROM #stages
          )AS s
    ON t.StageName = s.StageName
    WHEN NOT MATCHED BY TARGET
      THEN INSERT (StageName, StageDescription, Enabled)
            VALUES (s.StageName, s.StageDescription, s.Enabled)
    WHEN MATCHED
          AND (
                IsNull(t.StageDescription, '') <> IsNull(s.StageDescription, '')
                OR IsNull(t.Enabled, '') <> IsNull(s.Enabled, '')
              )
      THEN UPDATE 
              SET StageDescription = s.StageDescription
                ,Enabled = s.Enabled
    WHEN NOT MATCHED BY SOURCE
          AND @dropExisting = 1
      THEN DELETE
    OUTPUT $action INTO #outputActions;

    DECLARE @sourceRows INT = (SELECT Count(*) FROM #stages)
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