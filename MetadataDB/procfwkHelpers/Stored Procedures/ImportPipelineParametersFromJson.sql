CREATE PROCEDURE procfwkHelpers.ImportPipelineParametersFromJson
(
  @json NVARCHAR(MAX)
 ,@importIds BIT = 0
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

    DROP TABLE IF EXISTS #pipelineParameters;
    DROP TABLE IF EXISTS #outputActions;
    CREATE TABLE #outputActions (mergeAction VARCHAR(20));

    SELECT p.Id
          ,p.LogicalUsageValue
          ,pp.ParameterName
          ,pp.ParameterValue
    INTO #pipelineParameters
    FROM
      OpenJson( Json_Query( @json, '$.pipelines' ) )
      WITH (
        Id INT '$.Id'
       ,LogicalUsageValue VARCHAR(255) '$.logicalUsageValue'
       ,[parameters] NVARCHAR(MAX) AS JSON
      ) AS p
    CROSS APPLY OpenJson( Json_Query( p.[parameters], '$' ) )
                  WITH (
                    ParameterName NVARCHAR(200) '$.parameterName'
                   ,ParameterValue NVARCHAR(200) '$.parameterValue'
                  )AS pp

    --Merge PipelineParameters
    MERGE INTO procfwk.PipelineParameters AS t
    USING ( SELECT p.PipelineId
                  ,pp.ParameterName
                  ,pp.ParameterValue
            FROM #pipelineParameters AS pp
            INNER JOIN procfwk.Pipelines AS p
              ON p.LogicalUsageValue = pp.LogicalUsageValue
          )AS s
    ON t.PipelineId = s.PipelineId
       AND t.ParameterName = s.ParameterName
    WHEN NOT MATCHED BY TARGET
      THEN INSERT (PipelineId, ParameterName, ParameterValue)
           VALUES (s.PipelineId, s.ParameterName, s.ParameterValue)
    WHEN MATCHED
         AND IsNull(t.ParameterValue, '') <> IsNull(s.ParameterValue, '')
      THEN UPDATE 
             SET ParameterValue = s.ParameterValue
    WHEN NOT MATCHED BY SOURCE
         AND @deleteItemsNotInJson = 1
      THEN DELETE
    OUTPUT $action INTO #outputActions;

    DECLARE @sourceRows INT = (SELECT Count(*) FROM #pipelineParameters)
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