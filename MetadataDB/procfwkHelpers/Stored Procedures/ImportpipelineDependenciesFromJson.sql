CREATE PROCEDURE procfwkHelpers.ImportpipelineDependenciesFromJson
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

    DROP TABLE IF EXISTS #pipelineDependencies;
    DROP TABLE IF EXISTS #outputActions;
    CREATE TABLE #outputActions (mergeAction VARCHAR(20));

    SELECT p.LogicalUsageValue
      ,dp.Value AS dependsOnPipeline
    INTO #pipelineDependencies
    FROM
      OpenJson( Json_Query( @json, '$.pipelines' ) )
      WITH (
        --Id INT '$.Id'
        LogicalUsageValue VARCHAR(255) '$.logicalUsageValue'
       ,dependsOnPipelines NVARCHAR(MAX) AS JSON
      ) AS p
    CROSS APPLY OpenJson( Json_Query( p.dependsOnPipelines, '$' ) ) AS dp
    
    -- Merge PipelineDependencies
    MERGE INTO procfwk.PipelineDependencies AS t
    USING (SELECT pp.PipelineId AS PipelineId
                 ,pc.PipelineId AS DependantPipelineId
           FROM #pipelineDependencies AS pd
           INNER JOIN procfwk.Pipelines AS pp
             ON pp.LogicalUsageValue = pd.dependsOnPipeline
           INNER JOIN procfwk.Pipelines AS pc
             ON pc.LogicalUsageValue = pd.LogicalUsageValue
          )AS S
    ON t.PipelineId = s.PipelineId
       AND t.DependantPipelineId = s.DependantPipelineId
    WHEN NOT MATCHED BY TARGET
      THEN INSERT (PipelineId, DependantPipelineId)
           VALUES (s.PipelineId, s.DependantPipelineId)
    WHEN NOT MATCHED BY SOURCE
         AND @dropExisting = 1
      THEN DELETE
    OUTPUT $action INTO #outputActions;

    DECLARE @sourceRows INT = (SELECT Count(*) FROM #pipelineDependencies)
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