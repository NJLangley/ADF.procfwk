CREATE PROCEDURE procfwkHelpers.ImportPipelinesFromJson
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

    DROP TABLE IF EXISTS #pipelines;

    SELECT p.*
    INTO #pipelines
    FROM
      OpenJson( Json_Query( @json, '$.pipelines' ) )
      WITH (
        Id INT '$.Id'
       ,PipelineName NVARCHAR(200) '$.name'
       ,LogicalPredecessorLogicalUsageValue NVARCHAR(200) '$.logicalPredecessorLogicalUsageValue'
       ,OrchestratorSlug [nvarchar] (200) '$.orchestratorSlug'
       ,StageName [varchar] (255) '$.stageName'
       ,ServicePrincipalName [nvarchar] (256) '$.servicePrincipalName'
       ,LogicalUsageValue VARCHAR(255) '$.logicalUsageValue'
       ,Enabled bit '$.enabled'
      ) AS p


    -- The orchestrator name is env specific, so the json uses the slug to join stuff together...
    DROP TABLE IF EXISTS #orchestrators;
    
    SELECT *
    INTO #orchestrators
    FROM
      OpenJson( Json_Query( @json, '$.orchestrators' ) )
      WITH (
        [OrchestratorName] [nvarchar] (200) '$.name'
       ,[OrchestratorSlug] [nvarchar] (200) '$.slug'
      );
    

    -- Merge pipelines. Have to merge the pipelines, then set the LogicalPredecessorId after as it is a fk
    -- to the same table and we need the id's
    CREATE TABLE #updatedPipelines (
      MergeAction VARCHAR(20) NOT NULL
     ,PipelineId VARCHAR(255) NOT NULL
     ,LogicalPredecessorLogicalUsageValue VARCHAR(255) NULL
    )

    MERGE INTO procfwk.Pipelines AS t
    USING (SELECT o.OrchestratorId
                 ,s.StageId
                 ,p.PipelineName
                 ,p.LogicalPredecessorLogicalUsageValue
                 ,p.Enabled
                 ,p.LogicalUsageValue
           FROM #pipelines AS p
           LEFT JOIN #orchestrators AS os
             ON os.OrchestratorSlug = p.OrchestratorSlug
           LEFT JOIN procfwk.Orchestrators AS o
             ON o.OrchestratorName = os.OrchestratorName
           LEFT JOIN procfwk.Stages AS s
             ON s.StageName = p.StageName
          )AS S
    ON t.LogicalUsageValue = s.LogicalUsageValue
    WHEN NOT MATCHED BY TARGET
      THEN INSERT (OrchestratorId, StageId, PipelineName, Enabled, LogicalUsageValue)
           VALUES (s.OrchestratorId, s.StageId, s.PipelineName, s.Enabled, s.LogicalUsageValue)
    WHEN MATCHED
         AND (
               IsNull(t.OrchestratorId, '') <> IsNull(s.OrchestratorId, '')
               OR IsNull(t.StageId, '') <> IsNull(s.StageId, '')
               OR IsNull(t.PipelineName, '') <> IsNull(s.PipelineName, '')
               OR IsNull(t.Enabled, '') <> IsNull(s.Enabled, '')
               OR s.LogicalPredecessorLogicalUsageValue IS NOT NULL
             )
      THEN UPDATE 
             SET OrchestratorId = s.OrchestratorId
                ,StageId = s.StageId
                ,PipelineName = s.PipelineName
                ,Enabled = s.Enabled
    WHEN NOT MATCHED BY SOURCE
         AND @dropExisting = 1
      THEN DELETE
    OUTPUT $action AS MergeAction,
           INSERTED.PipelineId
          ,s.LogicalPredecessorLogicalUsageValue
      INTO #updatedPipelines;

    UPDATE p
    SET p.LogicalPredecessorId = lpp.PipelineId
    FROM procfwk.Pipelines AS p
    INNER JOIN #updatedPipelines AS u
      ON u.PipelineId = p.PipelineId
    INNER JOIN procfwk.Pipelines AS lpp
      ON lpp.LogicalUsageValue = u.LogicalPredecessorLogicalUsageValue
    WHERE MergeAction IN ('INSERT', 'UPDATE');


    DECLARE @sourceRows INT = (SELECT Count(*) FROM #pipelines)
           ,@insertedRows INT = (SELECT Count(*) FROM #updatedPipelines WHERE MergeAction = 'INSERT')
           ,@updatedRows INT = (SELECT Count(*) FROM #updatedPipelines WHERE MergeAction = 'UPDATE')
           ,@deletedRows INT = (SELECT Count(*) FROM #updatedPipelines WHERE MergeAction = 'DELETE');

    PRINT '  Source rows:   ' + Cast(@sourceRows AS VARCHAR(10));
    PRINT '  Inserted rows: ' + Cast(@insertedRows AS VARCHAR(10));
    PRINT '  Updated rows:  ' + Cast(@updatedRows AS VARCHAR(10));
    PRINT '  Deleted rows:  ' + Cast(@deletedRows AS VARCHAR(10));

    COMMIT;
  END TRY

  -- Rollback changes and throw to caller on an error
  BEGIN CATCH
    IF @@tranCount > 0
      ROLLBACK;
    
    THROW;
  END CATCH
END