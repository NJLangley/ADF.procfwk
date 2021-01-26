CREATE PROCEDURE procfwkHelpers.ImportPipelineAuthLinkFromJson
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

    DROP TABLE IF EXISTS #pipelineAuthLink;
    DROP TABLE IF EXISTS #outputActions;
    CREATE TABLE #outputActions (mergeAction VARCHAR(20));

    SELECT p.LogicalUsageValue
          ,p.OrchestratorName
          ,p.ServicePrincipalName
    INTO #pipelineAuthLink
    FROM
      OpenJson( Json_Query( @json, '$.pipelines' ) )
      WITH (
        Id INT '$.Id'
       ,LogicalUsageValue VARCHAR(255) '$.logicalUsageValue'
       ,OrchestratorName VARCHAR(200) '$.orchestratorName'
       ,ServicePrincipalName VARCHAR(256) '$.servicePrincipalName'
      ) AS p
    --LEFT JOIN procfwk.Pipelines AS p2
    --  ON p2.LogicalUsageValue = p.LogicalUsageValue
    --LEFT JOIN procfwk.Orchestrators AS df
    --  ON df.OrchestratorName = p.OrchestratorName
    --LEFT JOIN dbo.ServicePrincipals AS sp
    --  ON sp.PrincipalName = p.ServicePrincipalName
  
    -- Merge BatchStageLink
    MERGE INTO procfwk.PipelineAuthLink AS t
    USING (SELECT p.PipelineId
                 ,o.OrchestratorId
                 ,sp.CredentialId
           FROM #pipelineAuthLink AS pal
           LEFT JOIN procfwk.Pipelines AS p
             ON p.LogicalUsageValue = pal.LogicalUsageValue
           LEFT JOIN procfwk.Orchestrators AS o
             ON o.OrchestratorName = pal.OrchestratorName
           LEFT JOIN dbo.ServicePrincipals AS sp
             ON sp.PrincipalName = pal.ServicePrincipalName
          )AS S
    ON t.PipelineId = s.PipelineId
       AND t.OrchestratorId = s.OrchestratorId
       AND t.CredentialId = s.CredentialId
    WHEN NOT MATCHED BY TARGET
      THEN INSERT (PipelineId, OrchestratorId, CredentialId)
           VALUES (s.PipelineId, s.OrchestratorId, s.CredentialId)
    WHEN NOT MATCHED BY SOURCE
         AND @dropExisting = 1
      THEN DELETE
    OUTPUT $action INTO #outputActions;

    DECLARE @sourceRows INT = (SELECT Count(*) FROM #pipelineAuthLink)
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