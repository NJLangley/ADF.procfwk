CREATE PROCEDURE procfwkHelpers.ImportOrchestratorsFromJson
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

    DROP TABLE IF EXISTS #orchestrators;
    DROP TABLE IF EXISTS #outputActions;
    CREATE TABLE #outputActions (mergeAction VARCHAR(20));

    SELECT *
    INTO #orchestrators
    FROM
      OpenJson( Json_Query( @json, '$.orchestrators' ) )
      WITH (
        [OrchestratorName] [nvarchar] (200) '$.name'
       ,[OrchestratorType] CHAR(3) '$.type'
	   ,[IsFrameworkOrchestrator] BIT '$.isFrameworkOrchestrator'
       ,[ResourceGroupName] [nvarchar] (200) '$.resourceGroupName'
       ,[SubscriptionId] [uniqueidentifier] '$.subscriptionId'
       ,[Description] [nvarchar] (max) '$.description'
      );

    
    -- Merge orchestrators
    MERGE INTO procfwk.Orchestrators AS t
    USING (SELECT *
           FROM #orchestrators
          )AS s
    ON t.OrchestratorName = s.OrchestratorName
    WHEN NOT MATCHED BY TARGET
      THEN INSERT (OrchestratorName, OrchestratorType, IsFrameworkOrchestrator, ResourceGroupName, SubscriptionId, Description)
           VALUES (s.OrchestratorName, s.OrchestratorType, s.IsFrameworkOrchestrator, s.ResourceGroupName, s.SubscriptionId, s.Description)
    WHEN MATCHED
         AND (
               IsNull(t.OrchestratorType, '') <> IsNull(s.OrchestratorType, '')
               OR IsNull(t.IsFrameworkOrchestrator, '') <> IsNull(s.IsFrameworkOrchestrator, '')
               OR IsNull(t.ResourceGroupName, '') <> IsNull(s.ResourceGroupName, '')
               OR IsNull(t.SubscriptionId, '') <> IsNull(s.SubscriptionId, '')
               OR IsNull(t.Description, '') <> IsNull(s.Description, '')
             )
      THEN UPDATE 
             SET OrchestratorType = s.OrchestratorType
                ,IsFrameworkOrchestrator = s.IsFrameworkOrchestrator
                ,ResourceGroupName = s.ResourceGroupName
                ,SubscriptionId = s.SubscriptionId
                ,Description = s.Description
    WHEN NOT MATCHED BY SOURCE
         AND @deleteItemsNotInJson = 1
      THEN DELETE
    OUTPUT $action INTO #outputActions;

    DECLARE @sourceRows INT = (SELECT Count(*) FROM #orchestrators)
           ,@insertedRows INT = (SELECT Count(*) FROM #outputActions WHERE mergeAction = 'INSERT')
           ,@updatedRows INT = (SELECT Count(*) FROM #outputActions WHERE mergeAction = 'UPDATE')
           ,@deletedRows INT = (SELECT Count(*) FROM #outputActions WHERE mergeAction = 'DELETE');

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