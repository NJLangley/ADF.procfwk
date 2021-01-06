﻿CREATE PROCEDURE procfwkHelpers.ImportDataFactoriesFromJson
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

    DROP TABLE IF EXISTS #dataFactories;
    DROP TABLE IF EXISTS #outputActions;
    CREATE TABLE #outputActions (mergeAction VARCHAR(20));

    SELECT *
    INTO #dataFactories
    FROM
      OpenJson( Json_Query( @json, '$.dataFactories' ) )
      WITH (
        [DataFactoryName] [nvarchar] (200) '$.name'
       ,[ResourceGroupName] [nvarchar] (200) '$.resourceGroupName'
       ,[SubscriptionId] [uniqueidentifier] '$.subscriptionId'
       ,[Description] [nvarchar] (max) '$.description'
      );

    
    -- Merge dataFactories
    MERGE INTO procfwk.DataFactorys AS t
    USING (SELECT *
           FROM #dataFactories
          )AS s
    ON t.DataFactoryName = s.DataFactoryName
    WHEN NOT MATCHED BY TARGET
      THEN INSERT (DataFactoryName, ResourceGroupName, SubscriptionId, Description)
           VALUES (s.DataFactoryName, s.ResourceGroupName, s.SubscriptionId, s.Description)
    WHEN MATCHED
         AND (
               IsNull(t.ResourceGroupName, '') <> IsNull(s.ResourceGroupName, '')
               OR IsNull(t.SubscriptionId, '') <> IsNull(s.SubscriptionId, '')
               OR IsNull(t.Description, '') <> IsNull(s.Description, '')
             )
      THEN UPDATE 
             SET ResourceGroupName = s.ResourceGroupName
                ,SubscriptionId = s.SubscriptionId
                ,Description = s.Description
    WHEN NOT MATCHED BY SOURCE
         AND @dropExisting = 1
      THEN DELETE
    OUTPUT $action INTO #outputActions;

    DECLARE @sourceRows INT = (SELECT Count(*) FROM #dataFactories)
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