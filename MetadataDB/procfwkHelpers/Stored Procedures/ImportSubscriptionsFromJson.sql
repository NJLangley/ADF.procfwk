CREATE PROCEDURE procfwkHelpers.ImportSubscriptionsFromJson
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

    DROP TABLE IF EXISTS #subscriptions;
    DROP TABLE IF EXISTS #outputActions;
    CREATE TABLE #outputActions (mergeAction VARCHAR(20));

    SELECT *
    INTO #subscriptions
    FROM
      OpenJson( Json_Query( @json, '$.subscriptions' ) )
      WITH (
        SubscriptionId UNIQUEIDENTIFIER '$.subscriptionId'
       ,Name NVARCHAR( 200 ) '$.name'
       ,Description NVARCHAR( MAX ) '$.description'
       ,TenantId UNIQUEIDENTIFIER '$.tenantId'
      );

    -- Merge subscriptions
    MERGE INTO procfwk.Subscriptions AS t
    USING (SELECT *
           FROM #subscriptions
          )AS s
    ON t.SubscriptionId = s.SubscriptionId
    WHEN NOT MATCHED BY TARGET
      THEN INSERT (SubscriptionId, Name, Description, TenantId)
           VALUES (s.SubscriptionId, s.Name, s.Description, s.TenantId)
    WHEN MATCHED
         AND (
               IsNull(t.Name, '') <> IsNull(s.Name, '')
               OR IsNull(t.Description, '') <> IsNull(s.Description, '')
               OR IsNull(t.TenantId, '') <> IsNull(s.TenantId, '')
             )
      THEN UPDATE 
             SET Name = s.Name
                ,Description = s.Description
                ,TenantId = s.TenantId
    WHEN NOT MATCHED BY SOURCE
         AND @deleteItemsNotInJson = 1
      THEN DELETE
    OUTPUT $action INTO #outputActions;

    DECLARE @sourceRows INT = (SELECT Count(*) FROM #subscriptions)
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