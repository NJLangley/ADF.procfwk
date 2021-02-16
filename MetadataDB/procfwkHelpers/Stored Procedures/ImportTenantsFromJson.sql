﻿

CREATE   PROCEDURE procfwkHelpers.ImportTenantsFromJson
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

    DROP TABLE IF EXISTS #tenants;
    DROP TABLE IF EXISTS #outputActions;
    CREATE TABLE #outputActions (mergeAction VARCHAR(20));

    SELECT *
    INTO #tenants
    FROM
      OpenJson( Json_Query( @json, '$.tenants' ) )
      WITH (
        TenantId UNIQUEIDENTIFIER '$.tenantId'
       ,Name NVARCHAR( 200 ) '$.name'
       ,Description NVARCHAR( MAX ) '$.description'
      );
    

    -- Merge tenants
    MERGE INTO procfwk.Tenants AS t
    USING (SELECT *
           FROM #tenants
          )AS s
    ON t.TenantId = s.TenantId
    WHEN NOT MATCHED BY TARGET
      THEN INSERT (TenantId, Name, Description)
           VALUES (s.TenantId, s.Name, s.Description)
    WHEN MATCHED
         AND (
               IsNull(t.Name, '') <> IsNull(s.Name, '')
               OR IsNull(t.Description, '') <> IsNull(s.Description, '')
             )
      THEN UPDATE 
             SET Name = s.Name
                ,Description = s.Description
    WHEN NOT MATCHED BY SOURCE
         AND @deleteItemsNotInJson = 1
      THEN DELETE
    OUTPUT $action INTO #outputActions;

    DECLARE @sourceRows INT = (SELECT Count(*) FROM #tenants)
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