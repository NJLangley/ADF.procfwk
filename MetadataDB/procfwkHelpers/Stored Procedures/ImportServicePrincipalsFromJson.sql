CREATE PROCEDURE procfwkHelpers.ImportServicePrincipalsFromJson
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

    DROP TABLE IF EXISTS #servicePrincipals;
    DROP TABLE IF EXISTS #outputActions;
    CREATE TABLE #outputActions (mergeAction VARCHAR(20));

    SELECT *
    INTO #servicePrincipals
    FROM
      OpenJson( Json_Query( @json, '$.servicePrincipals' ) )
      WITH (
        [PrincipalName] [nvarchar] (256) '$.name'
       ,PrincipalId UNIQUEIDENTIFIER '$.principalId'
       ,PrincipalSecret VARBINARY(256) '$.secret'
       ,PrincipalIdUrl [nvarchar] (max) '$.principalIdKeyVaultUrl'
       ,PrincipalSecretUrl [nvarchar] (max) '$.secretKeyVaultUrl'
      );

    -- Merge servicePrincipals
    MERGE INTO dbo.ServicePrincipals AS t
    USING (SELECT *
            FROM #servicePrincipals
          )AS s
    ON t.PrincipalName = s.PrincipalName
    WHEN NOT MATCHED BY TARGET
            THEN INSERT (PrincipalName, /*PrincipalId, PrincipalSecret,*/ PrincipalIdUrl, PrincipalSecretUrl)
            VALUES (s.PrincipalName, /*s.PrincipalId, s.PrincipalSecret,*/ s.PrincipalIdUrl, s.PrincipalSecretUrl)
    WHEN MATCHED
          AND (
                --OR IsNull(t.PrincipalId, '') <> IsNull(s.PrincipalId, '')
                --OR IsNull(t.PrincipalSecret, '') <> IsNull(s.PrincipalSecret, '')
                IsNull(t.PrincipalIdUrl, '') <> IsNull(s.PrincipalIdUrl, '')
                OR IsNull(t.PrincipalSecretUrl, '') <> IsNull(s.PrincipalSecretUrl, '')
              )
      THEN UPDATE 
              SET --PrincipalId = s.PrincipalId
                --,PrincipalSecret = s.PrincipalSecret
                  PrincipalIdUrl = s.PrincipalIdUrl
                ,PrincipalSecretUrl = s.PrincipalSecretUrl
    WHEN NOT MATCHED BY SOURCE
          AND @deleteItemsNotInJson = 1
      THEN DELETE
    OUTPUT $action INTO #outputActions;

    DECLARE @sourceRows INT = (SELECT Count(*) FROM #servicePrincipals)
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