CREATE PROCEDURE procfwkHelpers.ImportAlertRecipientsFromJson
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

    DROP TABLE IF EXISTS #alertRecipients;
    DROP TABLE IF EXISTS #outputActions;
    CREATE TABLE #outputActions (mergeAction VARCHAR(20));

    SELECT *
    INTO #alertRecipients
    FROM
      OpenJson( Json_Query( @json, '$.alertRecipients' ) )
      WITH (
        Name [varchar] (255) '$.name'
       ,EmailAddress NVARCHAR(500) '$.emailAddress'
       ,MessagePreference char(3) '$.messagePreference'
       ,Enabled bit '$.enabled'
      );
    
    -- Merge alertRecipients
    MERGE INTO procfwk.Recipients AS t
    USING (SELECT *
            FROM #alertRecipients
          )AS s
    ON t.Name = s.Name
    WHEN NOT MATCHED BY TARGET
      THEN INSERT (Name, EmailAddress, MessagePreference, Enabled)
            VALUES (s.Name, s.EmailAddress, s.MessagePreference, s.Enabled)
    WHEN MATCHED
          AND (
                IsNull(t.EmailAddress, '') <> IsNull(s.EmailAddress, '')
                OR IsNull(t.MessagePreference, '') <> IsNull(s.MessagePreference, '')
                OR IsNull(t.Enabled, '') <> IsNull(s.Enabled, '')
              )
      THEN UPDATE
              SET EmailAddress = s.EmailAddress
                ,MessagePreference = s.MessagePreference
                ,Enabled = s.Enabled
    WHEN NOT MATCHED BY SOURCE
          AND @deleteItemsNotInJson = 1
      THEN DELETE
    OUTPUT $action INTO #outputActions;

    DECLARE @sourceRows INT = (SELECT Count(*) FROM #alertRecipients)
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