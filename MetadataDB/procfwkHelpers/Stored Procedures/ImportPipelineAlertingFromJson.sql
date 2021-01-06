CREATE PROCEDURE procfwkHelpers.ImportPipelineAlertingFromJson
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

    DROP TABLE IF EXISTS #pipelineAlerting;
    DROP TABLE IF EXISTS #outputActions;
    CREATE TABLE #outputActions (mergeAction VARCHAR(20));

    SELECT p.Id
          ,p.LogicalUsageValue
          ,ar.RecipientName
          ,ar.[Enabled]
          ,String_Agg(af.Value, ', ') WITHIN GROUP (ORDER BY ao.OutcomeBitPosition) AS AlertTypesString
          ,Sum(ao.BitValue) AS OutcomesBitValue
    INTO #pipelineAlerting
    FROM
      OpenJson( Json_Query( @json, '$.pipelines' ) )
      WITH (
        Id INT '$.Id'
       ,LogicalUsageValue VARCHAR(255) '$.logicalUsageValue'
       ,alertRecipients NVARCHAR(MAX) AS JSON
      ) AS p
    CROSS APPLY OpenJson( Json_Query( p.alertRecipients, '$' ) )
                  WITH (
                    RecipientName NVARCHAR(200) '$.name'
                   ,[Enabled] NVARCHAR(200) '$.enabled'
                   ,alertFor NVARCHAR(MAX) AS JSON
                  )AS ar
    CROSS APPLY OpenJson( Json_Query( ar.alertFor, '$' ) ) AS af
    INNER JOIN procfwk.AlertOutcomes AS ao
      ON ao.PipelineOutcomeStatus = af.Value
    GROUP BY p.Id
            ,p.LogicalUsageValue
            ,ar.RecipientName
            ,ar.Enabled

    -- Merge PipelineAlerting
      MERGE INTO procfwk.PipelineAlertLink AS t
      USING (SELECT p.PipelineId
                   ,r.RecipientId
                   ,pa.OutcomesBitValue
                   ,pa.Enabled
             FROM #pipelineAlerting AS pa
             INNER JOIN procfwk.Pipelines AS p
               ON p.LogicalUsageValue = pa.LogicalUsageValue
             INNER JOIN procfwk.Recipients AS r
               ON r.Name = pa.RecipientName
            )AS s
      ON t.PipelineId = s.PipelineId
         AND t.RecipientId = s.RecipientId
      WHEN NOT MATCHED BY TARGET
        THEN INSERT (PipelineId, RecipientId, OutcomesBitValue, Enabled)
             VALUES (s.PipelineId, s.RecipientId, s.OutcomesBitValue, s.Enabled)
      WHEN MATCHED
           AND (
                 IsNull(t.OutcomesBitValue, '') <> IsNull(s.OutcomesBitValue, '')
                 OR IsNull(t.Enabled, '') <> IsNull(s.Enabled, '')
               )
        THEN UPDATE 
               SET OutcomesBitValue = s.OutcomesBitValue
                  ,Enabled = s.Enabled
      WHEN NOT MATCHED BY SOURCE
           AND @dropExisting = 1
        THEN DELETE
    OUTPUT $action INTO #outputActions;

    DECLARE @sourceRows INT = (SELECT Count(*) FROM #pipelineAlerting)
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