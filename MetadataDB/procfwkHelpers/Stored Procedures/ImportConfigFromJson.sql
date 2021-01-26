CREATE PROCEDURE procfwkHelpers.ImportConfigFromJson
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
  
  PRINT 'Loading configuration from JSON.'
  PRINT 'Options: Import ID''s:            ' + (CASE WHEN @importIds = 1 THEN 'Yes' ELSE 'No' END)
  PRINT '         Drop existing metadata: ' + (CASE WHEN @dropExisting = 1 THEN 'Yes' ELSE 'No' END) + Char(10)


  BEGIN TRANSACTION;
  BEGIN TRY
    IF IsJson(@json) = 0
      RAISERROR('The json is not valid', 16, 1);

    IF @dropExisting = 1
    BEGIN
      PRINT 'Running framework cleanup process...'
      PRINT '  - Calling procfwkTesting.CleanUpMetadata' + Char(10)
      EXEC procfwkTesting.CleanUpMetadata;
    END

    PRINT Char(10) + 'Updating framework metadata...'

    EXEC procfwkHelpers.ImportPropertiesFromJson @json;

    EXEC procfwkHelpers.ImportTenantsFromJson @json
                                             ,@dropExisting;

    EXEC procfwkHelpers.ImportSubscriptionsFromJson @json
                                                   ,@dropExisting;

    EXEC procfwkHelpers.ImportOrchestratorsFromJson @json
                                                   ,@importIds
                                                   ,@dropExisting;

    EXEC procfwkHelpers.ImportServicePrincipalsFromJson @json
                                                       ,@dropExisting;

    EXEC procfwkHelpers.ImportAlertRecipientsFromJson @json
                                                     ,@importIds
                                                     ,@dropExisting;

    EXEC procfwkHelpers.ImportBatchesFromJson @json
                                             ,@dropExisting;

    EXEC procfwkHelpers.ImportStagesFromJson @json
                                            ,@importIds
                                            ,@dropExisting;

    EXEC procfwkHelpers.ImportBatchStageLinkFromJson @json
                                                    ,@dropExisting;
                                                    
    EXEC procfwkHelpers.ImportPipelinesFromJson @json
                                               ,@importIds
                                               ,@dropExisting;

    EXEC procfwkHelpers.ImportpipelineDependenciesFromJson @json
                                                          ,@importIds
                                                          ,@dropExisting;

    EXEC procfwkHelpers.ImportPipelineParametersFromJson @json
                                                        --,@importIds
                                                        ,@dropExisting;

    EXEC procfwkHelpers.ImportPipelineAlertingFromJson @json
                                                    --,@importIds
                                                      ,@dropExisting;

                                                          
                                                
    COMMIT;
  END TRY

  -- Rollback changes and throw to caller on an error
  BEGIN CATCH
    IF @@tranCount > 0
      ROLLBACK;
    
    THROW;
  END CATCH
END