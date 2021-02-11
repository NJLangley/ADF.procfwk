CREATE PROCEDURE [procfwkHelpers].[ImportConfigFromJson]
(
  @json NVARCHAR(MAX)
 ,@importIds BIT = 0
 ,@dropExisting BIT = 0
)
AS
BEGIN
  SET XACT_ABORT, NOCOUNT ON;

  EXEC procfwkHelpers.CreateMetadataSnapshot @Comments = 'Pre-deployment Backup';

  DECLARE @metadataPreDeploymentSnapshotIds INT;
  INSERT INTO procfwkHelpers.MetadataSnapshot (SnapshotJson, Comments)
  VALUES (@json, 'Deployment Metadata');
  SET @metadataPreDeploymentSnapshotIds = @@identity;
  

  -- We want to make sure that there is nothing running during the deployment, and block anything trying to start during the deployment, so use serializable.
  SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
  BEGIN TRANSACTION;

  DECLARE @isFrameworkRunning INT;
  EXEC @isFrameworkRunning = procfwkHelpers.IsFrameworkRunning;

  IF @isFrameworkRunning = 1
  BEGIN
    RAISERROR('Cannot deploy new metadata while there are batches running. Allow all jobs to finish, or cleanm up the procfwk.CurrentExecution & procfwk.BatchExecution tables.', 16, 1);
  END

  PRINT Replicate(Char(10), 2) + Replicate('-', 180);
  PRINT 'Running proc: ' + (Object_Schema_Name(@@procId) + '.' + Object_Name(@@procId));
  PRINT Replicate('-', 180);
  
  PRINT 'Loading configuration from JSON.'
  PRINT 'Options: Import ID''s:            ' + (CASE WHEN @importIds = 1 THEN 'Yes' ELSE 'No' END)
  PRINT '         Drop existing metadata: ' + (CASE WHEN @dropExisting = 1 THEN 'Yes' ELSE 'No' END) + Char(10)


  BEGIN TRY
    IF IsJson(@json) = 0
      RAISERROR('The json is not valid', 16, 1);

    IF @dropExisting = 1
    BEGIN
      PRINT 'Running framework cleanup process...'
      PRINT '  - Calling procfwkHelpers.DeleteMetadataWithIntegrity' + Char(10)
      EXEC procfwkHelpers.DeleteMetadataWithIntegrity;
    END

    PRINT Char(10) + 'Updating framework metadata...'

    EXEC procfwkHelpers.ImportPropertiesFromJson @json;

    EXEC procfwkHelpers.ImportTenantsFromJson @json
                                             ,@dropExisting;

    EXEC procfwkHelpers.ImportSubscriptionsFromJson @json
                                                   ,@dropExisting;

    EXEC procfwkHelpers.ImportOrchestratorsFromJson @json
                                                   ,@dropExisting;

    EXEC procfwkHelpers.ImportServicePrincipalsFromJson @json
                                                       ,@dropExisting;

    EXEC procfwkHelpers.ImportAlertRecipientsFromJson @json
                                                     ,@dropExisting;

    EXEC procfwkHelpers.ImportBatchesFromJson @json
                                             ,@dropExisting;

    EXEC procfwkHelpers.ImportStagesFromJson @json
                                            ,@dropExisting;

    EXEC procfwkHelpers.ImportBatchStageLinkFromJson @json
                                                    ,@dropExisting;
                                                    
    EXEC procfwkHelpers.ImportPipelinesFromJson @json
                                               ,@dropExisting;

    EXEC procfwkHelpers.ImportpipelineDependenciesFromJson @json
                                                          ,@dropExisting;

    EXEC procfwkHelpers.ImportPipelineParametersFromJson @json
                                                        ,@dropExisting;

    EXEC procfwkHelpers.ImportPipelineAlertingFromJson @json
                                                      ,@dropExisting;

    EXEC procfwkHelpers.ImportPipelineAuthLinkFromJson @json
                                                      ,@dropExisting;
                                                          
           
    UPDATE procfwkHelpers.MetadataSnapshot
    SET SnapshotDateTime = GetDate()
       ,Comments = 'Successful Deployment Metadata'
    WHERE Id = @metadataPreDeploymentSnapshotIds;

    COMMIT;
  END TRY

  -- Rollback changes and throw to caller on an error
  BEGIN CATCH
    IF @@tranCount > 0
      ROLLBACK;
    
    UPDATE procfwkHelpers.MetadataSnapshot
    SET SnapshotDateTime = GetDate()
       ,Comments = 'Successful Deployment Metadata'
    WHERE Id = @metadataPreDeploymentSnapshotIds;

    THROW;
  END CATCH
END