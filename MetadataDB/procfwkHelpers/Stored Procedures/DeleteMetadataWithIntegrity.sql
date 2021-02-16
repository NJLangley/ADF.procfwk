
CREATE PROCEDURE [procfwkHelpers].[DeleteMetadataWithIntegrity]
(
  @deleteLogs BIT = 1
 ,@deleteCurrentExecutions BIT = 1
 ,@reseedIdentity BIT = 1
)
AS
BEGIN
	/*
	DELETE ORDER IMPORTANT FOR REFERENTIAL INTEGRITY
	*/

	--BatchExecution
	IF @deleteCurrentExecutions = 1 AND Object_Id(N'[procfwk].[BatchExecution]') IS NOT NULL 
		BEGIN
			TRUNCATE TABLE [procfwk].[BatchExecution];
		END;

	--CurrentExecution
	IF @deleteCurrentExecutions = 1 AND Object_Id(N'[procfwk].[CurrentExecution]') IS NOT NULL 
		BEGIN
			TRUNCATE TABLE [procfwk].[CurrentExecution];
		END;

	--ExecutionLog
	IF @deleteLogs = 1 AND Object_Id(N'[procfwk].[ExecutionLog]') IS NOT NULL 
		BEGIN
			TRUNCATE TABLE [procfwk].[ExecutionLog];
		END

	--ErrorLog
	IF @deleteLogs = 1 AND OBJECT_ID(N'[procfwk].[ErrorLog]') IS NOT NULL 
		BEGIN
			TRUNCATE TABLE [procfwk].[ErrorLog];
		END

	--BatchStageLink
	IF OBJECT_ID(N'[procfwk].[BatchStageLink]') IS NOT NULL 
		BEGIN
			DELETE FROM [procfwk].[BatchStageLink];
		END;

	--Batches
	IF OBJECT_ID(N'[procfwk].[Batches]') IS NOT NULL 
		BEGIN
			DELETE FROM [procfwk].[Batches];
		END;

	--PipelineDependencies
	IF OBJECT_ID(N'[procfwk].[PipelineDependencies]') IS NOT NULL 
		BEGIN
			DELETE FROM [procfwk].[PipelineDependencies];
			IF @reseedIdentity = 1
			BEGIN
				DBCC CHECKIDENT ('[procfwk].[PipelineDependencies]', RESEED, 0);
			END
		END;

	--PipelineAlertLink
	IF OBJECT_ID(N'[procfwk].[PipelineAlertLink]') IS NOT NULL 
		BEGIN
			DELETE FROM [procfwk].[PipelineAlertLink];
			IF @reseedIdentity = 1
			BEGIN
				DBCC CHECKIDENT ('[procfwk].[PipelineAlertLink]', RESEED, 0);
			END
		END;

	--Recipients
	IF OBJECT_ID(N'[procfwk].[Recipients]') IS NOT NULL 
		BEGIN
			DELETE FROM [procfwk].[Recipients];
			IF @reseedIdentity = 1
			BEGIN
				DBCC CHECKIDENT ('[procfwk].[Recipients]', RESEED, 0);
			END
		END;

	--AlertOutcomes
	IF OBJECT_ID(N'[procfwk].[AlertOutcomes]') IS NOT NULL 
		BEGIN
			TRUNCATE TABLE [procfwk].[AlertOutcomes];
		END;

	--PipelineAuthLink
	IF OBJECT_ID(N'[procfwk].[PipelineAuthLink]') IS NOT NULL 
		BEGIN
			DELETE FROM [procfwk].[PipelineAuthLink];
			IF @reseedIdentity = 1
			BEGIN
				DBCC CHECKIDENT ('[procfwk].[PipelineAuthLink]', RESEED, 0);
			END
		END;

	--ServicePrincipals
	IF OBJECT_ID(N'[dbo].[ServicePrincipals]') IS NOT NULL 
		BEGIN
			DELETE FROM [dbo].[ServicePrincipals];
			IF @reseedIdentity = 1
			BEGIN
				DBCC CHECKIDENT ('[dbo].[ServicePrincipals]', RESEED, 0);
			END
		END;

	--Properties
	IF OBJECT_ID(N'[procfwk].[Properties]') IS NOT NULL 
		BEGIN
			DELETE FROM [procfwk].[Properties];
			IF @reseedIdentity = 1
			BEGIN
				DBCC CHECKIDENT ('[procfwk].[Properties]', RESEED, 0);
			END
		END;

	--PipelineParameters
	IF OBJECT_ID(N'[procfwk].[PipelineParameters]') IS NOT NULL 
		BEGIN
			DELETE FROM [procfwk].[PipelineParameters];
			IF @reseedIdentity = 1
			BEGIN
				DBCC CHECKIDENT ('[procfwk].[PipelineParameters]', RESEED, 0);
			END
		END;

	--Pipelines
	IF OBJECT_ID(N'[procfwk].[Pipelines]') IS NOT NULL 
		BEGIN
			DELETE FROM [procfwk].[Pipelines];
			IF @reseedIdentity = 1
			BEGIN
				DBCC CHECKIDENT ('[procfwk].[Pipelines]', RESEED, 0);
			END
		END;

	--Orchestrators
	IF OBJECT_ID(N'[procfwk].[Orchestrators]') IS NOT NULL 
		BEGIN
			DELETE FROM [procfwk].[Orchestrators];
			IF @reseedIdentity = 1
			BEGIN
				DBCC CHECKIDENT ('[procfwk].[Orchestrators]', RESEED, 0);
			END
		END;

	--Stages
	IF OBJECT_ID(N'[procfwk].[Stages]') IS NOT NULL 
		BEGIN
			DELETE FROM [procfwk].[Stages];
			IF @reseedIdentity = 1
			BEGIN
				DBCC CHECKIDENT ('[procfwk].[Stages]', RESEED, 0);
			END
		END;

	--Subscriptions
	IF OBJECT_ID(N'[procfwk].[Subscriptions]') IS NOT NULL 
		BEGIN
			DELETE FROM [procfwk].[Subscriptions];
		END;
	
	--Tenants
	IF OBJECT_ID(N'[procfwk].[Tenants]') IS NOT NULL 
		BEGIN
			DELETE FROM [procfwk].[Tenants];
		END;
END;