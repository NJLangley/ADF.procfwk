CREATE VIEW [procfwkReporting].[CompleteExecutionLog]
AS

SELECT
	[LogId],
	[LocalExecutionId],
	[StageId],
	[StageName],
	[StageRunOrder],
	[PipelineId],
	[CallingDataFactoryName],
	[ResourceGroupName],
	[DataFactoryName],
	[PipelineName],
	[PipelineLogicalUsageValue],
	[StartDateTime],
	[PipelineStatus],
	[EndDateTime],
	DATEDIFF(MINUTE, [StartDateTime], [EndDateTime]) 'RunDurationMinutes'
FROM 
	[procfwk].[ExecutionLog]