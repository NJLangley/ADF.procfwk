CREATE VIEW [procfwkReporting].[CompleteExecutionLog]
AS

SELECT
	[LogId],
	[LocalExecutionId],
	[StageId],
	[StageName],
	[StageRunOrder],
	[PipelineId],
	[CallingOrchestratorName],
	[ResourceGroupName],
	[OrchestratorType],
	[OrchestratorName],
	[PipelineName],
	[PipelineLogicalUsageValue],
	[StartDateTime],
	[PipelineStatus],
	[EndDateTime],
	DATEDIFF(MINUTE, [StartDateTime], [EndDateTime]) 'RunDurationMinutes'
FROM 
	[procfwk].[ExecutionLog]