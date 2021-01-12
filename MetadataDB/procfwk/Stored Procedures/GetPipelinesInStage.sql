CREATE PROCEDURE [procfwk].[GetPipelinesInStage]
	(
	@ExecutionId UNIQUEIDENTIFIER,
	@StageId INT
	)
AS
BEGIN
	SET NOCOUNT ON;

	SELECT 
		[PipelineId],
		[PipelineLogicalUsageValue]
	FROM 
		[procfwk].[CurrentExecution]
	WHERE 
		[LocalExecutionId] = @ExecutionId
		AND [StageId] = @StageId
		AND ISNULL([PipelineStatus],'') <> 'Success'
		AND [IsBlocked] <> 1
	ORDER BY
		[PipelineId] ASC;
END;