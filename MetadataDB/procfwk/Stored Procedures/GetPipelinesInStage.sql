CREATE   PROCEDURE [procfwk].[GetPipelinesInStage]
	(
	@ExecutionId UNIQUEIDENTIFIER,
  @BatchId INT,
	@StageId INT
	)
AS
BEGIN
	SET NOCOUNT ON;

	SELECT 
		[PipelineId]
	FROM 
		[procfwk].[CurrentExecution]
	WHERE 
		[LocalExecutionId] = @ExecutionId
		AND [StageId] = @StageId
    AND [BatchId] = @BatchId
		AND ISNULL([PipelineStatus],'') <> 'Success'
		AND [IsBlocked] <> 1
	ORDER BY
		[PipelineId] ASC;
END;