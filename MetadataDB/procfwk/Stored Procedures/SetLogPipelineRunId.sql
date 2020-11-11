CREATE   PROCEDURE [procfwk].[SetLogPipelineRunId]
	(
	@ExecutionId UNIQUEIDENTIFIER,
  @BatchId INT,
	@StageId INT,
	@PipelineId INT,
	@RunId UNIQUEIDENTIFIER = NULL
	)
AS
BEGIN
	SET NOCOUNT ON;

	UPDATE
		[procfwk].[CurrentExecution]
	SET
		[AdfPipelineRunId] = LOWER(@RunId)
	WHERE
		[LocalExecutionId] = @ExecutionId
    AND [BatchId] = @BatchId
		AND [StageId] = @StageId
		AND [PipelineId] = @PipelineId
END;