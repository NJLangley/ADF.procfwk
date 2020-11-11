CREATE   PROCEDURE [procfwk].[SetLogPipelineLastStatusCheck]
	(
	@ExecutionId UNIQUEIDENTIFIER,
  @BatchId INT,
	@StageId INT,
	@PipelineId INT
	)
AS
BEGIN
	SET NOCOUNT ON;

	UPDATE
		[procfwk].[CurrentExecution]
	SET
		[LastStatusCheckDateTime] = GETUTCDATE()
	WHERE
		[LocalExecutionId] = @ExecutionId
    AND [BatchId] = @BatchId
		AND [StageId] = @StageId
		AND [PipelineId] = @PipelineId
END;