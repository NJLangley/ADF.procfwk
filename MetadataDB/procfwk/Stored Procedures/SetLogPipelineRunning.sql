CREATE   PROCEDURE procfwk.SetLogPipelineRunning
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
		--case for clean up runs
		[StartDateTime] = CASE WHEN [StartDateTime] IS NULL THEN GETUTCDATE() ELSE [StartDateTime] END,
		[PipelineStatus] = 'Running'
	WHERE
		[LocalExecutionId] = @ExecutionId
    AND [BatchId] = @BatchId
		AND [StageId] = @StageId
		AND [PipelineId] = @PipelineId
END;
