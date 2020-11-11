CREATE   PROCEDURE [procfwk].[SetLogStagePreparing]
	(
	@ExecutionId UNIQUEIDENTIFIER,
  @BatchId INT,
	@StageId INT
	)
AS
BEGIN
	SET NOCOUNT ON;
	
	UPDATE
		[procfwk].[CurrentExecution]
	SET
		[PipelineStatus] = 'Preparing'
	WHERE
		[LocalExecutionId] = @ExecutionId
    AND [BatchId] = @BatchId
		AND [StageId] = @StageId
		AND [StartDateTime] IS NULL
		AND [IsBlocked] <> 1;
END;
