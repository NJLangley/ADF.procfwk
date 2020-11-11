﻿CREATE   PROCEDURE [procfwk].[SetLogPipelineChecking]
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
		[PipelineStatus] = 'Checking'
	WHERE
		[LocalExecutionId] = @ExecutionId
    AND [BatchId] = @BatchId
		AND [StageId] = @StageId
		AND [PipelineId] = @PipelineId
END;
