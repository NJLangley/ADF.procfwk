CREATE   PROCEDURE [procfwk].[SetLogChildPipelineRunId]
	(
	@ExecutionId UNIQUEIDENTIFIER,
  @BatchId INT,
	@StageId INT,
  @AdfChildPipelineRunId UNIQUEIDENTIFIER
	)
AS
BEGIN
	SET NOCOUNT ON;
	
	UPDATE
		[procfwk].[CurrentExecution]
	SET
		[AdfChildPipelineRunId] = @AdfChildPipelineRunId,
    [AdFInfantPipelineRunId] =  NULL,
    [AdfPipelineRunId] = NULL
	WHERE
		[LocalExecutionId] = @ExecutionId
    AND [BatchId] = @BatchId
		AND [StageId] = @StageId;
END;