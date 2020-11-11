CREATE   PROCEDURE [procfwk].[SetLogInfantPipelineRunId]
	(
	@ExecutionId UNIQUEIDENTIFIER,
  @BatchId INT,
	@StageId INT,
  @PipelineId INT,
  @AdfInfantPipelineRunId UNIQUEIDENTIFIER
	)
AS
BEGIN
	SET NOCOUNT ON;
	
	UPDATE
		[procfwk].[CurrentExecution]
	SET
		[AdfInfantPipelineRunId] = @AdfInfantPipelineRunId
	WHERE
		[LocalExecutionId] = @ExecutionId
    AND [BatchId] = @BatchId
		AND [StageId] = @StageId
    AND [PipelineId] = @PipelineId;
END