CREATE   PROCEDURE [procfwk].[GetWorkerPipelineDetails]
	(
	@ExecutionId UNIQUEIDENTIFIER,
  @BatchId INT,
	@StageId INT,
	@PipelineId INT
	)
AS
BEGIN
	SET NOCOUNT ON;

	SELECT 
		[PipelineName],
		[DataFactoryName],
		[ResourceGroupName],
    [AdfPipelineRunId],
    [AdFInfantPipelineRunId]
	FROM 
		[procfwk].[CurrentExecution]
	WHERE 
		[LocalExecutionId] = @ExecutionId
    AND BatchId = @BatchId
		AND [StageId] = @StageId
		AND [PipelineId] = @PipelineId;
END;