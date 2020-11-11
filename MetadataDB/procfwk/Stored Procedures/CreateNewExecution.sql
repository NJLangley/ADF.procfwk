CREATE   PROCEDURE [procfwk].[CreateNewExecution]
	(
	@CallingDataFactoryName NVARCHAR(200),
  @AdfParentPipelineRunId UNIQUEIDENTIFIER,
  @BatchId INT
	)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @LocalExecutionId UNIQUEIDENTIFIER = NEWID()

	DELETE FROM [procfwk].[CurrentExecution]
  WHERE [BatchId] = @BatchId;

	INSERT INTO [procfwk].[CurrentExecution]
		(
		[LocalExecutionId],
    [BatchId],
		[StageId],
		[PipelineId],
		[CallingDataFactoryName],
		[ResourceGroupName],
		[DataFactoryName],
		[PipelineName],
    [AdfParentPipelineRunId]
		)
	SELECT
		@LocalExecutionId,
    @BatchId,
		p.[StageId],
		p.[PipelineId],
		@CallingDataFactoryName,
		d.[ResourceGroupName],
		d.[DataFactoryName],
		p.[PipelineName],
    @AdfParentPipelineRunId
	FROM
    [procfwk].[BatchPipelineLink] bp
    INNER JOIN [procfwk].[Pipelines] p
      ON p.[PipelineId] = bp.[PipelineId]
		INNER JOIN [procfwk].[Stages] s
			ON p.[StageId] = s.[StageId]
		INNER JOIN [procfwk].[DataFactorys] d
			ON p.[DataFactoryId] = d.[DataFactoryId]
	WHERE
    bp.BatchId = @BatchId
    AND bp.[Enabled] = 1
		AND p.[Enabled] = 1
		AND s.[Enabled] = 1

	ALTER INDEX [IDX_GetPipelinesInStage] ON [procfwk].[CurrentExecution]
	REBUILD;

	SELECT
		@LocalExecutionId AS ExecutionId
END;