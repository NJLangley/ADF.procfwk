CREATE   PROCEDURE [procfwk].[UpdateExecutionLog]
	(
	@PerformErrorCheck BIT = 1,
  @BatchId INT
	)
AS
BEGIN
	SET NOCOUNT ON;
		
	DECLARE @AllCount INT
	DECLARE @SuccessCount INT

	IF @PerformErrorCheck = 1
	BEGIN
		--Check current execution
		SELECT @AllCount = COUNT(0) FROM [procfwk].[CurrentExecution] WHERE [BatchId] = @BatchId
		SELECT @SuccessCount = COUNT(0) FROM [procfwk].[CurrentExecution] WHERE [BatchId] = @BatchId AND [PipelineStatus] = 'Success'

		IF @AllCount <> @SuccessCount
			BEGIN
				RAISERROR('Framework execution complete but not all Worker pipelines succeeded. See the [procfwk].[CurrentExecution] table for details',16,1);
				RETURN 0;
			END;
	END;

	--Do this if no error raised and when called by the execution wrapper (OverideRestart = 1).
	INSERT INTO [procfwk].[ExecutionLog]
		(
		[LocalExecutionId],
    [BatchId],
		[StageId],
		[PipelineId],
		[CallingDataFactoryName],
		[ResourceGroupName],
		[DataFactoryName],
		[PipelineName],
		[StartDateTime],
		[PipelineStatus],
		[EndDateTime],
		[AdfPipelineRunId],
		[PipelineParamsUsed]
		)
	SELECT
		[LocalExecutionId],
    [BatchId],
		[StageId],
		[PipelineId],
		[CallingDataFactoryName],
		[ResourceGroupName],
		[DataFactoryName],
		[PipelineName],
		[StartDateTime],
		[PipelineStatus],
		[EndDateTime],
		[AdfPipelineRunId],
		[PipelineParamsUsed]
	FROM
		[procfwk].[CurrentExecution]
  WHERE
    [BatchId] = @BatchId;

	DELETE FROM [procfwk].[CurrentExecution]
  WHERE BatchId = @BatchId;
END;