CREATE   PROCEDURE [procfwk].[ResetExecution]
	(
  @BatchId INT,
  @AdfParentPipelineRunId UNIQUEIDENTIFIER
	)
AS
BEGIN 
	SET NOCOUNT	ON;

	--capture any pipelines that might be in an unexpected state
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
		[EndDateTime]
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
		'Unknown',
		[EndDateTime]
	FROM
		[procfwk].[CurrentExecution]
	WHERE
    [BatchId] = @BatchId
		--these are predicted states
		AND [PipelineStatus] NOT IN
			    (
			    'Success',
			    'Failed',
			    'Blocked',
			    'Cancelled'
			    );
		
	--reset status ready for next attempt
	UPDATE
		[procfwk].[CurrentExecution]
	SET
		[StartDateTime] = NULL,
		[EndDateTime] = NULL,
		[PipelineStatus] = NULL,
		[LastStatusCheckDateTime] = NULL,
		[AdfPipelineRunId] = NULL,
		[PipelineParamsUsed] = NULL,
		[IsBlocked] = 0,
    [AdfParentPipelineRunId] = @AdfParentPipelineRunId
	WHERE
    [BatchId] = @BatchId
		AND 
    (
      IsNull([PipelineStatus],'') <> 'Success'
		  OR [IsBlocked] = 1
    );
	
	--return current execution id
	SELECT DISTINCT
		[LocalExecutionId] AS ExecutionId
	FROM
		[procfwk].[CurrentExecution]
  WHERE [BatchId] = @BatchId;
END;