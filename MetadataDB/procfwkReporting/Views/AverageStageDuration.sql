CREATE   VIEW [procfwkReporting].[AverageStageDuration]
AS

WITH stageStartEnd AS
	(
	SELECT
		[LocalExecutionId],
		[StageId],
		[StageName],
		MIN([StartDateTime]) AS 'StageStart',
		MAX([EndDateTime]) AS 'StageEnd'
	FROM
		[procfwk].[ExecutionLog]
	GROUP BY
		[LocalExecutionId],
		[StageId],
    [StageName]
	)

SELECT
	sse.[StageId],
	sse.[StageName],
	s.[StageDescription],
	AVG(DATEDIFF(MINUTE, sse.[StageStart], sse.[StageEnd])) 'AvgStageRunDurationMinutes'
FROM
	stageStartEnd AS sse
	LEFT JOIN [procfwk].[Stages] s
		ON sse.[StageId] = s.[StageId]
GROUP BY
	sse.[StageId],
	sse.[StageName],
	s.[StageDescription]