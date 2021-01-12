CREATE VIEW procfwkReporting.CurrentBatchExecutionSummary
AS
  SELECT 
    IsNull(Cast(be.BatchId AS VARCHAR(36)), '') AS BatchId
   ,IsNull(be.BatchName, 'Total') AS BatchName
   ,IsNull(be.BatchStatus, '') AS BatchStatus
   ,Count(CASE WHEN ISNULL(PipelineStatus, 'Not Started') = 'Not Started' THEN 1 END) AS NotStartedCount
   ,Count(CASE WHEN ce.PipelineStatus = 'Checking' THEN 1 END) AS CheckingCount
   ,Count(CASE WHEN ce.PipelineStatus = 'Validating' THEN 1 END) AS ValidatingCount
   ,Count(CASE WHEN ce.PipelineStatus = 'Preparing' THEN 1 END) AS PreparingCount
   ,Count(CASE WHEN ce.PipelineStatus = 'Blocked' THEN 1 END) AS BlockedCount
   ,Count(CASE WHEN ce.PipelineStatus = 'Running' THEN 1 END) AS RunningCount
   ,Count(CASE WHEN ce.PipelineStatus = 'Success' THEN 1 END) AS SuccessCount
   ,Count(CASE WHEN ce.PipelineStatus = 'Failed' THEN 1 END) AS FailedCount
   ,Count(CASE WHEN ce.PipelineStatus = 'Cancelled' THEN 1 END) AS CancelledCount
   ,Count(CASE WHEN ce.PipelineStatus = 'Unknown' THEN 1 END) AS UnknownCount
   ,COUNT(0) AS 'TotalCount'
  FROM procfwk.BatchExecution AS be
  INNER JOIN procfwk.CurrentExecution AS ce
    ON be.ExecutionId = ce.LocalExecutionId
  GROUP BY be.BatchId
          ,be.BatchName
          ,be.BatchStatus
   WITH ROLLUP
     HAVING Grouping(BatchId) = 1         -- First group by column for overall rollup
            OR Grouping(BatchStatus) = 0  -- Last group by column, for non-rollup rows rows