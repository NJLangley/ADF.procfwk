
CREATE PROCEDURE procfwkHelpers.IsFrameworkRunning
AS
BEGIN
  SET XACT_ABORT, NOCOUNT ON;

  IF (SELECT Count(1) FROM procfwk.CurrentExecution) > 0
     OR (SELECT Count(1) FROM procfwk.BatchExecution WHERE BatchStatus = 'Running') > 0
    RETURN 1
  ELSE 
    RETURN 0
END