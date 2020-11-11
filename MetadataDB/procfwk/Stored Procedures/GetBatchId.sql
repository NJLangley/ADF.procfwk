CREATE   PROCEDURE [procfwk].[GetBatchId]
	(
  @BatchName NVARCHAR(200)
	)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @BatchId INT;

  SET @BatchId = ( SELECT TOP 1 BatchId FROM procfwk.Batches WHERE BatchName = @BatchName )
  IF @BatchId IS NULL
		BEGIN
			RAISERROR('The batch name does not exist. Check the batch name and re-run the batch.',16,1);
			RETURN 0;
		END;

	SELECT @BatchId AS BatchId
END;