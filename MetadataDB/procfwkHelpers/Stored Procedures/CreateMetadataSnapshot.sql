
CREATE PROCEDURE procfwkHelpers.CreateMetadataSnapshot
(
  @Comments NVARCHAR(1000) = NULL
)
AS
BEGIN
  SET XACT_ABORT, NOCOUNT ON;

  DECLARE @json NVARCHAR(MAX);
  EXEC procfwkHelpers.ExportConfigAsJson @prettyPrintJson = 1
                                        ,@json = @json OUTPUT;

  INSERT INTO procfwkHelpers.MetadataSnapshot (SnapshotJson, Comments)
  VALUES (@json, @Comments);
END