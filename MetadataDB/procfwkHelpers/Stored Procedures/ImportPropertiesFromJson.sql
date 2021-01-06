CREATE PROCEDURE procfwkHelpers.ImportPropertiesFromJson
(
  @json NVARCHAR(MAX)
)
AS
BEGIN
  SET XACT_ABORT, NOCOUNT ON;

  PRINT Replicate(Char(10), 2) + Replicate('-', 180);
  PRINT 'Running proc: ' + (Object_Schema_Name(@@procId) + '.' + Object_Name(@@procId));
  PRINT Replicate('-', 180);

  BEGIN TRANSACTION;
  BEGIN TRY
    IF IsJson(@json) = 0
      RAISERROR('The json is not valid', 16, 1);

    DROP TABLE IF EXISTS #properties

    SELECT np.PropertyName
          ,np.PropertyValue
          ,np.Description
          ,CASE WHEN cp.PropertyId IS NULL
                  THEN 'INSERT'
                WHEN IsNull(cp.PropertyValue, '') <> IsNull(np.PropertyValue, '')
                     OR IsNull(cp.Description, '') <> IsNull(np.Description, '')
                  THEN 'UPDATE'
                ELSE NULL
             END AS UpdateAction
    INTO #properties
    FROM
      OpenJson( Json_Query( @json, '$.properties' ) )
      WITH (
        PropertyName VARCHAR( 128 ) '$.name'
       ,PropertyValue NVARCHAR( MAX ) '$.value'
       ,Description NVARCHAR( MAX ) '$.description'
      )AS np
    LEFT JOIN procfwk.Properties AS cp
      ON cp.PropertyName = np.PropertyName
         AND cp.ValidTo IS NULL;
    
    IF EXISTS (SELECT * FROM #properties WHERE PropertyName IS NULL OR PropertyValue IS NULL)
      RAISERROR('One or more properties do not have a name or value', 16, 1)

    DECLARE @sourceRows INT = (SELECT Count(*) FROM #properties)
           ,@insertRows INT = (SELECT Count(*) FROM #properties WHERE UpdateAction = 'INSERT')
           ,@updateRows INT = (SELECT Count(*) FROM #properties WHERE UpdateAction = 'UPDATE')
    
    PRINT '  Source properties:  ' + Cast(@sourceRows AS VARCHAR(10));
    PRINT '  New properties:     ' + Cast(@insertRows AS VARCHAR(10));
    PRINT '  Changed properties: ' + Cast(@updateRows AS VARCHAR(10)) + Char(10);

    -- Change properties if required
    DECLARE @propertyName VARCHAR( 128 )
           ,@propertyValue NVARCHAR( MAX )
           ,@propertyDescription NVARCHAR( MAX )
           ,@updateAction VARCHAR( 10 );
    
    DECLARE CUR_Properties CURSOR LOCAL FAST_FORWARD FOR
      SELECT p.PropertyName
            ,p.PropertyValue
            ,p.Description
            ,p.UpdateAction
      FROM #properties AS p
      WHERE p.UpdateAction IS NOT NULL;
    OPEN CUR_Properties;
    
    FETCH NEXT FROM CUR_Properties 
      INTO @propertyName, @propertyValue, @propertyDescription, @updateAction;

    WHILE @@fetch_Status = 0
    BEGIN
      PRINT '  - ' + (CASE WHEN @updateAction = 'INSERT' THEN 'Adding' ELSE 'Updating' END) + ' property: ' + @propertyName

      EXEC procfwk.AddProperty @PropertyName = @propertyName
                              ,@PropertyValue = @propertyValue
                              ,@Description = @propertyDescription;

      FETCH NEXT FROM CUR_Properties
        INTO @propertyName, @propertyValue, @propertyDescription, @updateAction;
    END
    CLOSE CUR_Properties;
    DEALLOCATE CUR_Properties;

    COMMIT;
  END TRY

  -- Rollback changes and throw to caller on an error
  BEGIN CATCH
    IF @@tranCount > 0
      ROLLBACK;
    
    THROW;
  END CATCH
END