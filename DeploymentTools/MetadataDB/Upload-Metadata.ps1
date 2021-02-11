param(
    [Parameter (Mandatory)] $ConnectionString,
    [Parameter (Mandatory)] $JsonConfigPath,
    [Parameter (Mandatory)] $EnvJsonConfigPath
)

$connStrSafe = $connStr -replace 'Password=.+?;(User ID|MultipleActiveResultSets|Encrypt|TrustServerCertificate|Connection Timeout|Data Source|Server|Database|Initial Catalog|Persist Security Info|$)', 'Password=***;$1';
Write-Host "##[debug] Connection String: $connStrSafe";
Write-Host "##[debug] Json Config Path: $JsonConfigPath";


try {
    $sqlConn = New-Object System.Data.SqlClient.SqlConnection($ConnectionString);
    $sqlConn.Open();
    
    $sqlCmd = $sqlConn.CreateCommand();
    $sqlCmd.CommandType = 'StoredProcedure';
    $sqlCmd.CommandText = 'procfwkHelpers.ImportConfigFromJson';

    /# Dot source the functions we need to fix the JSON
    . $PSScriptRoot\Merge-Environment-Metadata.ps1;

    # Load the json, and run the function to replace the environment specific info
    $json = Update-MetadataPropertiesFromFile -MetadataPath $metadataPath -PropertiesPath $propertiesPath;

    $p1 = $sqlCmd.Parameters.Add('@json', [System.Data.SqlDbType]::NVarChar, -1);
    $p1.ParameterDirection.Input;
    $p1.Value = $json;
    
    $p2 = $sqlCmd.Parameters.Add('@dropExisting',[System.Data.SqlDbType]::Bit);
    $p2.ParameterDirection.Input;
    $p2.Value = $true;

    $results = $sqlCmd.ExecuteReader();
    $results;
 } finally {
     if ($sqlConn){
        $sqlConn.Close();
     }
 }
