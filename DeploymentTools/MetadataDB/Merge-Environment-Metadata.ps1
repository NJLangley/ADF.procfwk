



function ArrayToOrderedHash
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory = $true)] [System.Array] $Array,
        [parameter(Mandatory = $true)] [String] $KeyProperty
    )

    Write-Verbose "Entering Function: ArrayToOrderedHash";
   
    Write-Verbose "Key property: $keyProperty";

    $hash = [ordered]@{};
    $array | foreach { $hash[$_.$keyProperty] = $_ };
    return $hash;
}


function ConvertArraysToOrderedHashTables
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory = $true, ValueFromPipeline = $true)] $Item,
        [parameter(Mandatory = $false)] [String[]] $ArrayIdProps = @("name", "Name")
    )

    Write-Verbose "Entering Function: ConvertArraysToOrderedHashTables";

    if ( $Item.GetType().Name -eq "PSCustomObject" ) {

        Write-Verbose "Processing PSCustomObject...";
        Write-Verbose "Properties: $($Item.PSObject.Properties.Name)";

        # Loop through the properties, changing arrays and processing PSCustomObject's
        foreach ($prop in $Item.PSObject.Properties.Name) {

            Write-Verbose "Processing property '$prop' of type  $($Item.$prop.GetType().Name)";


            if ( $Item.$prop.GetType().Name -eq "Object[]" ) {
            
                foreach ($idProp in $arrayIdProps) {
                    if ( $Item.$prop.$idProp -notcontains $null ) {
                        Write-Verbose "Extracting ordered hash table from array using property $idProp as the key";
                        $Item.$prop = ArrayToOrderedHash -Array $Item.$prop -KeyProperty $idProp -Verbose:$VerbosePreference;
                        
                        break;
                    }
                }
            }

            elseif ( $Item.$prop.GetType().Name -eq "PSCustomObject" ) {
                Write-Verbose "Converting PSCustomObject property using a recursive function call...";

                $Item.$prop = ConvertArraysToOrderedHashTables -Item $Item.$prop;
            }
        }

        return $Item;
    }
    elseif ( $Item.GetType().BaseType -eq "Object[]" ) {
        Write-Verbose "Processing Array...";

        $wrapper = [PSCustomObject]@{
            WrappedObject = $Item
        }

        $result = ConvertArraysToOrderedHashTables -obj $wrapper;
        return $result.WrappedObject;
    }
    else {
        Write-Verbose "Unknown input object type, not supportted";
        return $Item;
    }
}



function ConvertOrderedHashTablesToArrays
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory = $true, ValueFromPipeline = $true)] $Item
    )

    Write-Verbose "Entering Function: ConvertOrderedHashTablesToArrays";

    if ( $Item.GetType().Name -eq "PSCustomObject" ) {

        Write-Verbose "Processing PSCustomObject...";
        Write-Verbose "Properties: $($Item.PSObject.Properties.Name)";

        # Loop through the properties, changing arrays and processing PSCustomObject's
        foreach ($prop in $Item.PSObject.Properties.Name) {

            Write-Verbose "Processing property '$prop' of type  $($Item.$prop.GetType().Name)";

            if ( $Item.$prop.GetType().Name -eq "OrderedDictionary" ) {
            
                Write-Verbose "Converting ordered hash table to array";
                $Item.$prop = $Item.$prop.Values;
            }
            elseif ( $Item.$prop.GetType().Name -eq "PSCustomObject" ) {
                Write-Verbose "Converting PSCustomObject property using a recursive function call...";

                $Item.$prop = ConvertArraysToOrderedHashTables -Item $Item.$prop;
            }
        }

        return $Item;
    }
    elseif ( $Item.GetType().BaseType -eq "OrderedDictionary" ) {
        Write-Verbose "Processing Array...";

        $wrapper = [PSCustomObject]@{
            WrappedObject = $Item
        }

        $result = ConvertOrderedHashTablesToArrays -obj $wrapper;
        return $result.WrappedObject;
    }
    else {
        Write-Verbose "Unknown input object type, not supportted";
        return $Item;
    }
}



function Format-Json {
    <#
    .SYNOPSIS
        Prettifies JSON output.
    .DESCRIPTION
        Reformats a JSON string so the output looks better than what ConvertTo-Json outputs.
    .PARAMETER Json
        Required: [string] The JSON text to prettify.
    .PARAMETER Minify
        Optional: Returns the json string compressed.
    .PARAMETER Indentation
        Optional: The number of spaces (1..1024) to use for indentation. Defaults to 4.
    .PARAMETER AsArray
        Optional: If set, the output will be in the form of a string array, otherwise a single string is output.
    .EXAMPLE
        $json | ConvertTo-Json  | Format-Json -Indentation 2
    #>
    [CmdletBinding(DefaultParameterSetName = 'Prettify')]
    Param(
        [Parameter(Mandatory = $true, Position = 0, ValueFromPipeline = $true)]
        [string]$Json,

        [Parameter(ParameterSetName = 'Minify')]
        [switch]$Minify,

        [Parameter(ParameterSetName = 'Prettify')]
        [ValidateRange(1, 1024)]
        [int]$Indentation = 4,

        [Parameter(ParameterSetName = 'Prettify')]
        [switch]$AsArray
    )

    if ($PSCmdlet.ParameterSetName -eq 'Minify') {
        return ($Json | ConvertFrom-Json) | ConvertTo-Json -Depth 100 -Compress
    }

    # If the input JSON text has been created with ConvertTo-Json -Compress
    # then we first need to reconvert it without compression
    if ($Json -notmatch '\r?\n') {
        $Json = ($Json | ConvertFrom-Json) | ConvertTo-Json -Depth 100
    }

    $indent = 0
    $regexUnlessQuoted = '(?=([^"]*"[^"]*")*[^"]*$)'

    $result = $Json -split '\r?\n' |
        ForEach-Object {
            # If the line contains a ] or } character, 
            # we need to decrement the indentation level unless it is inside quotes.
            if ($_ -match "[}\]]$regexUnlessQuoted") {
                $indent = [Math]::Max($indent - $Indentation, 0)
            }

            # Replace all colon-space combinations by ": " unless it is inside quotes.
            $line = (' ' * $indent) + ($_.TrimStart() -replace ":\s+$regexUnlessQuoted", ': ')

            # If the line contains a [ or { character, 
            # we need to increment the indentation level unless it is inside quotes.
            if ($_ -match "[\{\[]$regexUnlessQuoted") {
                $indent += $Indentation
            }

            $line
        }

    if ($AsArray) { return $result }
    return $result -Join [Environment]::NewLine
}




function Update-MetadataPropertiesFromFile
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory = $true)] [String] $MetadataPath,
        [parameter(Mandatory = $true)] [String] $PropertiesPath
    )

    Write-Host "======================================================================================";
    Write-Host "Invoking Update-MetadataPropertiesFromFile";
    Write-Host "with the following parameters:";
    Write-Host "======================================================================================";
    Write-Host "MetadataPath:       $MetadataPath";
    Write-Host "PropertiesPath:     $PropertiesPath";
    Write-Host "======================================================================================";

    $script:StartTime = Get-Date

    Write-Host "STEP: Reading Metadata and Properties JSON files..."

    # The id's are used in the order from the array below to act as keys when changing arrays into ordered hash sets
    $arrayIdProps = @("logicalUsageValue", "name")

    $metadataRaw = Get-Content -Path $MetadataPath -Raw;
    $metadata = ConvertFrom-Json -InputObject $metadataRaw | 
                ConvertArraysToOrderedHashTables -ArrayIdProps $arrayIdProps -Verbose:$VerbosePreference;

    $propertiesRaw = Get-Content -Path $PropertiesPath -Raw;
    $properties = ConvertFrom-Json -InputObject $propertiesRaw;

    Write-Host "======================================================================================";
    Write-Host "STEP: Replacing metadata properties with values defined in properties file..."


    foreach ($targetValue in $properties) {
        if ( $targetValue.target -eq $null ) {
            Write-Warning "Replacement value does not have a target. Skipping..."
            continue;
        }

        # Check if the target property exists.
        $expr = "`$targetExists = `$metadata.$($targetValue.target) -ne `$null";
        Invoke-Expression -Command $expr;
        if ( !$targetExists ) {
            Write-Warning "Target property not found: '$($targetValue.target)'";
            continue;
        }

        Write-Verbose "Updating properties on target: '$($targetValue.target)'";
        $targetProperties = $targetValue.PSObject.Properties.Name | Where-Object { $_ -ne "target" };

        foreach ($targetProperty in $targetProperties) {
            $targetPath = "$($targetValue.target).$targetProperty";

            # Check if the target path exists.
            $expr = "`$targetPathExists = `$metadata.$targetPath -ne `$null";
            Invoke-Expression -Command $expr;
            if ( !$targetPathExists ) {
                Write-Warning "Target property not found: '$targetPath'";
                continue;
            }
            
            # Get the property type, as we handle it differntly based on type, eg strings are quoted
            $expr = "`$targetType = `$metadata.$targetPath.GetType()";
            Invoke-Expression -Command $expr;

            Write-Verbose "Updating $($targetType.Name) property: '$targetPath'";

            if ( $targetType.Name -ceq "String" ){
                $expr = "`$metadata.$targetPath = `"$($targetValue.$targetProperty)`"";
            } elseif ( $targetType.Name -ceq "Boolean" ){
                $expr = "`$metadata.$targetPath = `$$($targetValue.$targetProperty)";
            } else {
                $expr = "`$metadata.$targetPath = $($targetValue.$targetProperty)";
            }

            Write-Verbose "Expression: '$expr'";
            Invoke-Expression -Command $expr;
        }

    }

    Write-Host "======================================================================================";
    Write-Host "STEP: Tidying up JSON output"
   
    $outputJson = $metadata | ConvertOrderedHashTablesToArrays | ConvertTo-Json -Depth 30 | Format-Json;
    return $outputJson;
}
