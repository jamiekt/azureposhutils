function Invoke-HiveStatement ([string]$query, [hashtable]$defines, [bool]$printDebugInfo=0)
{
    #Invoke-Hive on its own doesn't produce much meaningful info. Here we wrap Invoke-Hive and provide execution time
    if ($printDebugInfo) {
        $defines;
        $query; 
    }
    $startTime=get-date; 
    $startTime; 
    Invoke-Hive -Defines $defines -Query $query; 
    ((get-date).Subtract($startTime)).TotalSeconds.ToString() + "seconds" 
}
function Invoke-HiveScript ([string]$scriptPath, [hashtable]$defines, [bool]$printDebugInfo=0)
{
    $query = Get-Content $scriptPath -raw;
    Invoke-HiveStatement $query $defines $printDebugInfo
}
function Set-Subscription ([System.Security.Cryptography.X509Certificates.X509Certificate]$cert , [string]$subscriptionName , [string]$subscriptionid , [bool]$printDebugInfo=0)
{
    if ($printDebugInfo) {
        "subscriptionName = $subscriptionName"
        "subscriptionId = $subscriptionId"
    }
    Set-AzureSubscription $subscriptionName -Certificate $cert -SubscriptionId $subscriptionId
    Select-AzureSubscription -Current $subscriptionName
}

function Confirm-VariableIsDefined([string]$variablename)
{
	if (Test-Path variable:global:$variableName)
        {
            "$variableName=" + (Get-Variable $variableName).Value
        }
        else
        {
            throw [System.Exception] "Variable $variableName is not defined"
            Get-Member "$variableName"
        }
}

function Confirm-VariablesAreDefined([string[]]$variablenameArray) {
    foreach ($variableName in $variablenameArray)
    {
        Confirm-VariableIsDefined -variableName $variableName
    }
}


function New-AzureStorageAccountIfNotExists ([System.Security.Cryptography.X509Certificates.X509Certificate]$cert , [string]$subscriptionName , [string]$subscriptionid , [string]$storageAccountName , [string]$location , [bool]$printDebugInfo=0) {
    if ($printDebugInfo) {
        "subscriptionName = $subscriptionName"
        "subscriptionId = $subscriptionId"
        "storageAccountName = $storageAccountName"
        "location = $location"
    }
    Set-Subscription $cert $subscriptionName $subscriptionId $printDebugInfo


    $azureStorageKey = (Get-AzureStorageKey $storageAccountName | %{ $_.Primary })
    if ($printDebugInfo) {"azureStorageKey=$azureStorageKey"}
    
    if ( (Get-AzureStorageAccount -StorageAccountName $storageAccountName) -eq $null)
    {
        New-AzureStorageAccount -StorageAccountName $storageAccountName -Location $location;
    }
    else
    {
        "Storage account $StorageAccountName already exists";
    }


}


function New-AzureStorageContainerIfNotExists ([System.Security.Cryptography.X509Certificates.X509Certificate]$cert , [string]$subscriptionName , [string]$subscriptionid , [string]$storageAccountName , [string]$containerName, [bool]$printDebugInfo=0) {
    # Example usage
    #CreateStorageContainerIfNotExists -cert (Get-Item Cert:\CurrentUser\My\<thumbprint>) -subscriptionName <sub-name> -subscriptionid <sub-id> -storageAccountName <account-name> -containerName <container-name>
    if ($printDebugInfo) {
        "subscriptionName = $subscriptionName"
        "subscriptionId = $subscriptionId"
        "storageAccountName = $storageAccountName"
        "containerName = $containerName"
    }
    Set-Subscription $cert $subscriptionName $subscriptionId $printDebugInfo
    
    $azureStorageKey = (Get-AzureStorageKey $storageAccountName | %{ $_.Primary })
    if ($printDebugInfo) {"azureStorageKey=$azureStorageKey"}
    $azureStorageContext = (New-AzureStorageContext $storageAccountName -StorageAccountKey $azureStorageKey )
    if ($printDebugInfo) {"azureStorageContext=$azureStorageContext"}
    
    if (  (Get-AzureStorageContainer -Context $azureStorageContext | where {$_.Name -eq $ContainerName}) -eq $null)
    {
        New-AzureStorageContainer $ContainerName -Permission Off -Context $azureStorageContext
    }
    else
    {
        "Container $containerName already exists in storage account $storageAccountName"
    }         
}


function Set-AzureStorageBlobs ([System.Security.Cryptography.X509Certificates.X509Certificate]$cert , [string]$subscriptionName , [string]$subscriptionid , [string]$storageAccountName , [string]$containerName, [string]$sourceFolder , [string]$targetFolder ,  [bool]$printDebugInfo=0){
<#
    .SYNOPSIS 
      Upload contents of a folder to Azure BLOB Storage, respecting relative locations
    .EXAMPLE
     Set-AzureStorageBlobs -cert $cert -subscriptionName $subscriptionName -subscriptionid $subscriptionid -storageAccountName $storageAccountName -containerName $containerName -sourceFolder "c:\scripts\lib" -targetFolder "folder1/folder2"
     Upload files from "c:\scripts\lib" to "https://$storageAccountName.blob.core.windows.net/$containerName", prepending each BLOB name with "folder1/folder2" Any subfolders of "c:\scripts\lib" will also be preprended onto the BLOB name, thus giving the illusion of folders.
     .DESCRIPTION
     The Set-AzureStorageBlobs function uploads the contents of a defined folder to a container in Azure BLOB Storage. it will preserve subfolder locations by prepending those relative locations as part of the BLOB name, thus giving the illusion of a hierarchical file system.
#>
    if ($printDebugInfo) {
        "subscriptionName = $subscriptionName"
        "subscriptionId = $subscriptionId"
        "storageAccountName = $storageAccountName"
        "containerName = $containerName"
        "sourceFolder = $sourceFolder"
        "targetFolder = $targetFolder"
    }
    Set-Subscription $cert $subscriptionName $subscriptionId $printDebugInfo
    
    $azureStorageKey = (Get-AzureStorageKey $storageAccountName | %{ $_.Primary })
    if ($printDebugInfo) {"azureStorageKey=$azureStorageKey"}
    $azureStorageContext = (New-AzureStorageContext $storageAccountName -StorageAccountKey $azureStorageKey )
    if ($printDebugInfo) {"azureStorageContext=$azureStorageContext"}


    #useful article for uploading files http://blogs.msdn.com/b/shashankyerramilli/archive/2014/02/15/upload-files-to-blob-storage-using-azure-power-shell.aspx
    # $_.mode -match "-a---" scans the data directory and ony fetches the files. It filters out all directories
    $files = (Get-ChildItem $sourceFolder -force -recurse | Where-Object {$_.mode -match "-a---"}).FullName
 
    # iterate through all the files and start uploading data
    foreach ($file in $files)
    {
        #fq name represents fully qualified name
        $fqName = $file
        #upload the current file to the blob
        if ($printDebugInfo) {
            "Uploading $fqName"
        }
        if ($sourceFolder.LastIndexOf("\") -eq $sourceFolder.Length - 1)
        {
            $fileName = $file.Replace($sourceFolder,"")
        }
        else
        {
            $fileName = $file.Replace($sourceFolder + "\","")
        }
        Set-AzureStorageBlobContent -Blob "$targetFolder/$fileName" -Container $ContainerName -File $fqName -Context $azureStorageContext -Force
    }
}




function New-HDInsightClusterIfNotExists ([System.Security.Cryptography.X509Certificates.X509Certificate]$cert , [string]$subscriptionName , [string]$subscriptionid , [string]$storageAccountName , [string]$containerName, [string]$clusterName, [string]$location, [int]$clusterNodes, [string]$hdinsightVersion, [string]$clusterType, [bool]$printDebugInfo=0) {
    if ($printDebugInfo) {
        "subscriptionName = $subscriptionName"
        "subscriptionId = $subscriptionId"
        "storageAccountName = $storageAccountName"
        "containerName = $containerName"
        "clusterName = $clusterName"
        "location = $location"
        "clusterNodes = $clusterNodes"
        "clusterType = $clusterType"
        "hdinsightVersion=$hdinsightVersion"
    }
    Set-Subscription $cert $subscriptionName $subscriptionId $printDebugInfo
    
    if ($clusterType -eq ""){$clusterType = "Unknown"}
    
    $azureStorageKey = (Get-AzureStorageKey $storageAccountName | %{ $_.Primary })
    if ($printDebugInfo) {"azureStorageKey=$azureStorageKey"}
    $azureStorageContext = (New-AzureStorageContext $storageAccountName -StorageAccountKey $azureStorageKey )
    if ($printDebugInfo) {"azureStorageContext=$azureStorageContext"}


    if ((Get-AzureHDInsightCluster -Name $clusterName) -eq $null)
    {
        New-AzureHDInsightCluster 	-Name $clusterName -Location $location -DefaultStorageAccountName "$storageAccountName.blob.core.windows.net" -DefaultStorageAccountKey $storageAccountKey -DefaultStorageContainerName $containerName -ClusterSizeInNodes $clusterNodes -Version $hdinsightVersion $hdinsightVersion -ClusterType
    }
    else
    {
        "Cluster $clusterName already exists!!"
    }
}
