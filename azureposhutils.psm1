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
function SetSubscription ([System.Security.Cryptography.X509Certificates.X509Certificate]$cert , [string]$subscriptionName , [string]$subscriptionid , [bool]$printDebugInfo=0)
{
    if ($printDebugInfo) {
        "subscriptionName = $subscriptionName"
        "subscriptionId = $subscriptionId"
    }
    Set-AzureSubscription $subscriptionName -Certificate $cert -SubscriptionId $subscriptionId
    Select-AzureSubscription -Current $subscriptionName
}

function CheckVariablesAreDefined([string]$variableList) {
    $variables = $variableList.Split(',')
    foreach ($variable in $variables)
    {
        if (Test-Path variable:global:$variable)
        {
            "$variable=" + (Get-Variable $variable).Value
        }
        else
        {
            throw [System.Exception] "Variable $variable is not defined"
            Get-Member "$variable"
        }
    }
}

function CreateStorageAccountIfNotExists ([System.Security.Cryptography.X509Certificates.X509Certificate]$cert , [string]$subscriptionName , [string]$subscriptionid , [string]$storageAccountName , [string]$location , [bool]$printDebugInfo=0) {
    if ($printDebugInfo) {
        "subscriptionName = $subscriptionName"
        "subscriptionId = $subscriptionId"
        "storageAccountName = $storageAccountName"
        "location = $location"
    }
    SetSubscription $cert $subscriptionName $subscriptionId $printDebugInfo

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

function CreateStorageContainerIfNotExists ([System.Security.Cryptography.X509Certificates.X509Certificate]$cert , [string]$subscriptionName , [string]$subscriptionid , [string]$storageAccountName , [string]$containerName, [bool]$printDebugInfo=0) {
    # Example usage
    #CreateStorageContainerIfNotExists -cert (Get-Item Cert:\CurrentUser\My\<thumbprint>) -subscriptionName <sub-name> -subscriptionid <sub-id> -storageAccountName <account-name> -containerName <container-name>
    if ($printDebugInfo) {
        "subscriptionName = $subscriptionName"
        "subscriptionId = $subscriptionId"
        "storageAccountName = $storageAccountName"
        "containerName = $containerName"
    }
    SetSubscription $cert $subscriptionName $subscriptionId $printDebugInfo
    
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

function UploadBlobs ([System.Security.Cryptography.X509Certificates.X509Certificate]$cert , [string]$subscriptionName , [string]$subscriptionid , [string]$storageAccountName , [string]$containerName, [string]$sourceFolder , [string]$targetFolder ,  [bool]$printDebugInfo=0){
    if ($printDebugInfo) {
        "subscriptionName = $subscriptionName"
        "subscriptionId = $subscriptionId"
        "storageAccountName = $storageAccountName"
        "containerName = $containerName"
        "sourceFolder = $sourceFolder"
        "targetFolder = $targetFolder"
    }
    SetSubscription $cert $subscriptionName $subscriptionId $printDebugInfo
    
    $azureStorageKey = (Get-AzureStorageKey $storageAccountName | %{ $_.Primary })
    if ($printDebugInfo) {"azureStorageKey=$azureStorageKey"}
    $azureStorageContext = (New-AzureStorageContext $storageAccountName -StorageAccountKey $azureStorageKey )
    if ($printDebugInfo) {"azureStorageContext=$azureStorageContext"}

    #useful article for uploading files http://blogs.msdn.com/b/shashankyerramilli/archive/2014/02/15/upload-files-to-blob-storage-using-azure-power-shell.aspx
    # $_.mode -match "-a---" scans the data directory and ony fetches the files. It filters out all directories
    $files = Get-ChildItem $sourceFolder -force| Where-Object {$_.mode -match "-a---"}
 
    # iterate through all the files and start uploading data
    foreach ($file in $files)
    {
        #fq name represents fully qualified name
        $fqName = $sourceFolder + "\" + $file.Name
        #upload the current file to the blob
        if ($printDebugInfo) {
            "Uploading $fqName"
        }
        $fileName = $file.Name
        Set-AzureStorageBlobContent -Blob "$targetFolder/$fileName" -Container $ContainerName -File $fqName -Context $azureStorageContext -Force
    }
}

function CreateHDInsightClusterIfNotExists ([System.Security.Cryptography.X509Certificates.X509Certificate]$cert , [string]$subscriptionName , [string]$subscriptionid , [string]$storageAccountName , [string]$containerName, [string]$clusterName, [string]$location, [int]$clusterNodes, [string]$hdinsightVersion, [bool]$printDebugInfo=0) {
    if ($printDebugInfo) {
        "subscriptionName = $subscriptionName"
        "subscriptionId = $subscriptionId"
        "storageAccountName = $storageAccountName"
        "containerName = $containerName"
        "clusterName = $clusterName"
        "location = $location"
        "clusterNodes = $clusterNodes"
        "hdinsightVersion=$hdinsightVersion"
    }
    SetSubscription $cert $subscriptionName $subscriptionId $printDebugInfo
    
    $azureStorageKey = (Get-AzureStorageKey $storageAccountName | %{ $_.Primary })
    if ($printDebugInfo) {"azureStorageKey=$azureStorageKey"}
    $azureStorageContext = (New-AzureStorageContext $storageAccountName -StorageAccountKey $azureStorageKey )
    if ($printDebugInfo) {"azureStorageContext=$azureStorageContext"}

    if ((Get-AzureHDInsightCluster -Name $clusterName) -eq $null)
    {
        New-AzureHDInsightCluster 	-Name $clusterName -Location $location -DefaultStorageAccountName "$storageAccountName.blob.core.windows.net" -DefaultStorageAccountKey $storageAccountKey -DefaultStorageContainerName $containerName -ClusterSizeInNodes $clusterNodes -Version $hdinsightVersion
    }
    else
    {
        "Cluster $clusterName already exists!!"
    }
}

