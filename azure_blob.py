"""
Author : Rajwinder Singh Walia
Description : Module using azure blob storage client library
"""

from azure.storage.blob import BlobServiceClient, generate_account_sas, ResourceTypes, AccountSasPermissions
from azure_cli import getstorageaccounturl, getstorageaccountkeys, getstorageaccountconnectionstring
from azure.identity import DefaultAzureCredential ## Use Service principal to authenticate the user
from azure.core import exceptions
import os
from datetime import datetime, timedelta
import json
import time
import sys

class AzureBlob:
    """Wrapper around Azure Blob Storage Client library"""

    """Types of access(s) suppported
    1. Azure AD
    2. Shared Access Signature (SAS)
    3. Shared Key (Storage account access key)
    """

    # Types of authentication supported to get the credentials
    authenticationlist = ['azure-ad', 'sas', 'storage-account-key']
    
    def __init__(self, resourcegroup : str, storageaccount : str, authenticationtype : str):
        self.resourcegroup = resourcegroup
        self.storageaccount = storageaccount
        self.authenticationtype = authenticationtype

        if self.authenticationtype not in AzureBlob.authenticationlist:
             raise exceptions.AzureError("Authentication type provided is incorrect. It should be one out of {}".format(AzureBlob.authenticationlist))

    def _getstorageaccountkey(self):
        """This will return a list of dictionaries for the two acocunt access keys"""

        _account_keys = getstorageaccountkeys(storageaccountname=self.storageaccount, resourcegroup=self.resourcegroup)
        return _account_keys

    def _create_ad_token(self):
        """Make sure Azure tenant, client id and secret are defined as environment variables"""

        ad_environment_variables = ['AZURE_TENANT_ID', 'AZURE_CLIENT_ID', 'AZURE_CLIENT_CERTIFICATE_PATH']
        
        vars_undefined = [env for env in ad_environment_variables if env not in os.environ]
        if  vars_undefined:
            raise exceptions.AzureError("Environment variables {}  are not defined. They are required to make a connection using service principal".format(vars_undefined))
        else:
            # Derive the crednetial token and make it a class attribute
            self.credential_token = DefaultAzureCredential()

    def _create_sas_token(self):
        """Generate the shared access signature token using the storage account access key"""

        account_keys = self._getstorageaccountkey()
        # get one of the account keys
        account_keys = json.loads(account_keys)[0]['value']

        self.credential_token = generate_account_sas(
            account_name=self.storageaccount, 
            account_key=account_keys, 
            resource_types=ResourceTypes(service=True),
            permission=AccountSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=1)
            )

    def _create_key_token(self):
        """Get the account access key and use that as a credential token"""

        self.credential_token = self._getstorageaccountkey()[0]['value']

    def createCredential(self):
        """Create object for credential required to create a blob client to work with the blob data"""

        if self.authenticationtype == "azure-ad":
            self._create_ad_token()
        elif self.authenticationtype == "sas":
            self._create_sas_token()
        else: ## if authentication is storage-account-key
            self._create_key_token()

    def createBlobClientusingCredential(self, credentialtoken):
        """Create Blob Client using credential token"""
        
        client = BlobServiceClient(
            account_url=getstorageaccounturl(storageaccountname=self.storageaccount, resourcegroup=self.resourcegroup, storagetype='blob'), 
            credential=credentialtoken
            )

        return client

    def createBlobClientusingConnectionstring(self):
        """Create Blob client using blob account connection string which has the access key in it so there is no need to provide extra credentials"""

        connectionstring = getstorageaccountconnectionstring(storageaccountname=self.storageaccount, resourcegroup=self.resourcegroup)
        client = BlobServiceClient.from_connection_string(conn_str=connectionstring)

        return client

    def create_container_client(self, BlobServiceClient, Containername):
        """Create a container client from Blob service client"""

        client = BlobServiceClient.get_container_client(Containername)
        return client

    def create_container(self, containerclient):
        """Create container using the container client"""

        containerclient.create_container()

    def create_blob_client(self, BlobServiceClient, Containername, Blobname):
        """Create a blob client using Blob service client"""

        client = BlobServiceClient.get_blob_client(container=Containername, blob=Blobname)
        return client

    def create_blob(seld, blobclient):
        """Create blob using the blob client"""

        blobclient.create_append_blob()

    def check_if_blob_exists(self, ContainerClient, blobname):
        """See if the provided blob exists or not and returns boolean flag"""

        flag = False
        #get the list of containers using service client
        blobs = ContainerClient.list_blobs()
        for blob in blobs:
            if blob.name == blobname:
                flag = True
                break

        return flag

    def check_if_container_exists(self, BlobServiceClient, containername):
        """See if the provided container exists or not and returns boolean flag"""

        flag = False
        #get the list of containers using service client
        containers = BlobServiceClient.list_containers()
        for container in containers:
            if container.name == containername:
                flag = True
                break

        return flag

    def upload_blob(self, blobclient, file_to_upload):
        """Uploades a file into blob"""

        with open(file_to_upload, "rb") as data:
            blobclient.upload_blob(data, blob_type="AppendBlob")

    def download_blob(self, blobclient, file_to_download_into):
        """Downloads blob data into the provided file"""

        with open(file_to_download_into, "wb") as download:
            stream = blobclient.download_blob()
            download.write(stream.readall())

    def delete_blob(self, blobclient, lease=None):
        """Delete blob using blobclient"""

        blobclient.delete_blob(lease=lease)

    def delete_container(self, containerclient):
        """Delete container using container client"""

        containerclient.delete_container()

    def close_clients(self, client):
        """Close any client be it service/container/blob"""

        client.close()

    def get_containers(self, Blobserviceclient):
        """Get list of containers using the service client"""

        containers = Blobserviceclient.list_containers()

        return [container.name for container in containers]

    def get_blobs(self, containerclient):
        """Get list of blobs under a particular container using the container client"""

        blobs = containerclient.list_blobs()

        return [blob.name for blob in blobs]

    def get_blob_lease(self, blobclient):
        """Get lease(lock) on blob for write/delete operations"""

        lease = blobclient.acquire_lease()
        return lease

    


