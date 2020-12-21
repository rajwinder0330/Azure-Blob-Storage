"""create clients, container, blob and upload and download data"""

from azure_blob import AzureBlob

"""Create a service client"""
blob = AzureBlob(resourcegroup='azurelearning', storageaccount='azuredataengineer2', authenticationtype='sas')
serviceclient = blob.createBlobClientusingConnectionstring()


"""Create a container using container client"""
container_client = blob.create_container_client(BlobServiceClient=serviceclient, Containername='rajwinderblobtest')

if blob.check_if_container_exists(BlobServiceClient=serviceclient, containername=container_client.container_name):
    print("Container {} already exists ..".format(container_client.container_name))
else:
    blob.create_container(containerclient=container_client)
    print("container {} has been created..".format(container_client.container_name))

"""Create a Blob using blob client"""
blob_client = blob.create_blob_client(BlobServiceClient=serviceclient, Containername=container_client.container_name, Blobname='textdatablob')

if blob.check_if_blob_exists(ContainerClient=container_client, blobname=blob_client.blob_name):
    print("blob {} already exists ..".format(blob_client.blob_name))
else:
    blob.create_blob(blob_client)
    print("blob {} has been created..".format(blob_client.blob_name))

"""Upload data into  blob"""
blob.upload_blob(blobclient=blob_client, file_to_upload='E:\python\\azure\Test_file_to_upload_blob.txt')

"""Download blob data into a file"""
blob.download_blob(blobclient=blob_client, file_to_download_into='E:\python\\azure\Test_file_to_upload_blob.txt_download')
