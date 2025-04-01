import os

from datacontract.model.exceptions import DataContractException
from datacontract.model.run import Run, ResultEnum


def yield_az_files(run: Run, az_storageAccount, az_location):
    fs = az_fs(az_storageAccount)
    files = fs.glob(az_location)
    for file in files:
        with fs.open(file) as f:
            run.log_info(f"Downloading file {file}")
            yield f.read()


def az_fs(az_storageAccount):
    try:
        import adlfs
    except ImportError as e:
        raise DataContractException(
            type="schema",
            result=ResultEnum.failed,
            name="az extra missing",
            reason="Install the extra datacontract-cli\\[azure] to use az",
            engine="datacontract",
            original_exception=e,
        )

    az_client_id = os.getenv("DATACONTRACT_AZURE_CLIENT_ID")
    if az_client_id is None:
        raise DataContractException(
            type="schema",
            result=ResultEnum.failed,
            name="az env. variable DATACONTRACT_AZURE_CLIENT_ID missing",
            reason="configure export DATACONTRACT_AZURE_CLIENT_ID=*** ",
            engine="datacontract",
            original_exception=e,
        )

    az_client_secret = os.getenv("DATACONTRACT_AZURE_CLIENT_SECRET")
    if az_client_secret is None:
        raise DataContractException(
            type="schema",
            result=ResultEnum.failed,
            name="az env. variable DATACONTRACT_AZURE_CLIENT_SECRET missing",
            reason="configure export DATACONTRACT_AZURE_CLIENT_SECRET=*** ",
            engine="datacontract",
            original_exception=e,
        )
    
    az_tenant_id = os.getenv("DATACONTRACT_AZURE_TENANT_ID")
    if az_tenant_id is None:
        raise DataContractException(
            type="schema",
            result=ResultEnum.failed,
            name="az env. variable DATACONTRACT_AZURE_TENANT_ID missing",
            reason="configure export DATACONTRACT_AZURE_TENANT_ID=*** ",
            engine="datacontract",
            original_exception=e,
        )

    return adlfs.AzureBlobFileSystem(
        account_name=az_storageAccount,
        client_id=az_client_id,
        client_secret=az_client_secret,
        tenant_id=az_tenant_id,
        anon=az_client_id is None,
    )
