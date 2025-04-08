import os

from datacontract.model.exceptions import DataContractException
from datacontract.model.run import Run, ResultEnum


def yield_s3_files(run: Run, s3_endpoint_url, s3_location):
    fs = s3_fs(s3_endpoint_url)
    files = fs.glob(s3_location)
    for file in files:
        with fs.open(file) as f:
            run.log_info(f"Downloading file {file}")
            yield f.read()


def s3_fs(s3_endpoint_url):
    try:
        import s3fs
    except ImportError as e:
        raise DataContractException(
            type="schema",
            result=ResultEnum.failed,
            name="s3 extra missing",
            reason="Install the extra s3 to use s3",
            engine="datacontract",
            original_exception=e,
        )

    aws_access_key_id = os.getenv("DATACONTRACT_S3_ACCESS_KEY_ID")
    if aws_access_key_id is None:
        raise DataContractException(
            type="schema",
            result=ResultEnum.failed,
            name="s3 env. variable DATACONTRACT_S3_ACCESS_KEY_ID missing",
            reason="configure export DATACONTRACT_S3_ACCESS_KEY_ID=*** ",
            engine="datacontract",
            original_exception=e,
        )
    
    aws_secret_access_key = os.getenv("DATACONTRACT_S3_SECRET_ACCESS_KEY")
    if aws_secret_access_key is None:
        raise DataContractException(
            type="schema",
            result=ResultEnum.failed,
            name="s3 env. variable DATACONTRACT_S3_SECRET_ACCESS_KEY missing",
            reason="configure export DATACONTRACT_S3_SECRET_ACCESS_KEY=*** ",
            engine="datacontract",
            original_exception=e,
        )
    
    aws_session_token = os.getenv("DATACONTRACT_S3_SESSION_TOKEN")
    if aws_session_token is None:
        raise DataContractException(
            type="schema",
            result=ResultEnum.failed,
            name="s3 env. variable DATACONTRACT_S3_SESSION_TOKEN missing",
            reason="configure export DATACONTRACT_S3_SESSION_TOKEN=*** ",
            engine="datacontract",
            original_exception=e,
        )

    return s3fs.S3FileSystem(
        key=aws_access_key_id,
        secret=aws_secret_access_key,
        token=aws_session_token,
        anon=aws_access_key_id is None,
        client_kwargs={"endpoint_url": s3_endpoint_url},
    )
