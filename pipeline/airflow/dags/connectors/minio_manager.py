from contextlib import contextmanager
from minio import Minio


@contextmanager
def get_minio_client(minio_conf):
    try:
        client = Minio(
            endpoint=minio_conf.get("endpoint_url"),
            access_key=minio_conf.get("access_key"),
            secret_key=minio_conf.get("secret_key"),
            secure=False,
        )

        yield client
    except Exception as e:
        raise Exception(f"Error while creating minio client: {e}")
