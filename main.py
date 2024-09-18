from fastapi import FastAPI
import boto3
from fastapi import HTTPException
from botocore.exceptions import ClientError
from pydantic import BaseModel
import os
from dotenv import load_dotenv

load_dotenv()


app = FastAPI()

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")

s3_client = boto3.client(
    "s3",
    region_name=aws_region,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)


@app.get("/")
async def root():
    return {"message": "hello from the api server 😃🤜🤛😁"}


@app.get("/buckets")
async def list_buckets():
    try:
        response = s3_client.list_buckets()
        buckets = [{"name": bucket["Name"]} for bucket in response["Buckets"]]
        return {"buckets": buckets}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing buckets: {str(e)}")


@app.get("/bucket/{bucket_name}/objects")
async def list_files_and_directories(bucket_name: str, prefix: str = ""):
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            Delimiter="/",
        )

        objects = []

        if "CommonPrefixes" in response:
            for cp in response["CommonPrefixes"]:
                objects.append({"key": cp["Prefix"], "type": "directory"})

        if "Contents" in response:
            for obj in response["Contents"]:
                if obj["Key"] != prefix:
                    objects.append(
                        {"key": obj["Key"], "type": "file", "size": obj["Size"]}
                    )

        return {"bucket": bucket_name, "prefix": prefix, "objects": objects}
    except s3_client.exceptions.NoSuchBucket:
        raise HTTPException(status_code=404, detail=f"Bucket '{bucket_name}' not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing objects: {str(e)}")
