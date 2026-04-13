import subprocess
from src.utils.logger import setup_logger
import os

logger = setup_logger()

HDFS_BASE = "hdfs://namenode:9000"


def upload_to_hdfs(local_path, hdfs_path):
    try:
        cmd = f"hdfs dfs -fs hdfs://namenode:9000 -put -f {local_path} {hdfs_path}"
        subprocess.run(cmd, shell=True, check=True)

        logger.info(f"Uploaded {local_path} → HDFS:{hdfs_path}")

    except Exception as e:
        logger.error(f"HDFS upload failed: {e}")
        raise

def list_hdfs(path="/data"):
    cmd = f"hdfs dfs -ls {path}"
    subprocess.run(cmd, shell=True)