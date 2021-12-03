import os
import urllib3
import glob


# postgresql+psycopg2://user:password@uri:5432/pid_manager
PID_DATABASE_DSN = os.environ.get("PID_DATABASE_DSN")
PID_DATABASE_TIMEOUT = os.environ.get("PID_DATABASE_TIMEOUT")

# mongodb://my_user:my_password@127.0.0.1:27017/my_db
DATABASE_CONNECT_URL = os.environ.get("DATABASE_CONNECT_URL")


def get_db_uri():
    # mongodb://my_user:my_password@127.0.0.1:27017/my_db
    if not DATABASE_CONNECT_URL:
        raise ValueError(
            f"Missing value for environment variable DATABASE_CONNECT_URL. "
            "DATABASE_CONNECT_URL=mongodb://my_user:my_password@127.0.0.1:27017/my_db"
        )
    return DATABASE_CONNECT_URL


def get_pid_manager():
    if PID_DATABASE_DSN:
        from scielo_v3_manager.pid_manager import Manager

        return Manager(
            PID_DATABASE_DSN,
            timeout=PID_DATABASE_TIMEOUT,
        )
