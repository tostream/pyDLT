from typing import Optional, Type, Any
from pyspark.sql import SparkSession
from datetime import datetime,timedelta
import fnmatch

def get_dbutils(spark: SparkSession) -> Optional[Type]:
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        return None

def get_sercet(dbutils: callable,scope: str, key:str ) -> Optional[str]:
    if dbutils:
        return dbutils.secrets.get(scope, key)
    return None


def get_adls_path(file_system: str,account_name: str, path:str ="" ) -> str:
    return f"abfss://{file_system}@{account_name}.dfs.core.windows.net/{path}"

def archive_files(*args: Any, **kwargs: Any) -> None:
    processed_files = kwargs.get("processed_files")
    internal_storage = kwargs.get("internal_storage")
    curDate = datetime.now()
    curDateStr = curDate.strftime("%Y%m%d")
    archive_folder = f"archive/{curDateStr}/"
    # file_list = internal_storage.get_file_list(processed_files)
    file_list = internal_storage.get_file_list()
    def filter_logic(x):
        return fnmatch.fnmatch(x['name'],f"*{processed_files}") \
        and not fnmatch.fnmatch(x['name'],f"{internal_storage.get_directory()}archive*")
    # filter_name = lambda x : fnmatch.fnmatch(x['name'],f"*{processed_files}")
    filename_list = filter(filter_logic,file_list)
    for updated_file in filename_list:
        file_name = updated_file["name"]
        archive_file_name = file_name.replace(internal_storage.get_directory(),f"{archive_folder}")
        file_bytes = internal_storage.get_file(file_name.replace(internal_storage.get_directory(),""))
        internal_storage.put_file(archive_file_name,file_bytes)
        internal_storage.delete_file(file_name.replace(internal_storage.get_directory(),""))
    cleanupDate = curDate - timedelta(days=365)
    cleanupDateStr = cleanupDate.strftime("%Y%m%d")
    archFolderList = internal_storage.get_file_list("archive")
    for obj in archFolderList:
        match obj["is_directory"]:
            case True:
                tmpFolderName = obj["name"]
                checkName = tmpFolderName.replace(f"{internal_storage.get_directory()}archive/",'')
                if int(cleanupDateStr) > int(checkName):
                    file_system_client = internal_storage.conn.get_file_system_client(file_system=internal_storage.get_file_system())
                    directory_client = file_system_client.get_directory_client(
                    internal_storage.get_directory()+f"archive/{checkName}/")
                    directory_client.delete_directory()