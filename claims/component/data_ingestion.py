from collections import namedtuple
from claims.entity.artifact_entity import DataIngestionArtifact
from claims.entity.metadata_entity import DataIngestionMetadata
from claims.entity.config_entity import DataIngestionConfig
from claims.exception import ClaimsException
from claims.logger import logging as  logger
import pyarrow.parquet as pq
import os,sys 
import uuid
import pandas as pd
import pyarrow as pa
import re
import time
import requests
import urllib.request
from typing import List
import json
from pyspark import SparkFiles
from claims.config.spark_manager import spark_session
from datetime import datetime

DownloadUrl = namedtuple("DownloadUrl", ["url", "file_path", "n_retry"])

class DataIngestion:

        # Used to download data in chunks.
    def __init__(self, data_ingestion_config: DataIngestionConfig, n_retry: int = 0):
        """
        data_ingestion_config: Data Ingestion config
        n_retry: Number of retry filed should be tried to download in case of failure encountered
        n_month_interval: n month data will be downloded
        """
        try:
            logger.info(f"{'>>' * 20}Starting data ingestion.{'<<' * 20}")
            self.data_ingestion_config = data_ingestion_config
            self.failed_download_urls: List[DownloadUrl] = []
            self.n_retry = n_retry

        except Exception as e:
            raise ClaimsException(e, sys)

    def download_files(self, n_day_interval_url: int = None):
        """
        n_month_interval_url: if not provided then information default value will be set
        =======================================================================================
        returns: List of DownloadUrl = namedtuple("DownloadUrl", ["url", "file_path", "n_retry"])
        """
        try:
            logger.info("Started downloading files") 
            datasource_url: str = self.data_ingestion_config.datasource_url
            logger.debug(f"DatasourceUrl: {datasource_url}")
            datasource_url: str = self.data_ingestion_config.datasource_url
            file_name = f"{self.data_ingestion_config.file_name}.csv"
            file_path = os.path.join(self.data_ingestion_config.download_dir, file_name)
            download_url = DownloadUrl(url=datasource_url, file_path=file_path, n_retry=self.n_retry)
            self.download_data(download_url=download_url)
            logger.info(f"File download completed")
        except Exception as e:
            raise ClaimsException(e, sys)


    def download_data(self, download_url: DownloadUrl):
        try:
            if download_url.n_retry == 0:
                logger.info(f"Starting download operation: {download_url}")
                download_dir = os.path.dirname(download_url.file_path)

                # download dir
                os.makedirs(download_dir, exist_ok=True)

                # data = pd.read_csv(download_url.url)
                #download data
                data = requests.get(download_url.url, params={'User-agent': f'your bot {uuid.uuid4()}'})

                # to handle throatling requestion and can be slove if we wait for some second.
                content = data.content.decode("utf-8")
                wait_second = re.findall(r'\d+', content)

                if len(wait_second) > 0:
                    time.sleep(int(wait_second[0]) + 2)
            # Writing response to understand why request was failed
                file_path = os.path.join(self.data_ingestion_config.failed_dir,
                                                os.path.basename(download_url.file_path))

                with open(download_url.file_path, "wb") as file_obj:
                    file_obj.write(data.content)

            # calling download function again to retry
                download_url = DownloadUrl(download_url.url, file_path=download_url.file_path,
                                        n_retry=download_url.n_retry - 1)
                self.download_data(download_url=download_url)

        except Exception as e:
            logger.info(e)
            raise ClaimsException(e, sys)



    def retry_download_data(self, data, download_url: DownloadUrl):
        """
        This function help to avoid failure as it help to download failed file again

        data:failed response
        download_url: DownloadUrl
        """
        try:
            # if retry still possible try else return the response
            if download_url.n_retry == 0:
                self.failed_download_urls.append(download_url)
                logger.info(f"Unable to download file {download_url.url}")
                return

            # to handle throatling requestion and can be slove if we wait for some second.
            content = data.content.decode("utf-8")
            wait_second = re.findall(r'\d+', content)

            if len(wait_second) > 0:
                time.sleep(int(wait_second[0]) + 2)

            # Writing response to understand why request was failed
            failed_file_path = os.path.join(self.data_ingestion_config.failed_dir,
                                            os.path.basename(download_url.file_path))
            os.makedirs(self.data_ingestion_config.failed_dir, exist_ok=True)
            with open(failed_file_path, "wb") as file_obj:
                file_obj.write(data.content)

            # calling download function again to retry
            download_url = DownloadUrl(download_url.url, file_path=download_url.file_path,
                                       n_retry=download_url.n_retry - 1)
            self.download_data(download_url=download_url)
        except Exception as e:
            raise ClaimsException(e, sys)


    def convert_files_to_parquet(self) -> str:
        """
        downloaded files will be converted and merged into single parquet file
        json_data_dir: downloaded json file directory
        data_dir: converted and combined file will be generated in data_dir
        output_file_name: output file name
        =======================================================================================
        returns output_file_path
        """
        try:
            csv_data_dir = self.data_ingestion_config.download_dir
            data_dir = self.data_ingestion_config.feature_store_dir
            os.makedirs(data_dir, exist_ok=True)
            logger.info(f"Csv file will be downloaded at: {csv_data_dir}")
            file_path = os.path.join(data_dir, f"{self.data_ingestion_config.file_name}")
            if not os.path.exists(csv_data_dir):
                return data_dir

            for file_name in os.listdir(csv_data_dir):
                csv_file_path = os.path.join(csv_data_dir, file_name)
                logger.debug(f"Converting {csv_file_path} into parquet format at {file_path}")
                df = spark_session.read.csv(csv_file_path, header=True)
                df.write.parquet(file_path, mode="overwrite")
            return file_path


        except Exception as e:
            raise ClaimsException(e, sys)


    def write_metadata(self, file_path: str) -> None:
        """
        This function help us to update metadata information 
        so that we can avoid redundant download and merging.

        """
        try:
            logger.info(f"Writing metadata info into metadata file.")
            metadata_info = DataIngestionMetadata(metadata_file_path=self.data_ingestion_config.metadata_file_path)

            metadata_info.write_metadata_info(
                                              data_file_path=file_path
                                              )
            logger.info(f"Metadata has been written.")
        except Exception as e:
            raise ClaimsException(e, sys)


    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        try:
            download_url = DownloadUrl
            logger.info(f"Started downloading json file")
            self.download_files()

            if os.path.exists(self.data_ingestion_config.download_dir):
                logger.info(f"Converting and combining downloaded json into parquet file")
                file_path = self.convert_files_to_parquet()
                self.write_metadata(file_path=file_path)

            feature_store_file_path = os.path.join(self.data_ingestion_config.feature_store_dir,
                                                    self.data_ingestion_config.file_name)
            artifact = DataIngestionArtifact(
                feature_store_file_path=feature_store_file_path,
                download_dir=self.data_ingestion_config.download_dir,
                metadata_file_path=self.data_ingestion_config.metadata_file_path,

            )

            logger.info(f"Data ingestion artifact: {artifact}")
            return artifact
        except Exception as e:
            raise ClaimsException(e, sys)

