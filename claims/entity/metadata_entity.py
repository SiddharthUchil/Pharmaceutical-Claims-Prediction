from claims.exception import ClaimsException
import os, sys

from claims.utils import read_yaml_file, write_yaml_file
from collections import namedtuple
from claims.logger import logging as logger

DataIngestionMetadataInfo = namedtuple("DataIngestionMetadataInfo", ["data_file_path"])


class DataIngestionMetadata:

    def __init__(self, metadata_file_path):
        self.metadata_file_path = metadata_file_path


    @property
    def is_metadata_file_present(self):
        return os.path.exists(self.metadata_file_path)

    def write_metadata_info(self, data_file_path: str):
        try:
            metadata_info = DataIngestionMetadataInfo(
                data_file_path=data_file_path
            )
            write_yaml_file(file_path=self.metadata_file_path, data=metadata_info._asdict())

        except Exception as e:
            raise ClaimsException(e, sys)

    def get_metadata_info(self) -> DataIngestionMetadataInfo:
        try:
            if not self.is_metadata_file_present:
                raise Exception("Metadata file unavailable")
            metadata = read_yaml_file(self.metadata_file_path)
            metadata_info = DataIngestionMetadataInfo(**(metadata))
            logger.info(metadata)
            return metadata_info
        except Exception as e:
            raise ClaimsException(e, sys)