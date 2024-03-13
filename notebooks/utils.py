#!/usr/bin/env python3
"""
This file has to be copied
"""
__author__ = "Gustavo Luvizotto Cesar"
__email__ = "g.luvizottocesar@utwente.nl"

import os
import glob
from datetime import datetime
from abc import ABC, abstractmethod


class GetDatasetInterface(ABC):
    @abstractmethod
    def get_latest_file(self, date_str: str, folder: str = None) -> str:
        pass

    def _get_latest_file(self, date_str, pattern):
        dat_files = glob.glob(pattern)

        dat_files.sort(reverse=True)
        found_file = None
        for dat_file in dat_files:
            filename = os.path.basename(dat_file)
            actual_date_str = self._get_date_from_filename(filename)
            dat_file_date = datetime.strptime(actual_date_str, "%Y%m%d")
            desired_date = datetime.strptime(date_str, "%Y%m%d")
            if dat_file_date <= desired_date:
                found_file = dat_file
                break
        assert found_file is not None, "Could not find the closest file you were looking for"
        return found_file

    @abstractmethod
    def _get_date_from_filename(self, filename: str) -> str:
        pass

    @abstractmethod
    def all_files(self, date_list, folder: str = None):
        pass

    def _all_files(self, date_list, pattern):
        for date_str in date_list:
            try:
                _ = self._get_latest_file(date_str, pattern)
            except AssertionError:
                return False
        return True

class GetPyAsnDataset(GetDatasetInterface):
    pyasn_dat_dir = "pyasn-dataset"
    dataset_dir = None

    def __init__(self, dataset_dir: str):
        self.dataset_dir = dataset_dir

    def get_latest_file(self, date_str: str, folder: str = None) -> str:
        subdir = os.path.join(self.dataset_dir, self.pyasn_dat_dir)
        return self._get_latest_file(date_str, subdir + "/*.dat")

    def _get_date_from_filename(self, filename: str) -> str:
        filename_no_extension = os.path.splitext(filename)[0]
        return filename_no_extension.split("_")[-1]

    def all_files(self, date_list, folder: str = None):
        subdir = os.path.join(self.dataset_dir, self.pyasn_dat_dir)
        return self._all_files(date_list, subdir + "/*.dat")

class GetAsOrgDataset(GetDatasetInterface):
    as_org_dataset_dir = "as-org-dataset"
    dataset_dir = None

    def __init__(self, dataset_dir: str):
        self.dataset_dir = dataset_dir

    def get_latest_file(self, date_str: str, folder: str = None) -> str:
        subdir = os.path.join(self.dataset_dir, self.as_org_dataset_dir, folder)
        return self._get_latest_file(date_str, subdir + "/*.csv")

    def _get_date_from_filename(self, filename: str) -> str:
        filename_no_extension = os.path.splitext(filename)[0]
        return filename_no_extension.split(".")[0]

    def all_files(self, date_list, folder: str = None):
        subdir = os.path.join(self.dataset_dir, self.as_org_dataset_dir, folder)
        return self._all_files(date_list, subdir + "/*.csv")
