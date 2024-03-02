#!/usr/bin/env python3
"""
This file has to be copied
"""
__author__ = "Gustavo Luvizotto Cesar"
__email__ = "g.luvizottocesar@utwente.nl"

import pyasn
from utils import GetPyAsnDataset, GetAsOrgDataset
import pandas as pd


class IPASnPrefix(object):

    def __init__(self, date_str: str, dataset_dir: str):
        # Initialize module and load IP to ASN database
        p = GetPyAsnDataset(dataset_dir)
        pyasn_dat = p.get_latest_file(date_str)
        self.asndb = pyasn.pyasn(pyasn_dat)

    def get_asn_from_ip(self, ip: str) -> int:
        """
        asd
        :param ip:
        :return:
        """
        return self.asndb.lookup(ip)[0]

    def get_prefix_from_ip(self, ip: str) -> str:
        return self.asndb.lookup(ip)[-1]

    def get_prefixes_from_asn(self, asn: int):
        """
        asd
        :param asn:
        :return:
        """
        return self.asndb.get_as_prefixes(asn)


class ASOrg(object):

    def __init__(self, date_str: str, dataset_dir: str):
        as_org_dataset = GetAsOrgDataset(dataset_dir)
        as_org_id_file = as_org_dataset.get_latest_file(date_str, "as-org_id")
        self.as_org_id_df = pd.read_csv(as_org_id_file, sep="|")
        org_id_name_c_file = as_org_dataset.get_latest_file(date_str, "org_id-name-c")
        self.org_id_name_c_df = pd.read_csv(org_id_name_c_file, sep="|")

    def get_org_name_from_asn(self, asn: int) -> str:
        return self._get_org_name_country_from_asn(asn, "org_name")

    def _get_org_name_country_from_asn(self, asn, property):
        org_id = self.as_org_id_df.loc[self.as_org_id_df["aut"] == asn]["org_id"]
        item = self.org_id_name_c_df.loc[self.org_id_name_c_df["org_id"] ==
                                             org_id.item()][property]
        return item.item()

    def get_country_from_asn(self, asn: int) -> str:
        return self._get_org_name_country_from_asn(asn, "country")
