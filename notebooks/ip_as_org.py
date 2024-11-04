#!/usr/bin/env python3
"""
This file has to be copied
"""
__author__ = "Gustavo Luvizotto Cesar"
__email__ = "g.luvizottocesar@utwente.nl"

import pyasn
import pandas as pd

from utils import GetPyAsnDataset, GetAsOrgDataset


class IPASnPrefix(object):

    def __init__(self, date_list: list, dataset_dir: str):
        # Initialize module and load IP to ASN database
        p = GetPyAsnDataset(dataset_dir)
        if not p.all_files(date_list):
            raise AssertionError("Could not find all the necessary files")
        self.asndb_dict = {}
        for date_str in date_list:
            pyasn_dat = p.get_latest_file(date_str)
            self.asndb_dict[date_str] = pyasn.pyasn(pyasn_dat)

    def get_asn_from_ip(self, ip: str, when: str) -> int:
        """
        asd
        :param ip:
        :return:
        """
        db = self.asndb_dict[when]
        return db.lookup(ip)[0]

    def get_prefix_from_ip(self, ip: str, when: str) -> str:
        db = self.asndb_dict[when]
        return db.lookup(ip)[-1]

    def get_prefixes_from_asn(self, asn: int, when: str):
        """
        asd
        :param asn:
        :return:
        """
        db = self.asndb_dict[when]
        return db.get_as_prefixes(asn)


class ASOrg(object):

    def __init__(self, date_list: list, dataset_dir: str):
        as_org_dataset = GetAsOrgDataset(dataset_dir)
        if not as_org_dataset.all_files(date_list, "as-org_id"):
            raise AssertionError("Could not find all the necessary files")
        self.as_org_id_df_dict = {}
        self.org_id_name_c_df_dict = {}
        for date_str in date_list:
            as_org_id_file = as_org_dataset.get_latest_file(date_str, "as-org_id")
            self.as_org_id_df_dict[date_str] = pd.read_csv(as_org_id_file, sep="|")
            org_id_name_c_file = as_org_dataset.get_latest_file(date_str, "org_id-name-c")
            self.org_id_name_c_df_dict[date_str] = pd.read_csv(org_id_name_c_file, sep="|")

    def get_org_name_from_asn(self, asn: int, when: str) -> str:
        return self._get_org_name_country_from_asn(asn, "org_name", when)

    def _get_org_name_country_from_asn(self, asn, prop, when):
        as_org_id_df = self.as_org_id_df_dict[when]
        org_id_name_c_df = self.org_id_name_c_df_dict[when]
        org_id = as_org_id_df.loc[as_org_id_df["aut"] == asn]["org_id"]
        try:
            org_id_item = org_id.item()
            item = org_id_name_c_df.loc[org_id_name_c_df["org_id"] == org_id_item][prop]
        except ValueError as exc:
            #raise ValueError(f"Could not find {prop} for ASN {asn} at {when} item {org_id}") from exc
            return ""
        return item.item()

    def get_country_from_asn(self, asn: int, when: str) -> str:
        return self._get_org_name_country_from_asn(asn, "country", when)

    def get_asn_from_org_name(self, org_name: str, when: str) -> int:
        org_id_name_c_df = self.org_id_name_c_df_dict[when]
        def _has_org_in_dataset(org_actual, org_expected):
            if org_actual is None or pd.isna(org_actual):
                return False
            if org_expected.casefold() in org_actual.casefold().split(" "):
                return True
            return False
        #org_id = org_id_name_c_df.loc[org_id_name_c_df["org_name"].str.contains(org_name, case=False, na=False)]["org_id"]
        org_id_name_c_df["has_in_dataset"] = org_id_name_c_df["org_name"].apply(_has_org_in_dataset, args=(org_name,))
        org_id = org_id_name_c_df[org_id_name_c_df["has_in_dataset"] == True]["org_id"]
        as_org_id_df = self.as_org_id_df_dict[when]
        asn = as_org_id_df.loc[as_org_id_df["org_id"].isin(org_id)]["aut"].to_list()
        return asn
