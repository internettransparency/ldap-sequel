#!/usr/bin/env python3
"""
extract all dates preset in a bucket subpath
"""
__author__ = "Gustavo Luvizotto Cesar"
__email__ = "g.luvizottocesar@utwente.nl"

import argparse
import re
from collections import defaultdict
from datetime import datetime

import pandas as pd

from objstore import ObjStore


def main(args):
    dates_dict = defaultdict(int)
    objstore = ObjStore("censys")
    for obj in objstore.get_bucket().objects.filter(Prefix="dataset=" + args.dataset + "/format=avro/"):
        s3path = obj.key
        date_match = re.search(r"year=(\d+)/month=(\d+)/day=(\d+)", s3path)
        if date_match:
            year = int(date_match.group(1))
            month = int(date_match.group(2))
            day = int(date_match.group(3))

            extracted_datetime = datetime(year, month, day)
            dates_dict[extracted_datetime] += 1
        else:
            print(f"Could not extract date from {s3path}")

    if False:
        page_iterator = objstore.get_pages("dataset=" + args.dataset + "/format=avro/")
        for page in page_iterator:
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                s3path = obj['Key']
                date_match = re.search(r"year=(\d+)/month=(\d+)/day=(\d+)", s3path)
                if date_match:
                    year = int(date_match.group(1))
                    month = int(date_match.group(2))
                    day = int(date_match.group(3))

                    extracted_datetime = datetime(year, month, day)
                    dates_dict[extracted_datetime] += 1
                else:
                    print(f"Could not extract date from {s3path}")

    timestamps = []
    for ts in dates_dict.keys():
        timestamps.append(ts.strftime("%Y-%m-%d"))
    filename = f"censys_{args.dataset}_dates.csv"
    pd.DataFrame(timestamps, columns=["date"]).to_csv(filename, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, type=str,
                        help="Dataset name. E.g., universal-internet-dataset-v2-ipv4-virtual-hosts,"
                        "universal-internet-dataset-v2-ipv4 or universal-internet-dataset")
    main(parser.parse_args())
