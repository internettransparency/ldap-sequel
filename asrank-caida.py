#!/usr/bin/env python3
__author__ = "Bradley Huffaker"
__email__ = "<bradley@caida.org>"
# This software is Copyright (C) 2020 The Regents of the University of
# California. All Rights Reserved. Permission to copy, modify, and
# distribute this software and its documentation for educational, research
# and non-profit purposes, without fee, and without a written agreement is
# hereby granted, provided that the above copyright notice, this paragraph
# and the following three paragraphs appear in all copies. Permission to
# make commercial use of this software may be obtained by contacting:
#
# Office of Innovation and Commercialization
#
# 9500 Gilman Drive, Mail Code 0910
#
# University of California
#
# La Jolla, CA 92093-0910
#
# (858) 534-5815
#
# invent@ucsd.edu
#
# This software program and documentation are copyrighted by The Regents of
# the University of California. The software program and documentation are
# supplied “as is”, without any accompanying services from The Regents. The
# Regents does not warrant that the operation of the program will be
# uninterrupted or error-free. The end-user understands that the program
# was developed for research purposes and is advised not to rely
# exclusively on the program for any reason.
#
# IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
# DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES,
# INCLUDING LOST PR OFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
# DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF
# THE POSSIBILITY OF SUCH DAMAGE. THE UNIVERSITY OF CALIFORNIA SPECIFICALLY
# DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
# SOFTWARE PROVIDED HEREUNDER IS ON AN “AS IS” BASIS, AND THE UNIVERSITY OF
# CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
# ENHANCEMENTS, OR MODIFICATIONS.
#

# this code is extracted from https://api.asrank.caida.org/v2/docs#graphql
# this code has been modified to the needs of the project
import argparse
import os
import sys
import json
import requests
from pathlib import Path

import pandas as pd

URL = "https://api.asrank.caida.org/v2/graphql"
decoder = json.JSONDecoder()
encoder = json.JSONEncoder()

#method to print how to run script
def print_help():
    print(sys.argv[0], "input.csv 2024-10-01 2024-11-13")

######################################################################
## Parameters
######################################################################
parser = argparse.ArgumentParser()
parser.add_argument("inputfile", type=str, help="Input csv file. The file should have a column named 'asn'.")
parser.add_argument("start_date", type=str, help="Start date in the format 'YYYY-MM-DD'.")
parser.add_argument("end_date", type=str, help="End date in the format 'YYYY-MM-DD'.")
args = parser.parse_args()

######################################################################
## Main code
######################################################################
def main():
    if args.inputfile is None or args.start_date is None or args.end_date is None:
        parser.print_help()
        return

    asn_list = get_asn_list(args.inputfile)
    result = query_asn(asn_list, step=200)

    pdf = aggregate_asn_data(result)
    attempts = 3
    for attempt in range(attempts):
        if len(pdf) != len(asn_list):
            print("attempt", attempt)
            missing_asn = set(asn_list) - set(pdf['asn'].astype(str).tolist())
            result = query_asn(list(missing_asn), step=2)
            pdf = pd.concat([pdf, aggregate_asn_data(result)], ignore_index=True)
        else:
            break

    directory = os.path.dirname(args.inputfile)
    output_file = os.path.join(directory, Path(args.inputfile).stem + "_ranked.csv")
    pdf.to_csv(output_file, index=False)


def get_asn_list(inputfile):
    # read the input file
    pdf = pd.read_csv(inputfile)
    return pdf['asn'].astype(int).astype(str).tolist()


def aggregate_asn_data(result):
    nodes = result['data']['asns']['edges']
    records = [{'asn': node['node']['asn'], 'rank': node['node']['rank'], 'date': node['node']['date']} for node in nodes]
    return pd.DataFrame(records)


def query_asn(asn_list, step):
    result = {
        'data': {
            'asns': {
                'edges': []
            }
        }
    }
    for i in range(0, len(asn_list), step):
        print("step", i, "/", len(asn_list))
        query = asn_query(asn_list[i:i+step], args.start_date, args.end_date)
        request = requests.post(URL, json={'query': query}, timeout=10*60)
        if request.status_code == 200:
            result['data']['asns']['edges'].extend(request.json()["data"]['asns']['edges'])
        else:
            print("Query failed to run returned code of %d " % (request.status_code))

    return result

######################################################################
## Queries
######################################################################
def asn_query(asn_list, start_date, end_date):
    asn_list_str = str(asn_list).replace("'", '"')
    return """{
        asns(asns:%s, dateStart:"%s", dateEnd:"%s") {
            edges {
                node {
                    asn
                    rank
                    date
                }
            }
        }
    }""" % (asn_list_str, start_date, end_date)


if __name__ == "__main__":
    main()
