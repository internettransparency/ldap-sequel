"""This script prints badkeys and pwnedkeys ips resulting from badkeys and pwnedkeys result files"""

import json
from datetime import datetime

BADKEYS_RESULTS_FILE = "../dataset/processing/key_analysis/badkeys_results.json"
PWNEDKEYS_RESULTS_FILE = "../dataset/processing/key_analysis/pwnedkeys_results.json"


def write_file(filecontent: str, out_path: str):
    with open(out_path, "w") as f:
        f.write(filecontent)


def read_json_file(path: str):
    with open(path, "r") as f:
        file_content = f.read()

    return json.loads(file_content)


def main():

    badkeys_results = read_json_file(BADKEYS_RESULTS_FILE)
    pwnedkeys_results = read_json_file(PWNEDKEYS_RESULTS_FILE)

    badkeys_ips = []
    pwnedkeys_ips = []

    print("## Badkeys ##")
    for badkey_entry in badkeys_results:
        res = badkey_entry.get("results")
        blocklist = res.get("blocklist")
        if not blocklist:
            continue
        ip = badkey_entry.get("ipv4")
        port = badkey_entry.get("port")
        badkeys_ips.append(f"{ip},{port}")
        print(f"{ip},{port}")

    print("")
    print("## Pwnedkeys ##")
    for pwnedkey_entry in pwnedkeys_results:
        res = pwnedkey_entry.get("pwned")
        if res:
            ip = pwnedkey_entry.get("ipv4")
            port = pwnedkey_entry.get("port")
            pwnedkeys_ips.append(f"{ip},{port}")
            print(f"{ip},{port}")

    print("")
    print("Pwnedkeys IPs not in badkeys")
    count_not_in_badkeys_ips = 0
    for ip in pwnedkeys_ips:
        if ip not in badkeys_ips:
            print(ip)
            count_not_in_badkeys_ips += 1
    print(f"Count pwnedkeys IPs not in badkeys: {count_not_in_badkeys_ips}")

    print("")
    print(f"Count badkeys IPs: {len(badkeys_ips)}")
    print(f"Count pwnedkeys IPs: {len(pwnedkeys_ips)}")


if __name__ == "__main__":
    now = datetime.now().strftime("%Y%m%d_%H%M%S")

    main()
