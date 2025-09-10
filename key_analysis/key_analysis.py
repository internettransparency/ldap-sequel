"""This script reads badkeys and pwnedkeys results and builds statistics."""

import json
from collections import OrderedDict, defaultdict

BADKEYS_RESULT_FILE = "../dataset/processing/key_analysis/badkeys_results.json"
PWNEDKEYS_RESULT_FILE = "../dataset/processing/key_analysis/pwnedkeys_results.json"


def read_json_file(path):
    with open(path, "r") as fp:
        content = fp.read()
        json_content = json.loads(content)

    return json_content


def get_badkeys(result_dict):
    "Returns entries where badkeys where detected"

    badkeys_list = [entry for entry in result_dict if entry.get("results")]
    return badkeys_list


def get_pwnedkeys(result_dict, badkeys_results):
    """Searches for entries where keys are pwned according to pwnedkeys.com.
    Converts entries in the same format as badkeys entries and merges results."""

    pwnedkeys_list = []
    for pk_entry in result_dict:
        if pk_entry.get("pwned"):
            pk_unique_id = get_unique_entry_identifier(pk_entry)
            key_type = ""
            key_size = 0
            for bk_entry in badkeys_results:
                if pk_unique_id == get_unique_entry_identifier(bk_entry):
                    spki = bk_entry.get("spki", "")
                    key_type = bk_entry.get("type", "")
                    key_size = bk_entry.get("bits", 0)
                    break

            pk_entry.update(
                {
                    "spki": spki,
                    "type": key_type,
                    "bits": key_size,
                    "results": {"blocklist": {"subtest": "pwnedkeys"}},
                }
            )
            pwnedkeys_list.append(pk_entry)
    return pwnedkeys_list


def get_unique_entry_identifier(entry):
    """Returns a unique pingerprint for each entry"""
    return (
        entry.get("ipv4"),
        entry.get("port"),
        entry.get("timestamp"),
        entry.get("hash"),
    )


if __name__ == "__main__":

    badkeys_results = read_json_file(BADKEYS_RESULT_FILE)
    pwnedkeys_results = read_json_file(PWNEDKEYS_RESULT_FILE)

    badkeys_list = get_badkeys(badkeys_results)
    print(f"badkeys: {len(badkeys_list)}")
    pwnedkeys_list = get_pwnedkeys(pwnedkeys_results, badkeys_results)
    print(f"pwnedkeys: {len(pwnedkeys_list)}")

    merged_list = []

    bk_ip_port = []
    for bk in badkeys_list:
        merged_list.append(bk)
        bk_ip_port.append(get_unique_entry_identifier(bk))

    pk_ip_port = []
    for pk in pwnedkeys_list:
        if get_unique_entry_identifier(pk) in bk_ip_port:
            continue
        else:
            merged_list.append(pk)

    print(f"merged results: {len(merged_list)}")

    isp_grouping = defaultdict(list)
    usage_type_grouping = defaultdict(list)

    server_name_count = defaultdict(int)
    isp_count = defaultdict(int)
    usage_type_count = defaultdict(int)
    issuer_count = defaultdict(int)
    subject_count = defaultdict(int)
    chain_error_count = defaultdict(int)
    badkeys_blocklist_subtest_count = defaultdict(int)
    spki_count = defaultdict(int)
    spki_correlation = defaultdict(dict)
    key_size_count = defaultdict(int)
    key_type_count = defaultdict(int)

    for result in merged_list:
        spki = result.get("spki")
        key_type = result.get("type")
        key_size = result.get("bits")
        ip = result.get("ipv4")
        server_name = result.get("server_name")
        isp = result.get("isp")
        usage_type = result.get("usage_type")
        issuer = result.get("issuer").get("common_name")
        subject = result.get("subject").get("common_name")
        chain_error = result.get("chain_error")
        badkeys_tests_dict = result.get("results")  # also pwnedkeys results here
        badkeys_blocklist_test = badkeys_tests_dict.get("blocklist")
        badkeys_blocklist_subtest = badkeys_blocklist_test.get("subtest")

        isp_grouping[isp].append(ip)
        usage_type_grouping[usage_type].append(ip)
        server_name_count[server_name] += 1
        isp_count[isp] += 1
        usage_type_count[usage_type] += 1
        issuer_count[issuer] += 1
        subject_count[subject] += 1
        chain_error_count[chain_error] += 1
        badkeys_blocklist_subtest_count[badkeys_blocklist_subtest] += 1
        spki_count[spki] += 1
        key_type_count[key_type] += 1
        key_size_count[key_size] += 1

        if spki_correlation.get(spki):
            spki_correlation[spki]["count"] += 1
            spki_correlation[spki]["isp"][isp] += 1
            spki_correlation[spki]["issuer"][issuer] += 1
            spki_correlation[spki]["subject"][subject] += 1
            spki_correlation[spki]["usage_type"][usage_type] += 1
            spki_correlation[spki]["chain_error"][chain_error] += 1
            spki_correlation[spki]["server_name"][server_name] += 1
            spki_correlation[spki]["key_size"][key_size] += 1
            spki_correlation[spki]["key_type"][key_type] += 1
            spki_correlation[spki]["badkeys-pwnedkeys"][badkeys_blocklist_subtest] += 1
        else:
            spki_correlation[spki] = {
                "count": 1,
                "isp": defaultdict(int),
                "issuer": defaultdict(int),
                "subject": defaultdict(int),
                "usage_type": defaultdict(int),
                "chain_error": defaultdict(int),
                "server_name": defaultdict(int),
                "badkeys-pwnedkeys": defaultdict(int),
                "key_type": defaultdict(int),
                "key_size": defaultdict(int),
            }

            spki_correlation[spki]["isp"][isp] += 1
            spki_correlation[spki]["issuer"][issuer] += 1
            spki_correlation[spki]["subject"][subject] += 1
            spki_correlation[spki]["usage_type"][usage_type] += 1
            spki_correlation[spki]["chain_error"][chain_error] += 1
            spki_correlation[spki]["server_name"][server_name] += 1
            spki_correlation[spki]["key_size"][key_size] += 1
            spki_correlation[spki]["key_type"][key_type] += 1
            spki_correlation[spki]["badkeys-pwnedkeys"][badkeys_blocklist_subtest] += 1

    # Sort each subdictionary within `spki_correlation` based on integer values
    for spki in spki_correlation:
        spki_correlation[spki]["isp"] = OrderedDict(
            sorted(
                spki_correlation[spki]["isp"].items(),
                key=lambda item: item[1],
                reverse=True,
            )
        )
        spki_correlation[spki]["issuer"] = OrderedDict(
            sorted(
                spki_correlation[spki]["issuer"].items(),
                key=lambda item: item[1],
                reverse=True,
            )
        )
        spki_correlation[spki]["subject"] = OrderedDict(
            sorted(
                spki_correlation[spki]["subject"].items(),
                key=lambda item: item[1],
                reverse=True,
            )
        )
        spki_correlation[spki]["usage_type"] = OrderedDict(
            sorted(
                spki_correlation[spki]["usage_type"].items(),
                key=lambda item: item[1],
                reverse=True,
            )
        )
        spki_correlation[spki]["chain_error"] = OrderedDict(
            sorted(
                spki_correlation[spki]["chain_error"].items(),
                key=lambda item: item[1],
                reverse=True,
            )
        )
        spki_correlation[spki]["server_name"] = OrderedDict(
            sorted(
                spki_correlation[spki]["server_name"].items(),
                key=lambda item: item[1],
                reverse=True,
            )
        )
        spki_correlation[spki]["key_type"] = OrderedDict(
            sorted(
                spki_correlation[spki]["key_type"].items(),
                key=lambda item: item[1],
                reverse=True,
            )
        )
        spki_correlation[spki]["key_size"] = OrderedDict(
            sorted(
                spki_correlation[spki]["key_size"].items(),
                key=lambda item: item[1],
                reverse=True,
            )
        )
        spki_correlation[spki]["badkeys-pwnedkeys"] = OrderedDict(
            sorted(
                spki_correlation[spki]["badkeys-pwnedkeys"].items(),
                key=lambda item: item[1],
                reverse=True,
            )
        )

    print("#### ISP count")
    print(
        json.dumps(
            dict(sorted(isp_count.items(), key=lambda item: item[1], reverse=True)),
            indent=4,
        )
    )
    print("")
    print("#### Usage Type count")
    print(
        json.dumps(
            dict(
                sorted(usage_type_count.items(), key=lambda item: item[1], reverse=True)
            ),
            indent=4,
        )
    )

    print("")
    print("#### Issuer count")
    print(
        json.dumps(
            dict(sorted(issuer_count.items(), key=lambda item: item[1], reverse=True)),
            indent=4,
        )
    )

    print("")
    print("#### Subject count")
    print(
        json.dumps(
            dict(sorted(subject_count.items(), key=lambda item: item[1], reverse=True)),
            indent=4,
        )
    )

    print("")
    print("#### Chain error count")
    print(
        json.dumps(
            dict(
                sorted(
                    chain_error_count.items(), key=lambda item: item[1], reverse=True
                )
            ),
            indent=4,
        )
    )

    print("")
    print("#### Server name count")
    print(
        json.dumps(
            dict(
                sorted(
                    server_name_count.items(), key=lambda item: item[1], reverse=True
                )
            ),
            indent=4,
        )
    )

    print("")
    print("#### Badkeys/pwnedkeys blocklist subtests")
    print(
        json.dumps(
            dict(
                sorted(
                    badkeys_blocklist_subtest_count.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )
            ),
            indent=4,
        )
    )

    print("")
    print("#### SPKI count")
    print(
        json.dumps(
            dict(sorted(spki_count.items(), key=lambda item: item[1], reverse=True)),
            indent=4,
        )
    )

    print("")
    print("#### Key type count")
    print(
        json.dumps(
            dict(
                sorted(key_type_count.items(), key=lambda item: item[1], reverse=True)
            ),
            indent=4,
        )
    )

    print("")
    print("#### Key size count")
    print(
        json.dumps(
            dict(
                sorted(key_size_count.items(), key=lambda item: item[1], reverse=True)
            ),
            indent=4,
        )
    )

    print("")
    print("#### SPKI correlation")
    print(json.dumps(spki_correlation, indent=4))
