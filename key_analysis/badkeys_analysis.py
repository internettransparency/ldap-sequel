"""This script checks certificates for vulnerable keys with the badkeys tool."""

import base64
import csv
import hashlib
import json
from datetime import datetime
from typing import Any

import badkeys
import jc
from progress.bar import Bar

MAX_B64DECODE_ITERATIONS = 10

CSV_FILE = "../dataset/processing/2024-nov-unique_peer_certs.csv"
BADKEYS_RESULT_FILE = "../dataset/processing/key_analysis/badkeys_results.json"
BADKEYS_STATS_FILE = "../dataset/processing/key_analysis/badkeys_stats.json"


def parse_certificate(cert_data: str) -> tuple[str, dict[str, Any]]:
    """
    Parses a certificate in either pem or der format to a dict, using the jc library.
    """
    try:
        cert_data = (
            cert_data.replace("\n", "")
            .replace("\r", "")
            .replace("-----BEGIN CERTIFICATE-----", "")
            .replace("-----END CERTIFICATE-----", "")
            .replace(" ", "")
            .replace("[", "")
            .replace("]", "")
        )
    except Exception:
        pass

    iterations = 0
    b64_cert_candidate = cert_data
    byte_cert_candidate = cert_data
    cert_data_history = []
    while iterations < MAX_B64DECODE_ITERATIONS:
        try:
            cert_data_history.append(b64_cert_candidate)
            byte_cert_candidate = base64.b64decode(b64_cert_candidate.encode())
            byte_cert_candidate = (
                byte_cert_candidate.decode()
                .replace("\n", "")
                .replace("\r", "")
                .replace("-----BEGIN CERTIFICATE-----", "")
                .replace("-----END CERTIFICATE-----", "")
                .replace(" ", "")
                .replace("[", "")
                .replace("]", "")
            )
            b64_cert_candidate = byte_cert_candidate
            cert_candidate = byte_cert_candidate
        except Exception:
            cert_candidate = byte_cert_candidate
            break
        iterations += 1

    try:
        parse_result = jc.parse("x509_cert", cert_candidate)
    except Exception:
        return None, None

    # parse returns a list with one entry
    if isinstance(parse_result, list):
        fields = parse_result[0]
    elif isinstance(parse_result, dict):
        fields = parse_result
    else:
        return None, None

    cert_data = cert_data_history[-1]
    if isinstance(cert_data, bytes):
        try:
            cert_data = (
                cert_candidate.decode()
                .replace("\n", "")
                .replace("\r", "")
                .replace("-----BEGIN CERTIFICATE-----", "")
                .replace("-----END CERTIFICATE-----", "")
                .replace(" ", "")
                .replace("[", "")
                .replace("]", "")
            )
        except Exception:
            cert_data = base64.b64encode(cert_candidate).decode()
    return cert_data, fields


def extract_certs_from_csv(csv_file: str):

    certs_dict_list = []

    with open(csv_file, "r") as csv_file:
        csv_reader = csv.DictReader(
            csv_file,
            delimiter=",",
            fieldnames=[
                "ipv4",
                "port",
                "date",
                "server_name",
                "isp",
                "usage_type",
                "issuer",
                "subject",
                "chain_error",
                "peer_certificate",
            ],
            skipinitialspace=True,
        )

        next(csv_reader)

        line_count = 0
        for row in csv_reader:
            ip = row["ipv4"]
            port = row["port"]
            timestamp = datetime.strptime(
                row["date"], "%Y-%m-%dT%H:%M:%S.%fZ"
            ).strftime("%Y-%m-%d")
            server_name = row["server_name"]
            isp = row["isp"]
            usage_type = row["usage_type"]
            issuer = row["issuer"]
            subject = row["subject"]
            chain_error = row["chain_error"]
            cert = row["peer_certificate"]

            parsed_cert = parse_certificate(cert)
            try:
                subject = parsed_cert[1]["tbs_certificate"]["subject"]
            except Exception:
                pass

            try:
                issuer = parsed_cert[1]["tbs_certificate"]["issuer"]
            except Exception:
                pass

            try:
                valid_not_before = parsed_cert[1]["tbs_certificate"]["validity"][
                    "not_before_iso"
                ]
            except Exception:
                pass

            try:
                valid_not_after = parsed_cert[1]["tbs_certificate"]["validity"][
                    "not_after_iso"
                ]
            except Exception:
                pass

            try:
                serial_number = parsed_cert[1]["tbs_certificate"]["serial_number"]
            except Exception:
                serial_number = None

            hash_object = hashlib.sha1(cert.encode("utf-8"))
            hash_hex = hash_object.hexdigest()

            certs_dict_list.append(
                {
                    "ipv4": ip,
                    "port": port,
                    "timestamp": timestamp,
                    "server_name": server_name,
                    "isp": isp,
                    "usage_type": usage_type,
                    "issuer": issuer,
                    "subject": subject,
                    "valid_not_before": valid_not_before,
                    "valid_not_after": valid_not_after,
                    "chain_error": chain_error,
                    "peer_certificate": cert,
                    "serial_number": serial_number,
                    "hash": hash_hex,
                }
            )

            line_count += 1

    print(f"Processed {line_count} lines.")
    return certs_dict_list


def run(base64_cert: str) -> dict[str, Any]:
    """Perform badkeys analysis on given certificate."""

    base64_cert = (
        base64_cert.replace("-----BEGIN CERTIFICATE-----", "")
        .replace("-----END CERTIFICATE-----", "")
        .replace("\n", "")
    )

    pem_format = f"-----BEGIN CERTIFICATE-----{base64_cert}-----END CERTIFICATE-----"

    result_data = badkeys.detectandcheck(pem_format)
    r_type = result_data.get("type")
    r_bits = result_data.get("bits")
    r_dict = result_data.get("results")
    r_spki = result_data.get("spkisha256")

    return {
        "type": r_type,
        "bits": r_bits,
        "spki": r_spki,
        "results": r_dict,
    }


if __name__ == "__main__":
    certs_dict_list = extract_certs_from_csv(CSV_FILE)

    with Bar("Processing...", max=len(certs_dict_list)) as bar:
        for cert_dict in certs_dict_list:
            result_dict = run(cert_dict.get("peer_certificate"))
            cert_dict.update(result_dict)
            bar.next()

    with open(BADKEYS_RESULT_FILE, "w") as fp:
        fp.write(json.dumps(certs_dict_list, indent=4))

    stats_list = []

    for cert in certs_dict_list:
        bad = cert.get("results", None)
        if bad is None:
            continue
        if len(bad) == 0:
            continue
        else:
            stats_list.append(bad)

    with open(BADKEYS_STATS_FILE, "w") as fp:
        fp.write(json.dumps(stats_list, indent=4))
