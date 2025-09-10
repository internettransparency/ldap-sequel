"""This script checks if the corresponding private key of a certificate's public key is known to pwnedkeys.com."""

import base64
import csv
import hashlib
import json
import logging
import os
import re
import time
import traceback
from datetime import datetime
from typing import Any

import jc
import requests
from asn1crypto import pem, x509
from progress.bar import Bar
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

STATUS_OK = 200
STATUS_NOT_FOUND = 404
STATUS_TOO_MANY_REQUESTS = 429

PWNEDKEY_API_SECRET_ENV = "PWNEDKEY_API_SECRET-"
MAX_B64DECODE_ITERATIONS = 10

WAIT_TIME = 0.0

CSV_FILE = "../dataset/processing/2024-nov-unique_peer_certs.csv"
PWNEDKEYS_RESULT_FILE = "../dataset/processing/key_analysis/pwnedkeys_results.json"
PWNEDKEYS_STATS_FILE = "../dataset/processing/key_analysis/pwnedkeys_stats.json"


def parse_certificate(cert_data: str) -> tuple[str, dict[str, Any]]:
    """
    Parses a certificate in either pem or der format to a dict, using the jc library.
    """
    try:  # noqa SIM105
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

            hash_object = hashlib.sha1(cert.encode("utf-8"))
            hash_hex = hash_object.hexdigest()

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


def retry_after(api_url: str, headers: dict[str, Any], seconds: int) -> dict[str, Any]:
    """Makes manual retry if api returns 429."""
    time.sleep(seconds)

    try:
        response = requests.get(api_url, headers=headers, timeout=5)
    except Exception as e:
        logging.error(traceback.format_exc())
        return {"error": {"code": "exception", "text": repr(e)}}

    if response.status_code == STATUS_OK:
        return {"pwned": True}
    elif response.status_code == STATUS_NOT_FOUND:
        return {"pwned": False}
    elif response.status_code == STATUS_TOO_MANY_REQUESTS:
        retry_header = response.headers.get("Retry-After")

        return {
            "error": {
                "code": response.status_code,
                "text": response.text,
                "retry_after": retry_header,
            }
        }
    else:
        return {"error": {"code": response.status_code, "text": response.text}}


def send_request(spki_hash: str, pwnendkeys_secret: str) -> dict[str, Any]:
    """Sends request to pwnedkeys.com with given hash."""
    api_url = f"https://v1.pwnedkeys.com/{spki_hash}"

    s = requests.Session()
    retries = Retry(
        total=10,
        backoff_factor=2,  # 2s, 4s, 8s, 16s, ...
        status_forcelist=[429, 500, 502, 503, 504, 524],
        allowed_methods=frozenset(["GET", "POST"]),
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))

    if pwnendkeys_secret:
        headers = {"Authorization": f"Bearer {pwnendkeys_secret}"}
    else:
        headers = {}

    try:
        response = s.get(api_url, headers=headers)
    except Exception as e:
        logging.error(traceback.format_exc())
        return {"error": {"code": "exception", "text": repr(e)}}

    if response.status_code == STATUS_OK:
        return {"pwned": True}
    elif response.status_code == STATUS_NOT_FOUND:
        return {"pwned": False}
    elif response.status_code == STATUS_TOO_MANY_REQUESTS:
        retry_header: str = response.headers.get("Retry-After", "0")
        return retry_after(api_url, headers, int(retry_header))
    else:
        return {"error": {"code": response.status_code, "text": response.text}}


def check_base64(input_str: str) -> bool:
    """Checks input for base64 string."""
    base64_pattern = re.compile(r"^[A-Za-z0-9+/]+={0,2}$")

    return base64_pattern.match(input_str) is not None


def compute_spki_hash(pem_format: str):
    """Computes spki hash"""
    try:
        # Convert PEM -> DER
        type_name, headers, der_bytes = pem.unarmor(pem_format.encode("ascii"))
        cert = x509.Certificate.load(der_bytes)

        # SPKI (DER bytes)
        spki_der = cert["tbs_certificate"]["subject_public_key_info"].dump()

        spki_hash = hashlib.sha256(spki_der).hexdigest()
        return {"hash": spki_hash}

    except Exception as e:
        return {
            "error": {
                "code": "asn1crypto",
                "text": str(e),
            }
        }


def run(base64_cert: str) -> dict[str, Any]:
    """Perform pwnedkeys analysis on given certificate."""
    pwnendkeys_secret = os.getenv(PWNEDKEY_API_SECRET_ENV)
    if not pwnendkeys_secret:
        logging.warning(f"Environment variable {PWNEDKEY_API_SECRET_ENV} not set.")

    base64_cert = (
        base64_cert.replace("-----BEGIN CERTIFICATE-----", "")
        .replace("-----END CERTIFICATE-----", "")
        .replace("\r", "")
        .replace("\n", "")
    )

    if not check_base64(base64_cert):
        return {
            "error": {
                "code": "base64",
                "text": "certificate data is invalid base64 string",
            }
        }

    pem_format = (
        f"-----BEGIN CERTIFICATE-----\n{base64_cert}\n-----END CERTIFICATE-----"
    )

    spki_hash = compute_spki_hash(pem_format)

    return send_request(spki_hash, pwnendkeys_secret)


if __name__ == "__main__":

    certs_dict_list = extract_certs_from_csv(CSV_FILE)

    with Bar("Processing...", max=len(certs_dict_list)) as bar:
        for cert_dict in certs_dict_list:
            result_dict = run(cert_dict.get("peer_certificate"))
            cert_dict.update(result_dict)
            time.sleep(WAIT_TIME)
            bar.next()

    with open(PWNEDKEYS_RESULT_FILE, "w") as fp:
        fp.write(json.dumps(certs_dict_list, indent=4))

    stats_dict = {"pwned": 0, "not_pwned": 0}

    for cert in certs_dict_list:
        pwned = cert.get("pwned", None)
        if pwned is None:
            error = cert.get("error")
            code = error.get("code")
            if stats_dict.get(f"error_{code}") is None:
                stats_dict[f"error_{code}"] = 1
            else:
                stats_dict[f"error_{code}"] += 1
                continue

        if pwned:
            stats_dict["pwned"] += 1
        else:
            stats_dict["not_pwned"] += 1

    with open(PWNEDKEYS_STATS_FILE, "w") as fp:
        fp.write(json.dumps(stats_dict, indent=4))
