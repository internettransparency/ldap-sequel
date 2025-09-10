# LDAP Sequel - Key Analysis

This subproject contains scripts for the key analysis of certificates conducted in the paper. For the analysis, the [badkeys](https://github.com/badkeys/badkeys) tool and the [pwnedkeys](https://pwnedkeys.com/) platform were used.

## Installation

To run the scripts, the dependencies need to be installed first. A Python virtual environment is recommended:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

After installing the dependencies, the scripts [badkeys_analysis.py](badkeys_analysis.py) and [pwnedkeys_analysis.py](pwnedkeys_analysis.py) can be executed. The results will be written to the `dataset/processing/key_analysis` folder:

```bash
python badkeys_analysis.py
python pwnedkeys_analysis.py
```

The script [key_analysis.py](key_analysis.py) works on the previously generated results and builds the statistics used for the evaluation in the paper.

```bash
python key_analysis.py
```