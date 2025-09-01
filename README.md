# Characterizing Hosting and Security Practices for Public-Facing LDAP Servers

Welcome to the software artifacts of the aforementioned paper. This repository provides resources needed to examine and reproduce our work.  

## Description
Research project to investigate the LDAP hosting ecosystem and their security.  

This repository consists of the following modules:  
- [cert-validator](https://github.com/gustavoluvizotto/cert-validator): Tool to validate chains of X.509 certificates. The certificates are extracted from LDAP servers using Goscanner.  
- [goscanner-fork](https://github.com/gustavoluvizotto/goscanner-fork): The Goscanner from [TUM](https://github.com/tumi8/goscanner) with extensions that enable LDAP scanning. The scanner is capable of extracting Root DSE attributes and values in both plaintext and raw base64 format that can be further used for server identification.  
- [ldap-identification/recog](https://github.com/gustavoluvizotto/recog): The Rapid7's [recog](https://github.com/rapid7/recog) official repository with extensions that enable identification of more Windows Server instances. The folder ``ldap-identification`` contains the necessary software to intake the measurements from ``goscanner-fork`` and process into a human-readable format.
- [internet-wide-scans](https://gitlab.utwente.nl/m7711402/internet-wide-scans/): ZMap measurement tool with a few extras. Extras: running with containers, handling errors such as email notification with details of the error, use a curated blocklist, scan with only routeable and up-to-date IPv4 addresses (allowlist), and automatically upload the output data (and its artifacts) to a remote S3 storage.  

Important files to start the Jupyter notebook for analysis:  
- ``docker-compose.yml``: Docker configuration for the jupyter notebook used in the analysis. Pay attention that it uses environment variables that is (under ``${VAR}``) which is stored in the ``.env`` file.  
- ``.env``: Environment configuration. It contains the following IMPORTANT variables:  
    - ``PORT``: I used 8889, but the default is 8888 for Jupyter;  
    - ``DATASET``: CHANGE. Set the location where you should unpack the dataset. See the dataset DOI below;  
    - ``PROJECT_NAME``: just a name, keep as is;  
    - ``PROJECT_DIR``: CHANGE. Set the location of where you placed this repository in your filesystem;  
    - ``CONTAINER_USER``: CHANGE. Set with your username (of the machine you are running this code);  
    - ``U_ID`` and ``G_ID``: CHANGE. Set with your user id and group id (``id -u`` and ``id -g`` in Unix machines) of the machine you are running this code.  
- ``start_notebook.sh``: simple script to start the Jupyter notebook container. Look at the ``session`` file that it generates to retrieve the url of the Jupyter notebook;  

Once you get the Jupyter notebook up and running, these are the most important notebooks:  
- ``analysis.ipynb``: It contains the main paper analysis. Here's your entrypoint.  
- ``spark-instance-gustavo.ipynb``: the Spark configuration I used. Here I set only 1 executor (only in a single machine), with 40 executor cores, 300Gb of RAM for executors, 10 driver cores and 100Gb of RAM for the driver. You can fine tune this for your hardware, but be mindful that if the limitation is too much (e.g., user's 2014 laptop comparable HW), then you possible won't be able to run this notebook. Please let me know if you do.  
- ``analysis-org-size.ipynb``: Code that provides the Number of IPs of an Autonomous System (AS) and IP prefixes for a given scanned LDAP IP address. This script is necessary for the main analysis one. You also need to run IP2Location over all your LDAP IPs to be able to run this notebook.  

The other notebooks under ``notebooks/`` directory that are not listed are not relevant. Check them on your own will.  

## HW and OS used and dependencies
We used Debian 11 on a cluster with 128 cores and 1.48Tb of RAM, and 36Tb of storage (ZFS). However, lower configurations specs can be used (see above ``spark-instance-gustavo.ipynb`` configuration for Spark). The analysis took place via ``ssh`` tunnel to connect to a powerful machine and using VS Code from the laptop.  

The machine should have ``podman`` and ``podman-compose`` (the latter installed via ``python`` -- ``pip``). The ``podman-compose`` version is ``0.1.11``. Stick with this version for better compatibility with the provided tools.

## Usage
First configure the ``.env`` file (as detailed in the Description above) and unpack the dataset into your filesystem. Some of the analysis depends on external dataset such as IP2Location and Censys. So, for those you must obtain the dataset yourself.  

After configuring the ``.env``, start the jupyter notebook:  
```
./start_notebook.sh
```

Then read the ``session`` file that is created to find the Jupyter notebook url. The folder ``work`` contains the jupyter notebooks, where the analysis can be fetched and started. Then, follow the ``analysis.ipynb`` notebook.  

## Dataset
DOI: 10.5281/zenodo.17019740  

## Contact
For further information, please contact [gustavoluvizotto](https://github.com/gustavoluvizotto).  

## Acknowledgment
Censys, IP2Location.  

## License
MIT.  
