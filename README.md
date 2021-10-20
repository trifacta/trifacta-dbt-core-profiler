# What is the Trifacta dbt Core Dataset Profiling Utility?

The Trifacta dbt Core Dataset Profiling Utility is a command line tool that crawls your local dbt Core repository and generates data profiles from the output datasets you’ve created in Google BigQuery.

The utility connects to your local dbtCore repository models and BigQuery, and creates all the necessary connections and dataset metadata in Google Cloud Dataprep by Trifacta. The utility then runs profiling jobs using our SQL pushdown engine and generates data profiles for BigQuery objects that were populated by dbtCore.

# Environment Support

### Data Warehouse
The current version of the profiling utility is optimized for BigQuery data warehouses. This version of the utility takes advantage of Google Dataprep by Trifacta’s BiqQuery auto-generated SQL pushdown capabilities. The team is working on expanding SQL pushdown to support other cloud data warehouses soon.

### dbt Core Repository
The utility uses the dbt Core v0.20.1 to compile models and supports repository versions that are compatible with this version.

### Trifacta
The current version of the utility is best experienced using our Google Cloud product, Cloud Dataprep by Trifacta, against BigQuery data warehouses. Users wishing to use the utility on AWS or against other cloud data warehouses can post a comment on our community discussion group thread here and someone from the team will reach out to you.

# Requirements

1) Trifacta Account - The utility requires an active Google Dataprep by Trifacta Professional Tier account. If you do not already have an account, you can access Cloud Dataprep from the GCP console or [sign up for a 30-day Professional trial](https://cloud.google.com/dataprep).

2) Local dbt Core repository with BigQuery Connectivity - The utility assumes you have a local dbt Core repository that is already connected to your BigQuery data warehouse.

3) Docker Desktop - We use a docker image to encapsulate all the dependencies in order to help folks get up and running much easier. Install a free version of Docker Desktop here.

# Getting Started

## Configuring your Trifacta Connection

1) Create an empty file on your local machine and name it .trifacta.conf. This configuration file tells the utility how to connect to your Google Cloud Dataprep by Trifacta account.

1a - Copy and paste the following configuration into .trifacta.conf:

1b - Update the username with your Google cloud account email address.

1c - Obtain a valid API token from Cloud Dataprep:

  - Log into your Cloud Dataprep account from your browser
  - In the bottom left corner, click on your your user avatar (or initials)
  - Select ‘Preferences’
  - From User Preferences, select Access Tokens
  - Click Generate Token
  - Copy the token and paste it into trifacta.conf

1d - Save .trifacta.conf (in a new folder or in your home directory)

## Running the latest version of trifacta-dbt-profiler from DockerHub

Once you have Docker Desktop installed, you may issue the following commands:

2a) To retrieve the list of available subcommands, issue the following command:
```
docker run -it -v ~:/home --rm trifacta/trifacta-dbt-profiler --help
```
2b) To retrieve the list of required parameters for the profile command, enter:
```
docker run -it -v ~:/home --rm trifacta/trifacta-dbt-profiler profile --help
```
2c) To run the profiler, you can run the profile command.
```
docker run -it -v ~:/home --rm trifacta/trifacta-dbt-profiler profile \
--dbt-profiles-dir .dbt --trifacta-config .trifacta.conf  \
--dbt-project-dir ./my_dbt_project_directory --include-list mymodel
```
### NOTE: You’ll need to update the various parameters listed in 2b with your specific directory and file locations

The profiler utility is generally available under the Apache Software License v2.0. For those interested in accessing the utility’s source code, please visit our [GitHub repository](https://github.com/trifacta/trifacta-dbt-core-profiler). We welcome feedback and contributions.

# Troubleshooting & Support
For help with troubleshooting issues with the utility, please ask a question in our [dbt Core Utility discussion group](https://community.trifacta.com/s/group/0F93j000000gEz8CAE/dbt-core-profiler-utility-users) and someone from our team will respond to you quickly.

# FAQs

### Q: Does the utility automatically issue `dbt run` commands to populate BigQuery?

A: No, the utility assumes that `dbt run` has already been executed and the target tables and views already exist in BigQuery.

### Q: Does the utility import dbt Core models/sql into Trifacta?

A: No, this version of the utility reads and compiles local dbt Core models and sends the target table and view names to Trifacta to profile against BigQuery. In the near future we will be exploring the ability to import and orchestrate dbt jobs directly from Trifacta.

### Q: How does Cloud Dataprep connect to BigQuery?

A: The utility sends Dataprep the BigQuery connection information located in your dbt Core repository to connect to your tables and views.

### Q: Can the utility connect to my repository on dbt Cloud?

 A: No, the utility assumes you have cloned a local copy of your dbt Core repository from your Git provider.

### Q: Which versions of Python and dbt Core does the utility support?

 A: We have packaged up all the required Python and dbt Core dependencies within the Docker file, so that users don’t have to worry about them. For those interested in looking under the hood at the Python source code, libraries, and Trifacta API calls, please visit our [GitHub repository](visit https://github.com/trifacta/trifacta-dbt-core-profiler).

### Q: Will I be charged for running profiles?

 A: The utility makes temporary copies of your datasets, and issues queries against each column in the dataset to generate the profiling reports. Each dataset copy is then deleted after each profile report is created to significantly reduce storage costs. Temporary storage and profile queries are subject to BigQuery's Storage and Analysis pricing.
