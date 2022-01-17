# Gustavo Almeida Jobsity Data Engineer

This project was developed using Google Cloud and benefits from its self-managed services like Cloud Storage, BigQuery, Cloud Composer and DataProc.

With Cloud Composer self-managed Airflow infrastructure, I am using it as the main pipeline for building a temp bucket for processing along with creating and provisioning and DataProc cluster and deleting it after the pipeline is succeeded, so we do not have an idle cluster. All the notification are managed through the TelegramOperator, where the most important steps are notified through a private group.

This project is fully scalable with all the most critical parameters, like Cloud Composer and DataProc clusters, can be updated with a few lines of codes, and within a few minutes everything can be scaled.

The ELT pipeline was developed using PySpark running on DataProc, so it leverages from the cluster parameters, along side Cloud Storage wich is serveless.

Additional scripts for creating buckets, environments and roles are using Google Python libraries, running on a set up environment with Google Cloud SDK or services accounts, and bash scriptis running gcloud commands

Power BI is used as a data visualization platform, the leading platform according to Gartner, with its very intuitive UI it is easy to users to start building their first dashboard.


