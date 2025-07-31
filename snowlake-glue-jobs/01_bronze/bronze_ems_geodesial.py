# -*- coding: utf-8 -*-
import sys
import requests
from datetime import datetime
from time import sleep
import base64
import pandas as pd
import io
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from etl_tools import write_s3, get_attr_val, add_meta
from pyspark.sql.functions import col, lit

# CONTEXT

args = getResolvedOptions(sys.argv, ["JOB_NAME", "environment"])
sc = SparkContext()
gc = GlueContext(sc)
job = Job(gc)
spark = gc.spark_session
job.init(args["JOB_NAME"], args)
env = args["environment"]
db = 'bronze'
desti = 'ems_geodesial_'

username = 'SOGELINK_DEV_SNOW'
password = 'Ta7#jP^Ud'
base_url = 'https://geodesial.prod.sentinelcloud.com'

encodage = 'utf-8'
auth_str = f'{username}:{password}'.encode(encodage)
credentials = base64.b64encode(auth_str)
headers = {
    "Authorization": f'Basic {credentials.decode(encodage)}',
    "content-type": "application/json",
}

banned_reports = [
    "User Usage",  # Trop long (fail après 24h pour 1 jour de data)
    "Raw Usage",  # Trop long (fail après 24h pour 1 jour de data)
    "Custom Entities",  # Erreur 400 : Bad Request
    "Granular Usage Transactions"  # Erreur 400 : Bad Request
]
wait_for_exports = False


def job_satisfies_criteria(job, report, user, start_date, end_date):
    return job['templateName'] == report \
        and job['createdBy'] == user \
        and any(item['name'] == 'StartDate' and item['value'] == start_date for item in job['inputParameters']['inputParameter']) \
        and any(item['name'] == 'EndDate' and item['value'] == end_date for item in job['inputParameters']['inputParameter'])


# EXTRACT

# Get all data reports
response = requests.get(
    url=f"{base_url}/ems/api/v5/reportTemplates",
    headers=headers
)
reports = response.json()['reportTemplates']['reportTemplate']

for report in reports:
    print(f"\n[*] {report['displayName']} (nom technique {report['name']})")

    if report['displayName'] in banned_reports:
        print("Rapport banni (trop long à exporter, erreur 400, ...), passage au rapport suivant")
        continue

    # In order to consult data of the report, ask to generate a time-bounded export via a job
    start_date = "2024-01-01"
    end_date = "2024-02-01"
    # TODO Retrieve date range from Athena et mettre en append
    queryParams = {
        "StartDate": start_date,
        "EndDate": end_date,
    }

    export_job = None
    while not export_job:
        # Search existing export jobs for the data report created by sogelink_dev_snow and on the specified date range
        print(f"Recherche d'un export du rapport {report['name']} créé par {username} et avec pour paramètres {queryParams}...")
        response = requests.get(
            url=f"{base_url}/ems/api/v5/reportJobs",
            headers=headers
        )
        jobs = response.json()['reportJobs']['reportJob']
        jobs = [job for job in jobs if job_satisfies_criteria(job, report['name'], username.lower(), start_date, end_date)]
        jobs = sorted(jobs, key=lambda job: datetime.strptime(job['creationDate'], "%Y-%m-%d %H:%M"), reverse=True)

        # Take the most recent COMPLETED export job or the most recent export job or ask for generate one
        jobs_completed = [job for job in jobs if job['state'] == 'COMPLETED']
        if len(jobs_completed) > 0:
            export_job = jobs_completed[0]
            print(f"{len(jobs)} trouvé(s) dont {len(jobs_completed)} {export_job['state']}, le {export_job['state']} le plus récent est sélectionné")
        elif len(jobs) > 0:
            export_job = jobs[0]
            print(f"{len(jobs)} trouvé(s), le plus récent est sélectionné ({export_job['state']})")
        else:
            print("Aucun job d'export trouvé, demande d'export...")

            # Ask for an export of the data report
            response = requests.post(
                url=f"{base_url}/ems/api/v5/reportTemplates/{report['id']}/generateReport",
                params=queryParams,
                headers=headers
            )
            if not response.ok:
                print(f"Erreur {response.status_code} : {response.reason}")
                break

    if not export_job:
        print("Problème lors de la récupération ou génération d'un export, passage au rapport suivant")
        continue

    print("Au moins un export trouvé pour ce rapport de données, le plus récent :")
    print(f"  Id : {export_job['id']}")
    print(f"  Name : {export_job['name']}")
    print(f"  CreationDate : {export_job['creationDate']}")
    print(f"  TemplateName : {export_job['templateName']}")
    print(f"  TemplayDisplayName : {export_job['templateDisplayName']}")
    print(f"  InputParameters : {export_job['inputParameters']['inputParameter']}")
    print(f"  CreatedBy : {export_job['createdBy']}")
    print(f"  StartDate : {export_job['startDate']}")
    print(f"  State : {export_job['state']}")
    print(f"  CompleteDate : {export_job['completedDate']}")

    # If needed, wait for the end of the export job and the availability of the CSV
    while True:
        response = requests.get(
            url=f"{base_url}/ems/api/v5/reportJobs/{export_job['id']}?getDownloadUrl=true",
            headers=headers
        )
        export_job = response.json()['reportJob']

        if export_job['state'] == 'COMPLETED':
            break
        else:
            print("Export en cours de création, attente de la fin de l'exécution du job...")
            if not wait_for_exports:
                print("Job non terminé mais passage au rapport suivant (wait_for_exports=False)")
                break
            sleep(120)

    if not export_job['downloadUrl']:
        continue

    print(f"Export disponible : {export_job['downloadUrl']}")

    # Extract the export and load it in pandas/spark/s3+athena
    request = requests.get(export_job['downloadUrl'])
    data = request.content.decode('utf8')
    df = pd.read_csv(io.StringIO(data))
    print(df.head())

    if len(df):
        print("Export non vide => Chargement dans S3/Athena...")
        b_data = spark.createDataFrame(df.astype(str))
        m_data = add_meta(b_data, False, _event=lit(export_job['creationDate']))
        m_data = m_data.withColumn("_src", lit(export_job['downloadUrl']))
        # TODO ingest date

        write_s3(
            spark=spark,
            df=m_data,
            env=env,
            db=db,
            table=f"{desti}{export_job['templateName']}",
            method='overwrite',  # TODO append
            partition_cols=[],  # TODO partition cols basé sur la date fonctionnelle (date log, date, licence, ...)
            path_prefix='ems_geodesial/'
        )

    # requests.delete(
    #     url = f"{base_url}/ems/api/v5/reportJobs/{export_job['id']}",
    #     headers = headers
    # )
