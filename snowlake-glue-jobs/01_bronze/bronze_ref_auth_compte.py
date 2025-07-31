# -*- coding: utf-8 -*-
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from etl_tools import write_s3


# CONTEXT
args = getResolvedOptions(sys.argv, ["JOB_NAME", "environment"])
sc = SparkContext()
gc = GlueContext(sc)
job = Job(gc)
spark = gc.spark_session
job.init(args["JOB_NAME"], args)
env = args["environment"]

# EXTRACT
df = spark.sql(f"""
SELECT
    cb.idcompte
    , cb.datecreation
    , cb.dateexpiration
    , cb.password
    , cb.statut
    , cb.derniereconnexion
    , cb.delaideconnexion
    , cb.nombreconnexion
    , cb.flagapplicatif
    , cb.datemailactivation
    , cb.idfonction
    , cb.numeroagent
    , cb.idcommercial
    , cb.type
    , cb.username
    , cb.passwordhistory
    , cb.changepassword
    , cb.changeusername
    , cb.datemodifusername
    , cb.datemodifpassword
    , cb.cleapi
    , cb.consultationonly
    , cb.typeblocage
    , cb.tentativesconnexion
    , cb.bloquejusqua
    , cb.langue
    , cb.clientadmin
    , cb.production
    , cb.api
    , cb.dmc
    , cb.usedmobileapp
    , cb.inviteamiante
    , cb.invitemobile
    , cb.avatarurlps
    , cb.metadata
    , cb.srn
    , cr.nom
    , cr.prenom
    , cr.mail
    , cr.civilite
    , cr.telephone
    , cr.mobile
    , cr.trigramme
FROM {env}_snowlake_bronze.auth_compte_base cb
LEFT JOIN {env}_snowlake_bronze.auth_compte_regional cr ON cr.idcompte = cb.idcompte
""")

# LOAD
write_s3(
    spark=spark,
    df=df,
    env=env,
    db='bronze',
    table='auth_compte',
    method='overwrite',
    path_prefix='referentiel/'
)

job.commit()
