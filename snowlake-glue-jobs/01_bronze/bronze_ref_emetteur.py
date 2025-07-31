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
    eb.idemetteur
    , eb.idagence
    , eb.personnemorale
    , eb.idpays
    , eb.datecreation
    , eb.statut
    , eb.mode
    , eb.modereception
    , eb.formatplanvectoriel
    , eb.idcontactprestataire
    , eb.responsableprojetflag
    , eb.executanttravauxflag
    , eb.idtypeemetteur
    , eb.idtypecontact
    , eb.statutworkflow
    , eb.capaciteimpression
    , eb.couleur
    , eb.usageunique
    , eb.metadata
    , eb.srn
    , er.nom
    , er.prenom
    , er.societe
    , er.agence
    , er.numrue
    , er.adresse
    , er.codepostal
    , er.commune
    , er.bp
    , er.cedex
    , er.fax
    , er.mail
    , er.siret
    , er.civilite
    , er.mobile
    , er.telephone
    , er.complement
FROM {env}_snowlake_bronze.ref_emetteur_base eb
LEFT JOIN {env}_snowlake_bronze.ref_emetteur_regional er ON er.idemetteur = eb.idemetteur
""")

# LOAD
write_s3(
    spark=spark,
    df=df,
    env=env,
    db='bronze',
    table='ref_emetteur',
    method='overwrite',
    path_prefix='referentiel/'
)

job.commit()
