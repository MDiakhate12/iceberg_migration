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
    x.todo
FROM {env}_snowlake_bronze.xxx x
LEFT JOIN {env}_snowlake_bronze.xxx x ON x.x = x.xx
""")

# SELECT now()::date - '1 day'::interval AS date_debut,
# now() AS date_fin
# )

# SELECT d.iddeclaration,
# d.idagence AS idsociete,
# d.datereception AS date_reception_declaration,
# upper(COALESCE(d.metadatas -> 'modeReception'::text, d.metadatas -> 'modeEnvoi'::text)) AS mode_envoi_declaration,
#     CASE
#         WHEN d.codetypemodele = 'AUTRE_SPECIFIQUE'::reponse.enum_type_modele THEN COALESCE(d.metadatas -> 'sousTypeAutre'::text, d.codetypemodele::text)
#         ELSE d.codetypemodele::text
#     END AS affichagetypemodele_declaration,
# d.numconsultation AS numconsultation_declaration,
# ltrim(rtrim((COALESCE(d.nomemetteur, ''::text) || ' '::text) || COALESCE(d.prenomemetteur, ''::text))) AS nom_prenom_emetteur_declaration,
# NULL::text AS moa_declaration,
# d.societeemetteur AS societe_emetteur_declaration,
# dd.jsondata #>> '{emetteur,adresse,commune}'::text[] AS commune_emetteur_declaration,
# d.idchantier,
# d.refdeclarant AS refchantier,
# d.chantierdate AS date_chantier,
#     CASE
#         WHEN d.codetypemodele = 'ATU'::reponse.enum_type_modele THEN d.chantierduree / 2
#         ELSE d.chantierduree
#     END::numeric(10,1) AS dureechantier,
# d.adresse AS adresse_chantier,
# d.codepostal AS cp_chantier,
# d.commune AS commune_chantier,
# d.inseecommune AS insee_chantier,
# dd.jsondata #>> '{chantier,description}'::text[] AS descriptionchantier,
# st_x(st_astext(st_centroid(dd.geom))::geometry)::numeric AS coord_chantier_x,
# st_y(st_astext(st_centroid(dd.geom))::geometry)::numeric AS coord_chantier_y,
# d.statutworkflow::text AS statut_doc,
# d.metadatas -> 'origineDeclaration'::text AS codeappsrc_declaration,
# r.idreponse,
# r.coderecepisse AS coderecepisse_reponse,
# r.referencedossier AS refclient_reponse,
# r.dateenvoi AS date_envoi_reponse,
# r.datereception AS date_reception_reponse,
# r.metadatas -> 'modeEnvoi'::text AS mode_envoi_reponse,
# r.nomsignataire AS compte_reponse,
# concat_ws(' - '::text, r.metadatas -> 'societeEmetteur'::text,
#     CASE
#         WHEN btrim(concat_ws(' '::text, r.metadatas -> 'nomEmetteur'::text, r.metadatas -> 'prenomEmetteur'::text)) = ''::text THEN NULL::text
#         ELSE btrim(concat_ws(' '::text, r.metadatas -> 'nomEmetteur'::text, r.metadatas -> 'prenomEmetteur'::text))
#     END) AS nom_prenom_emetteur_reponse,
# r.valeurreponse::text AS valeur_reponse,
# d.datereceptionlr AS date_reception_lr,
# r.categoriesreseaux AS categories_reseaux,
# d.nomdestinataire,
# d.iddestinataire AS idcontactdestinataire,
# d.idemetteur AS idemetteur_declaration,
# btrim((COALESCE(dd.jsondata #>> '{emetteur,adresse,num}'::text[], ''::text) || ' '::text) || COALESCE(dd.jsondata #>> '{emetteur,adresse,rue}'::text[], ''::text)) AS adresse_emetteur_declaration,
# COALESCE(dd.jsondata #>> '{emetteur,adresse,cp}'::text[], ''::text) AS codepostal_emetteur_declaration,
# COALESCE(dd.jsondata #>> '{emetteur,tel}'::text[], ''::text) AS telephone_emetteur_declaration,
#     CASE d.codetypemodele
#         WHEN 'DT'::reponse.enum_type_modele THEN COALESCE(dd.jsondata #> '{data,DT,projetCalendrier,codesNatureTvx}'::text[], '[]'::json)
#         WHEN 'DICT'::reponse.enum_type_modele THEN COALESCE(dd.jsondata #> '{data,DICT,tvxCalendrier,codesNatureTvx}'::text[], '[]'::json)
#         WHEN 'DT_DICT'::reponse.enum_type_modele THEN (COALESCE(dd.jsondata #> '{data,DT,projetCalendrier,codesNatureTvx}'::text[], '[]'::json)::jsonb || COALESCE(dd.jsondata #> '{data,DICT,tvxCalendrier,codesNatureTvx}'::text[], '[]'::json)::jsonb)::json
#         ELSE NULL::json
#     END AS naturestravaux,
#     CASE d.codetypemodele
#         WHEN 'DICT'::reponse.enum_type_modele THEN COALESCE(dd.jsondata #> '{data,DICT,tvxCalendrier,codesTechs}'::text[], '[]'::json)
#         WHEN 'DT_DICT'::reponse.enum_type_modele THEN COALESCE(dd.jsondata #> '{data,DICT,tvxCalendrier,codesTechs}'::text[], '[]'::json)
#         ELSE NULL::json
#     END AS techniquestravaux,
# COALESCE(d.metadatas -> 'hasMl'::text, 'false'::text)::boolean AS hasml,
# COALESCE(d.metadatas -> 'hasIc'::text, 'false'::text)::boolean AS hasic,
# COALESCE(d.metadatas -> 'bannetteInit'::text, ''::text) AS bannetteinit
# FROM reponse.declaration d
#     LEFT JOIN scope ON true
#     JOIN reponse.declaration_data dd ON d.iddeclaration = dd.iddeclaration
#     LEFT JOIN ( SELECT r_1.iddeclaration,
#         r_1.idreponse,
#         r_1.referencedossier,
#         r_1.datecreation,
#         r_1.datereception,
#             CASE
#                 WHEN e.typeenvoi = 'ENVOI_AUTO'::reponse.enum_type_envoi THEN 'SOGELINK'::text
#                 ELSE r_1.nomsignataire
#             END AS nomsignataire,
#         r_1.metadatas,
#         r_1.valeurreponse,
#         r_1.categoriesreseaux,
#         tm.coderecepisse,
#         e.datecreation AS dateenvoi
#         FROM reponse.reponse r_1
#             JOIN reponse.envoi e ON e.idreponse = r_1.idreponse AND (e.typeenvoi = ANY (ARRAY['ENVOI'::reponse.enum_type_envoi, 'ENVOI_AUTO'::reponse.enum_type_envoi]))
#             LEFT JOIN reponse.type_modele tm ON r_1.codetypemodele = tm.code
#         WHERE NOT r_1.isbrouillon) r ON r.iddeclaration = d.iddeclaration
# WHERE d.datecreation >= scope.date_debut AND d.datecreation <= scope.date_fin OR d.datereception >= scope.date_debut AND d.datereception <= scope.date_fin OR d.datereceptionlr >= scope.date_debut AND d.datereceptionlr <= scope.date_fin OR r.datecreation >= scope.date_debut AND r.datecreation <= scope.date_fin OR r.datereception >= scope.date_debut AND r.datereception <= scope.date_fin OR r.dateenvoi >= scope.date_debut AND r.dateenvoi <= scope.date_fin;

# LOAD
write_s3(
    spark=spark,
    df=df,
    env=env,
    db='bronze',
    table='reponse_vue_stats',
    method='overwrite'
)

job.commit()
