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

# scope_datas = (
# SELECT d.iddeclaration,
#     d.idclient,
#     d.idagence,
#     d.nomagence,
#     d.idemetteur,
#     d.typemodele,
#     d.referencetravaux,
#     d.debuttravaux,
#     d.dureetravaux,
#     d.adressetravaux,
#     d.codepostaltravaux,
#     d.communetravaux,
#     d.codeinseetravaux,
#     d.descriptiontravaux,
#     d.denominationemetteur,
#     d.denominationcreateur,
#     st_y(st_astext(st_centroid(data.geom))::geometry) AS x,
#     st_x(st_astext(st_centroid(data.geom))::geometry) AS y,
#         CASE
#             WHEN d.typemodele = 'DICT'::declaration.enum_type_modele THEN concat_ws('-'::text, data.data #>> '{infosDT,responsableProjet,societe}'::text[], concat_ws(' '::text, data.data #>> '{infosDT,responsableProjet,nom}'::text[], data.data #>> '{infosDT,responsableProjet,prenom}'::text[]))
#             WHEN d.typemodele = ANY (ARRAY['DT'::declaration.enum_type_modele, 'CONJOINTE'::declaration.enum_type_modele]) THEN concat_ws('-'::text, data.data #>> '{responsableProjet,societe}'::text[], concat_ws(' '::text, data.data #>> '{responsableProjet,nom}'::text[], data.data #>> '{responsableProjet,prenom}'::text[]))
#             ELSE NULL::text
#         END AS responsableprojet,
#     con.numconsultation,
#     d.numconsultationdt,
#     doc.iddocument,
#     doc.typedocument,
#     doc.datecreation,
#     doc.nomcomptesignataire,
#     doc.modeenvoi AS modeenvoidocument,
#         CASE
#             WHEN doc.sensibilite = 'TRANSPORT_MATIERE_DANGEREUSE'::declaration.enum_sensibilite THEN 'tmd'::text
#             WHEN doc.sensibilite = 'SENSIBLE'::declaration.enum_sensibilite THEN 's'::text
#             ELSE 'ns'::text
#         END AS sensibilite,
#         CASE
#             WHEN doc.typedestinataire = 'DP'::declaration.enum_type_destinataire THEN doc.idcontactref::text
#             ELSE doc.idcontactar
#         END AS idcontact,
#         CASE
#             WHEN doc.typedestinataire = 'DP'::declaration.enum_type_destinataire THEN 'ref'::text
#             ELSE 'ar'::text
#         END AS originecontact,
#     doc.nomdestinataire,
#     rep.idreponse,
#     rep.reponsedate,
#     rep.modeenvoi AS modeenvoireponse,
#     rep.reponseexploitant,
#     rel.idrelance,
#     rel.relancedatereception,
#     rel.modeenvoi AS modeenvoirelance,
#     COALESCE(data.data #> '{natureDesTravaux}'::text[], '[]'::json)::text AS naturestravaux,
#     COALESCE(data.data #> '{techniquesPrevues}'::text[], '[]'::json)::text AS techniquestravaux
#     FROM declaration.declaration d
#         JOIN declaration.declaration_data data USING (iddeclaration)
#         JOIN declaration.consultation con USING (iddeclaration)
#         JOIN declaration.document doc USING (idconsultation)
#         LEFT JOIN declaration.relance rel USING (iddocument)
#         LEFT JOIN declaration.reponse rep USING (iddocument)
#     WHERE doc.reponsedate > (now()::date - '1 day'::interval)
# UNION
#     SELECT d.iddeclaration,
#     d.idclient,
#     d.idagence,
#     d.nomagence,
#     d.idemetteur,
#     d.typemodele,
#     d.referencetravaux,
#     d.debuttravaux,
#     d.dureetravaux,
#     d.adressetravaux,
#     d.codepostaltravaux,
#     d.communetravaux,
#     d.codeinseetravaux,
#     d.descriptiontravaux,
#     d.denominationemetteur,
#     d.denominationcreateur,
#     st_y(st_astext(st_centroid(data.geom))::geometry) AS x,
#     st_x(st_astext(st_centroid(data.geom))::geometry) AS y,
#         CASE
#             WHEN d.typemodele = 'DICT'::declaration.enum_type_modele THEN concat_ws('-'::text, data.data #>> '{infosDT,responsableProjet,societe}'::text[], concat_ws(' '::text, data.data #>> '{infosDT,responsableProjet,nom}'::text[], data.data #>> '{infosDT,responsableProjet,prenom}'::text[]))
#             WHEN d.typemodele = ANY (ARRAY['DT'::declaration.enum_type_modele, 'CONJOINTE'::declaration.enum_type_modele]) THEN concat_ws('-'::text, data.data #>> '{responsableProjet,societe}'::text[], concat_ws(' '::text, data.data #>> '{responsableProjet,nom}'::text[], data.data #>> '{responsableProjet,prenom}'::text[]))
#             ELSE NULL::text
#         END AS responsableprojet,
#     con.numconsultation,
#     d.numconsultationdt,
#     doc.iddocument,
#     doc.typedocument,
#     doc.datecreation,
#     doc.nomcomptesignataire,
#     doc.modeenvoi AS modeenvoidocument,
#         CASE
#             WHEN doc.sensibilite = 'TRANSPORT_MATIERE_DANGEREUSE'::declaration.enum_sensibilite THEN 'tmd'::text
#             WHEN doc.sensibilite = 'SENSIBLE'::declaration.enum_sensibilite THEN 's'::text
#             ELSE 'ns'::text
#         END AS sensibilite,
#         CASE
#             WHEN doc.typedestinataire = 'DP'::declaration.enum_type_destinataire THEN doc.idcontactref::text
#             ELSE doc.idcontactar
#         END AS idcontact,
#         CASE
#             WHEN doc.typedestinataire = 'DP'::declaration.enum_type_destinataire THEN 'ref'::text
#             ELSE 'ar'::text
#         END AS originecontact,
#     doc.nomdestinataire,
#     rep.idreponse,
#     rep.reponsedate,
#     rep.modeenvoi AS modeenvoireponse,
#     rep.reponseexploitant,
#     rel.idrelance,
#     rel.relancedatereception,
#     rel.modeenvoi AS modeenvoirelance,
#     COALESCE(data.data #> '{natureDesTravaux}'::text[], '[]'::json)::text AS naturestravaux,
#     COALESCE(data.data #> '{techniquesPrevues}'::text[], '[]'::json)::text AS techniquestravaux
#     FROM declaration.declaration d
#         JOIN declaration.declaration_data data USING (iddeclaration)
#         JOIN declaration.consultation con USING (iddeclaration)
#         JOIN declaration.document doc USING (idconsultation)
#         LEFT JOIN declaration.relance rel USING (iddocument)
#         LEFT JOIN declaration.reponse rep USING (iddocument)
#     WHERE doc.datecreation > (now()::date - '1 day'::interval)
# UNION
#     SELECT d.iddeclaration,
#     d.idclient,
#     d.idagence,
#     d.nomagence,
#     d.idemetteur,
#     d.typemodele,
#     d.referencetravaux,
#     d.debuttravaux,
#     d.dureetravaux,
#     d.adressetravaux,
#     d.codepostaltravaux,
#     d.communetravaux,
#     d.codeinseetravaux,
#     d.descriptiontravaux,
#     d.denominationemetteur,
#     d.denominationcreateur,
#     st_y(st_astext(st_centroid(data.geom))::geometry) AS x,
#     st_x(st_astext(st_centroid(data.geom))::geometry) AS y,
#         CASE
#             WHEN d.typemodele = 'DICT'::declaration.enum_type_modele THEN concat_ws('-'::text, data.data #>> '{infosDT,responsableProjet,societe}'::text[], concat_ws(' '::text, data.data #>> '{infosDT,responsableProjet,nom}'::text[], data.data #>> '{infosDT,responsableProjet,prenom}'::text[]))
#             WHEN d.typemodele = ANY (ARRAY['DT'::declaration.enum_type_modele, 'CONJOINTE'::declaration.enum_type_modele]) THEN concat_ws('-'::text, data.data #>> '{responsableProjet,societe}'::text[], concat_ws(' '::text, data.data #>> '{responsableProjet,nom}'::text[], data.data #>> '{responsableProjet,prenom}'::text[]))
#             ELSE NULL::text
#         END AS responsableprojet,
#     con.numconsultation,
#     d.numconsultationdt,
#     doc.iddocument,
#     doc.typedocument,
#     doc.datecreation,
#     doc.nomcomptesignataire,
#     doc.modeenvoi AS modeenvoidocument,
#         CASE
#             WHEN doc.sensibilite = 'TRANSPORT_MATIERE_DANGEREUSE'::declaration.enum_sensibilite THEN 'tmd'::text
#             WHEN doc.sensibilite = 'SENSIBLE'::declaration.enum_sensibilite THEN 's'::text
#             ELSE 'ns'::text
#         END AS sensibilite,
#         CASE
#             WHEN doc.typedestinataire = 'DP'::declaration.enum_type_destinataire THEN doc.idcontactref::text
#             ELSE doc.idcontactar
#         END AS idcontact,
#         CASE
#             WHEN doc.typedestinataire = 'DP'::declaration.enum_type_destinataire THEN 'ref'::text
#             ELSE 'ar'::text
#         END AS originecontact,
#     doc.nomdestinataire,
#     rep.idreponse,
#     rep.reponsedate,
#     rep.modeenvoi AS modeenvoireponse,
#     rep.reponseexploitant,
#     rel.idrelance,
#     rel.relancedatereception,
#     rel.modeenvoi AS modeenvoirelance,
#     COALESCE(data.data #> '{natureDesTravaux}'::text[], '[]'::json)::text AS naturestravaux,
#     COALESCE(data.data #> '{techniquesPrevues}'::text[], '[]'::json)::text AS techniquestravaux
#     FROM declaration.declaration d
#         JOIN declaration.declaration_data data USING (iddeclaration)
#         JOIN declaration.consultation con USING (iddeclaration)
#         JOIN declaration.document doc USING (idconsultation)
#         LEFT JOIN declaration.relance rel USING (iddocument)
#         LEFT JOIN declaration.reponse rep USING (iddocument)
#     WHERE doc.relancedatereception > (now()::date - '1 day'::interval)
# )

# SELECT scope_datas.iddocument,
#     scope_datas.idagence AS idagence_declaration,
#     scope_datas.modeenvoidocument AS mode_envoi_declaration,
#     scope_datas.typemodele::text AS affichagetypemodele_declaration,
#     scope_datas.numconsultation::text AS numconsultation_declaration,
#     scope_datas.denominationemetteur AS nom_emetteur_declaration,
#         CASE
#             WHEN scope_datas.nomcomptesignataire <> ''::text THEN scope_datas.nomcomptesignataire
#             ELSE scope_datas.denominationcreateur::text
#         END AS nom_compte_emetteur_declaration,
#         CASE
#             WHEN scope_datas.responsableprojet <> ''::text THEN scope_datas.responsableprojet
#             ELSE scope_datas.denominationemetteur
#         END AS moa_declaration,
#     scope_datas.nomagence AS agence_emetteur_declaration,
#         CASE
#             WHEN scope_datas.idcontact ~ '^[0-9]+$'::text THEN scope_datas.idcontact
#             ELSE NULL::text
#         END AS idcontactdestinataire,
#     scope_datas.originecontact AS origine_contact,
#     scope_datas.nomdestinataire AS nom_destinataire_declaration,
#     scope_datas.iddeclaration AS idchantier,
#     scope_datas.referencetravaux AS refchantier,
#     scope_datas.datecreation AS date_creation_chantier,
#     scope_datas.debuttravaux AS date_chantier,
#     scope_datas.dureetravaux AS dureechantier,
#     scope_datas.adressetravaux AS adresse_chantier,
#     scope_datas.codepostaltravaux::text AS cp_chantier,
#     scope_datas.communetravaux AS commune_chantier,
#     scope_datas.codeinseetravaux::text AS insee_chantier,
#     scope_datas.descriptiontravaux AS descriptionchantier,
#     scope_datas.x::numeric(10,6) AS coord_chantier_x,
#     scope_datas.y::numeric(10,6) AS coord_chantier_y,
#     scope_datas.sensibilite AS sns,
#     'DECLARATION'::text AS codeappsrc_declaration,
#     scope_datas.idreponse,
#     scope_datas.reponsedate AS date_envoi_reponse,
#     scope_datas.modeenvoireponse AS mode_envoi_reponse,
#     scope_datas.idcontact AS id_emetteur_reponse,
#     scope_datas.nomdestinataire AS nom_emetteur_reponse,
#     scope_datas.reponseexploitant::text AS valeur_reponse,
#     scope_datas.idrelance AS id_lettre_de_rappel,
#     scope_datas.relancedatereception AS date_reception_lr,
#     scope_datas.modeenvoirelance AS mode_envoi_lr,
#     scope_datas.numconsultationdt::text AS numconsultationdt_declaration,
#     scope_datas.naturestravaux,
#     scope_datas.techniquestravaux,
#     scope_datas.typedocument
#    FROM scope_datas
# WHERE scope_datas.datecreation > (now()::date - '1 day'::interval) OR scope_datas.reponsedate > (now()::date - '1 day'::interval) OR scope_datas.relancedatereception > (now()::date - '1 day'::interval);

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
