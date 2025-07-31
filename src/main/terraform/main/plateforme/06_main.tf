# ===========================================================
# Buckets S3
# ===========================================================

/* Snowlake */

resource "aws_s3_bucket" "snowlake" {
  bucket = "sglk-snowlake-${terraform.workspace}-${data.aws_region.current.name}"

  tags = {
    Environment = terraform.workspace
    Application = "snowlake"
    exportOnly  = "snowlake"
  }

  lifecycle {
    ignore_changes = [lifecycle_rule, server_side_encryption_configuration]
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "snowlake_encryption" {
  bucket = aws_s3_bucket.snowlake.bucket

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = data.aws_kms_key.snowlake_kms_key.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_object" "snowlake_lakeformation_prefix" {
  bucket = aws_s3_bucket.snowlake.id
  key    = "lakeformation-results/"
}

resource "aws_s3_bucket_acl" "snowlake_acl" {
  bucket = aws_s3_bucket.snowlake.id
  acl    = "private"
}

resource "aws_s3_bucket_lifecycle_configuration" "snowlake_lifecycle" {
  bucket = aws_s3_bucket.snowlake.id

  rule {
    id = "athena-results"

    expiration {
      days = 7
    }

    filter {
      prefix = "athena-results/"
    }

    status = "Enabled"
  }

  rule {

    id = "jman-results"
    expiration {
      days = 30
    }

    filter {
      prefix = "jman-results/"
    }
    status = "Enabled"
  }

  rule {

    id = "lakeformation-results"
    expiration {
      days = 7
    }

    filter {
      prefix = "lakeformation-results/"
    }
    status = "Enabled"
  }
}

/* Ringover */

resource "aws_s3_bucket" "ringover" {
  bucket = "sglk-ringover-${terraform.workspace}-${data.aws_region.current.name}"

  tags = {
    Environment = terraform.workspace
    Application = "ringover"
    exportOnly  = "ringover"
  }

  lifecycle {
    ignore_changes = [server_side_encryption_configuration]
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "ringover_encryption" {
  bucket = aws_s3_bucket.ringover.bucket

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = data.aws_kms_key.snowlake_kms_key.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_acl" "ringover_acl" {
  bucket     = aws_s3_bucket.ringover.id
  acl        = "private"
  depends_on = [aws_s3_bucket_ownership_controls.ringover_acl_ownership]
}

resource "aws_s3_bucket_ownership_controls" "ringover_acl_ownership" {
  bucket = aws_s3_bucket.ringover.id
  rule {
    object_ownership = "ObjectWriter"
  }
}

/* Go Connect */

resource "aws_s3_bucket" "go_connect" {
  bucket = "sglk-go-connect-${terraform.workspace}-${data.aws_region.current.name}"

  tags = {
    Environment = terraform.workspace
    Application = "go-connect"
    exportOnly  = "go-connect"
  }

  lifecycle {
    ignore_changes = [server_side_encryption_configuration]
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "go_connect_encryption" {
  bucket = aws_s3_bucket.go_connect.bucket

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = data.aws_kms_key.go_connect_kms_key.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_acl" "go_connect_acl" {
  bucket     = aws_s3_bucket.go_connect.id
  acl        = "private"
  depends_on = [aws_s3_bucket_ownership_controls.go_connect_acl_ownership]
}

resource "aws_s3_bucket_ownership_controls" "go_connect_acl_ownership" {
  bucket = aws_s3_bucket.go_connect.id
  rule {
    object_ownership = "ObjectWriter"
  }
}

/* Archives */

resource "aws_s3_bucket" "archives" {
  bucket = "sglk-archives-${terraform.workspace}-${data.aws_region.current.name}"

  tags = {
    Environment = terraform.workspace
    Application = "archives"
    exportOnly  = "archives"
  }

  lifecycle {
    ignore_changes = [server_side_encryption_configuration]
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "archives_encryption" {
  bucket = aws_s3_bucket.archives.bucket

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.archives_kms_key.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_acl" "archives_acl" {
  bucket     = aws_s3_bucket.archives.id
  acl        = "private"
  depends_on = [aws_s3_bucket_ownership_controls.archives_acl_ownership]
}

resource "aws_s3_bucket_ownership_controls" "archives_acl_ownership" {
  bucket = aws_s3_bucket.archives.id
  rule {
    object_ownership = "ObjectWriter"
  }
}

# ===========================================================
# Step Functions
# ===========================================================

/* Supervisor */
resource "aws_sfn_state_machine" "supervisor" {
  name     = "${terraform.workspace}-snowlake-supervisor"
  role_arn = data.aws_iam_role.snowlake_sfn_supervisor.arn

  definition = templatefile("${path.module}/templates/step_functions/sfn_supervisor.nocheck.json", {
    sfn_export_arn                     = aws_sfn_state_machine.export_snapshot_rds.arn
    sfn_run_workflow_arn               = aws_sfn_state_machine.run_workflow.arn
    workflow_ingest_external_name      = aws_glue_workflow.snowlake_workflow_ingest_external.name
    workflow_ingest_jira_name          = aws_glue_workflow.snowlake_workflow_ingest_jira.name
    workflow_ingest_lucca_name         = aws_glue_workflow.snowlake_workflow_ingest_lucca.name
    workflow_ingest_matomo_name        = aws_glue_workflow.snowlake_workflow_ingest_matomo.name
    workflow_ingest_netsuite_name      = aws_glue_workflow.snowlake_workflow_ingest_netsuite.name
    workflow_ingest_superoffice_name   = aws_glue_workflow.snowlake_workflow_ingest_superoffice.name
    workflow_ingest_tribe_name         = aws_glue_workflow.snowlake_workflow_ingest_tribe.name
    workflow_ingest_visma_name         = aws_glue_workflow.snowlake_workflow_ingest_visma.name
    workflow_ingest_webkua_name        = aws_glue_workflow.snowlake_workflow_ingest_webkua.name
    workflow_ingest_afas_name          = aws_glue_workflow.snowlake_workflow_ingest_afas.name
    workflow_bronze_crawlers_name      = aws_glue_workflow.snowlake_workflow_bronze_crawlers.name
    workflow_referentiel_scodify_name  = aws_glue_workflow.snowlake_workflow_referentiel_scodify.name
    workflow_silver_gold_jobs_name     = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name
    workflow_tech_jobs_name            = aws_glue_workflow.snowlake_workflow_tech_jobs.name
    workflow_tech_deactivate_jobs_name = aws_glue_workflow.snowlake_workflow_tech_deactivate_jobs.name
    env                                = terraform.workspace
    max_concurrency                    = local.max_concurrency_exports[terraform.workspace]
    sources = [
      {
        "application" : "api",
        "exportOnly" : [
          "api.api"
        ]
      },
      {
        "application" : "commande",
        "exportOnly" : [
        ]
      },
      {
        "application" : "declaration",
        "exportOnly" : [
          "declaration.declaration.abonnement_mail",
          "declaration.declaration.categorie",
          "declaration.declaration.consultation",
          "declaration.declaration.choix_destinataire",
          "declaration.declaration.declaration",
          "declaration.declaration.declaration_data",
          "declaration.declaration.demande_precisions",
          "declaration.declaration.document",
          "declaration.declaration.parametre_agence",
          "declaration.declaration.parametre_client",
          "declaration.declaration.parametre_modele",
          "declaration.declaration.piece_jointe",
          "declaration.declaration.piece_jointe_reponse",
          "declaration.declaration.relance",
          "declaration.declaration.reponse"
        ]
      },
      {
        "application" : "demat",
        "exportOnly" : [
          "demat.demat",
          "demat.demat_precalcul"
        ]
      },
      {
        "application" : "pmsr",
        "exportOnly" : [
          "pmsr.pmsr.commercial",
          "pmsr.pmsr.pmsr",
          "pmsr.pmsr.pmsr_carto",
          "pmsr.pmsr.pmsr_history"
        ]
      },
      {
        "application" : "scream",
        "exportOnly" : [
          "scream.scream.activite_to_societe",
          "scream.scream.adresse",
          "scream.scream.banque",
          "scream.scream.categorie_formation",
          "scream.scream.chapitre",
          "scream.scream.client_regle_factu",
          "scream.scream.client_to_produit",
          "scream.scream.competence_devis_to_offre",
          "scream.scream.competence_devis",
          "scream.scream.competence_to_societe",
          "scream.scream.competence",
          "scream.scream.complement_devis",
          "scream.scream.concurrent",
          "scream.scream.condition_paiement",
          "scream.scream.conso_conso",
          "scream.scream.conso_data",
          "scream.scream.conso_erreur",
          "scream.scream.conso_plan",
          "scream.scream.contact_to_entite",
          "scream.scream.contact",
          "scream.scream.entite_to_societe",
          "scream.scream.entite",
          "scream.scream.etat_synchro",
          "scream.scream.facturation_terme_echu",
          "scream.scream.famille_produit",
          "scream.scream.feature",
          "scream.scream.feature_to_licence",
          "scream.scream.feature_to_produit_model",
          "scream.scream.fonction",
          "scream.scream.licence_client",
          "scream.scream.mapping_netsuite",
          "scream.scream.offre",
          "scream.scream.origine",
          "scream.scream.payeur_echeance",
          "scream.scream.payeurclient_to_conso",
          "scream.scream.produitclient_to_conso",
          "scream.scream.produit_renouvellement",
          "scream.scream.produit_to_offre",
          "scream.scream.produit_to_produit",
          "scream.scream.produit",
          "scream.scream.regle_calcul_envoi",
          "scream.scream.regle_facturation",
          "scream.scream.secteur",
          "scream.scream.societe_regle_factu",
          "scream.scream.societe_to_concurrent",
          "scream.scream.societe",
          "scream.scream.sous_chapitre",
          "scream.scream.ste_admin_entite",
          "scream.scream.template_facture",
          "scream.scream.unite_conso",
          "scream.screamcle.licence_cle",
          "scream.sogetask.activite_tache",
          "scream.sogetask.affaire",
          "scream.sogetask.bon_commande_achat",
          "scream.sogetask.bon_commande",
          "scream.sogetask.campagne",
          "scream.sogetask.categorie_tache",
          "scream.sogetask.commentaire_tache_data",
          "scream.sogetask.commentaire_tache_evenement",
          "scream.sogetask.commentaire_tache",
          "scream.sogetask.date_cloture_compta",
          "scream.sogetask.detail_file_tache",
          "scream.sogetask.devis",
          "scream.sogetask.devise",
          "scream.sogetask.echeance",
          "scream.sogetask.encaissement",
          "scream.sogetask.facture",
          "scream.sogetask.file_tache",
          "scream.sogetask.historique_tache",
          "scream.sogetask.mention_legale",
          "scream.sogetask.motif_affaire",
          "scream.sogetask.produit_devis",
          "scream.sogetask.produit_facture",
          "scream.sogetask.recouvrement",
          "scream.sogetask.select_values",
          "scream.sogetask.tache_to_compte",
          "scream.sogetask.tache_to_contact",
          "scream.sogetask.tache_to_detail",
          "scream.sogetask.tache_to_devis",
          "scream.sogetask.tache_to_tache",
          "scream.sogetask.tache",
          "scream.sogetask.tacheencaisse_to_facture",
          "scream.sogetask.tva_code",
          "scream.sogetask.tva_taux"
        ]
      },
      {
        "application" : "statistic",
        "exportOnly" : [
          "statistic.dtm.coc_conso_client",
          "statistic.dtm.dm_envoi",
          "statistic.dtm.sca_stats_ca2",
          "statistic.dtm.sta_stats_activite",
          "statistic.dtm.stc_suivi_client",
          "statistic.dtm.std_stat_devis",
          "statistic.dtm.stk_stats_stock",
          "statistic.dtm.stv_stats_ventes",
          "statistic.dtm.sup_suivi_previsionnel",
          "statistic.dtm.suv_suivi_ventes",
          "statistic.dwh.aff_affaire",
          "statistic.dwh.age_agence",
          "statistic.dwh.bdc_ratio_commercial",
          "statistic.dwh.cal_jour_ferie",
          "statistic.dwh.cct_contact",
          "statistic.dwh.cdp_condition_paiement",
          "statistic.dwh.cha_chantier",
          "statistic.dwh.chp_chapitre",
          "statistic.dwh.chp_sous_chapitre",
          "statistic.dwh.cli_client",
          "statistic.dwh.cmd_commande",
          "statistic.dwh.cnn_compte_notification",
          "statistic.dwh.com_commune",
          "statistic.dwh.cpg_campagne",
          "statistic.dwh.cpl_conso_plan",
          "statistic.dwh.cpt_compte",
          "statistic.dwh.cpu_ca2_pu_data",
          "statistic.dwh.cta_categorie_tache",
          "statistic.dwh.ctc_compte_to_client",
          "statistic.dwh.cte_contact_to_entite",
          "statistic.dwh.ctp_client_to_produit",
          "statistic.dwh.declaration_document",
          "statistic.dwh.declaration_exploitant",
          "statistic.dwh.declaration_synthese",
          "statistic.dwh.dev_devis",
          "statistic.dwh.ech_echeance",
          "statistic.dwh.enc_encaissement",
          "statistic.dwh.ent_entite",
          "statistic.dwh.ets_entite_to_societe",
          "statistic.dwh.fac_facture",
          "statistic.dwh.for_formation",
          "statistic.dwh.for_stagiaire",
          "statistic.dwh.fta_file_tache",
          "statistic.dwh.fte_facturation_terme_echu",
          "statistic.dwh.grc_groupe_client",
          "statistic.dwh.lic_licence_client",
          "statistic.dwh.map_conso_dmenvoi",
          "statistic.dwh.off_offre",
          "statistic.dwh.ori_origine",
          "statistic.dwh.pae_payeur_echeance",
          "statistic.dwh.pay_pays",
          "statistic.dwh.pcc_produitclient_to_conso",
          "statistic.dwh.prd_produit",
          "statistic.dwh.prf_produit_facture",
          "statistic.dwh.produitclient_to_conso",
          "statistic.dwh.prs_produit_devis",
          "statistic.dwh.ptp_produit_to_produit",
          "statistic.dwh.rce_regle_calcul_envoi",
          "statistic.dwh.rec_recouvrement",
          "statistic.dwh.rfa_client_regle_factu",
          "statistic.dwh.rfa_regle_facturation",
          "statistic.dwh.rfa_societe_regle_factu",
          "statistic.dwh.ringover_call",
          "statistic.dwh.sai_saisie_dmc_configuration",
          "statistic.dwh.sai_saisie_paiement",
          "statistic.dwh.sca_client",
          "statistic.dwh.sca_client_to_produit",
          "statistic.dwh.sca_facture",
          "statistic.dwh.sca_produit",
          "statistic.dwh.sca_produit_facture",
          "statistic.dwh.sca_tache",
          "statistic.dwh.sec_secteur",
          "statistic.dwh.soc_societe",
          "statistic.dwh.svd_suivi_declarant",
          "statistic.dwh.sve_suivi_exploitant",
          "statistic.dwh.svs_suivi_scodify",
          "statistic.dwh.tch_tache",
          "statistic.dwh.tpa_type_agence",
          "statistic.dwh.ttc_tache_to_contact",
          "statistic.dwh.ttd_tache_to_devis",
          "statistic.dwh.ttp_tache_to_compte",
          "statistic.dwh.tva_code",
          "statistic.dwh.tva_taux",
          "statistic.dwh.tyc_type_client"
        ]
      },
      {
        "application" : "referencement",
        "exportOnly" : [
          "referencement.ar"
        ]
      },
      {
        "application" : "referentiel",
        "exportOnly" : [
          "referentiel.auth.app",
          "referentiel.auth.app_to_client",
          "referentiel.auth.compte_base",
          "referentiel.auth.compte_hierarchie",
          "referentiel.auth.compte_notification",
          "referentiel.auth.compte_regional",
          "referentiel.auth.compte_to_app",
          "referentiel.auth.compte_to_client",
          "referentiel.auth.compte_to_profil",
          "referentiel.auth.droit_app",
          "referentiel.auth.droit_app_to_profil",
          "referentiel.auth.historique_compte",
          "referentiel.auth.profil",
          "referentiel.log.auth_log_connexion",
          "referentiel.ref.agence",
          "referentiel.ref.client",
          "referentiel.ref.commune",
          "referentiel.ref.compte_to_agence",
          "referentiel.ref.compte_to_emetteur",
          "referentiel.ref.emetteur_base",
          "referentiel.ref.emetteur_regional",
          "referentiel.ref.entite_to_type_agence",
          "referentiel.ref.entite",
          "referentiel.ref.groupe_agences",
          "referentiel.ref.groupe_client",
          "referentiel.ref.historique_agence",
          "referentiel.ref.historique_client",
          "referentiel.ref.historique_contact",
          "referentiel.ref.pays",
          "referentiel.ref.prestataire",
          "referentiel.ref.rue",
          "referentiel.ref.secteur",
          "referentiel.ref.societe",
          "referentiel.ref.statut_envoi",
          "referentiel.ref.type_agence",
          "referentiel.ref.type_client",
          "referentiel.ref.type_emetteur"
        ]
      },
      {
        "application" : "reponse",
        "exportOnly" : [
          "reponse.reponse.declaration",
          "reponse.reponse.declaration_data",
          "reponse.reponse.envoi",
          "reponse.reponse.parametre_agence",
          "reponse.reponse.parametre_client",
          "reponse.reponse.pjdossier",
          "reponse.reponse.pjdossier_to_declaration",
          "reponse.reponse.pjdossier_to_mission",
          "reponse.reponse.pjdossier_to_reponse",
          "reponse.reponse.pjdossier_to_template",
          "reponse.reponse.reponse",
          "reponse.reponse.reponse_data",
          "reponse.reponse.suivi_reponse",
          "reponse.reponse.template",
          "reponse.reponse.type_modele"
        ]
      },
      {
        "application" : "litteralis",
        "exportOnly" : [
          "litteralis.da_dpa.consultation_reponse",
          "litteralis.da_dpa.demande",
          "litteralis.da_dpa.demande_specifique",
          "litteralis.da_dpa.demande_type",
          "litteralis.da_dpa.emprise",
          "litteralis.da_dpa.envoi",
          "litteralis.da_dpa.notification_preference",
          "litteralis.da_dpa.piece_jonte",
          "litteralis.da_dpa.piece_jointe_demande_type",
          "litteralis.litteralis.admin_data",
          "litteralis.litteralis.annexe",
          "litteralis.litteralis.arrete",
          "litteralis.litteralis.arrete_data",
          "litteralis.litteralis.coll_ext_to_agence",
          "litteralis.litteralis.deduction_role",
          "litteralis.litteralis.delegation",
          "litteralis.litteralis.demande_recue",
          "litteralis.litteralis.demande_recue_data",
          "litteralis.litteralis.emprise",
          "litteralis.litteralis.envoi",
          "litteralis.litteralis.mapping_chantier",
          "litteralis.litteralis.notification_preference",
          "litteralis.litteralis.pj_arrete",
          "litteralis.litteralis.publication",
          "litteralis.litteralis.reponse",
          "litteralis.litteralis.visas"
        ]
      },
      {
        "application" : "formulaire",
        "exportOnly" : [
          "formulaire.formulaire"
        ]
      },
      {
        "application" : "scodify",
        "exportOnly" : [
        ]
      },
    ]
  })

  # logging_configuration {
  #   log_destination        = "${aws_cloudwatch_log_group.log_group_for_sfn.arn}:*" #TODO?
  #   include_execution_data = true
  #   level                  = "ALL"
  # }

  depends_on = [
    aws_sfn_state_machine.run_workflow,
    aws_sfn_state_machine.export_snapshot_rds,
    aws_glue_workflow.snowlake_workflow_ingest_external,
    aws_glue_workflow.snowlake_workflow_ingest_jira,
    aws_glue_workflow.snowlake_workflow_ingest_lucca,
    aws_glue_workflow.snowlake_workflow_ingest_matomo,
    aws_glue_workflow.snowlake_workflow_ingest_netsuite,
    aws_glue_workflow.snowlake_workflow_ingest_superoffice,
    aws_glue_workflow.snowlake_workflow_ingest_tribe,
    aws_glue_workflow.snowlake_workflow_ingest_visma,
    aws_glue_workflow.snowlake_workflow_ingest_webkua,
    aws_glue_workflow.snowlake_workflow_ingest_afas,
    aws_glue_workflow.snowlake_workflow_bronze_crawlers,
    aws_glue_workflow.snowlake_workflow_referentiel_scodify,
    aws_glue_workflow.snowlake_workflow_silver_gold_jobs,
    aws_glue_workflow.snowlake_workflow_tech_jobs
  ]
}

/* Run Workflow */
resource "aws_sfn_state_machine" "run_workflow" {
  name     = "${terraform.workspace}-snowlake-run-workflow"
  role_arn = data.aws_iam_role.snowlake_sfn_supervisor.arn

  definition = templatefile("${path.module}/templates/step_functions/sfn_run_workflow.json", {
  })
}

/* Export Snapshots RDS */
resource "aws_sfn_state_machine" "export_snapshot_rds" {
  name     = "${terraform.workspace}-snowlake-export-snapshot-rds"
  role_arn = data.aws_iam_role.snowlake_sfn_export_rds.arn

  definition = templatefile("${path.module}/templates/step_functions/sfn_export_snapshot_rds.nocheck.json", {
    kms_key_id     = data.aws_kms_key.snowlake_kms_key.key_id
    iam_role_arn   = data.aws_iam_role.snowlake_sfn_export_snapshot_rds.arn
    s3_bucket_name = "sglk-snowlake-${terraform.workspace}-eu-west-1"
    env            = terraform.workspace
  })
}

/* Run Jobs */
resource "aws_sfn_state_machine" "run_jobs" {
  name     = "${terraform.workspace}-snowlake-run-jobs"
  role_arn = data.aws_iam_role.snowlake_sfn_supervisor.arn

  definition = templatefile("${path.module}/templates/step_functions/sfn_run_jobs.json", {
    env = terraform.workspace
  })

}

# ===========================================================
# Event Bridge
# ===========================================================

/* Supervisor : Full run */

resource "aws_cloudwatch_event_target" "sfn_event_target_supervisor_full_run" {
  arn      = aws_sfn_state_machine.supervisor.arn
  rule     = aws_cloudwatch_event_rule.sfn_event_rule_supervisor_full_run.id
  role_arn = data.aws_iam_role.snowlake_eventbridge_step_functions.arn

  input = jsonencode({
    "clean_bronze" : true,
    "clean_bronze_targets" : [
      {
        "s3_folders" : "",
        "table_schemas" : "api,commande,public,declaration,demat,demat_precalcul"
      },
      {
        "s3_folders" : "",
        "table_schemas" : "formulaire,da_dpa,litteralis,pmsr,ar"
      },
      {
        "s3_folders" : "",
        "table_schemas" : "auth,log,ref,reponse,scream,screamcle,sogetask,scodify"
      }
      # ,
      # {
      #   "s3_folders" : "",
      #   "table_schemas" : "dtm,dwh"
      # },
    ],
    "ingest_rds" : true,
    "ingest_rds_applications" : [
      "api",
      "commande",
      "declaration",
      "demat",
      "formulaire",
      "litteralis",
      "pmsr",
      "referencement",
      "referentiel",
      "reponse",
      "scream",
      "scodify",
      # "statistic"
    ],
    "ingest_archives" : false,
    "ingest_external" : true,
    "ingest_jira" : true,
    "ingest_lucca" : true,
    "ingest_matomo" : false,
    "ingest_netsuite" : true,
    "ingest_scodify" : true,
    "ingest_visma" : true,
    "ingest_afas" : true,
    "ingest_dispatch_pbi" : false,
    "run_workflow_silver_gold" : true
  })
}

resource "aws_cloudwatch_event_rule" "sfn_event_rule_supervisor_full_run" {
  name        = "${terraform.workspace}_snowlake_sfn_rule_supervisor_full_run"
  description = "Event Rule to trigger datalake supervisor (full run)"
  role_arn    = data.aws_iam_role.snowlake_eventbridge_step_functions.arn

  state               = try(local.eventbridge_sfn_supervisor[terraform.workspace].full_run.state, "DISABLED")
  schedule_expression = try(local.eventbridge_sfn_supervisor[terraform.workspace].full_run.cron, local.eventbridge_sfn_supervisor.default)

  tags = {
    Name = "Snowlake"
  }
}

/* Supervisor : Critical run */

resource "aws_cloudwatch_event_target" "sfn_event_target_supervisor_critical_run" {
  arn      = aws_sfn_state_machine.supervisor.arn
  rule     = aws_cloudwatch_event_rule.sfn_event_rule_supervisor_critical_run.id
  role_arn = data.aws_iam_role.snowlake_eventbridge_step_functions.arn

  input = jsonencode({
    "clean_bronze" : true,
    "clean_bronze_targets" : [
      {
        "s3_folders" : "",
        "table_schemas" : "auth,log,ref,scream,screamcle,sogetask"
      }
    ],
    "ingest_rds" : true,
    "ingest_rds_applications" : [
      "referentiel",
      "scream"
    ],
    "ingest_external" : true,
    "ingest_jira" : true,
    "ingest_lucca" : true,
    "ingest_netsuite" : true,
    "ingest_visma" : true,
    "ingest_afas" : true,
    "ingest_dispatch_pbi" : false,
    "run_workflow_silver_gold" : true,
  })
}

resource "aws_cloudwatch_event_rule" "sfn_event_rule_supervisor_critical_run" {
  name        = "${terraform.workspace}_snowlake_sfn_rule_supervisor_critical_run"
  description = "Event Rule to trigger datalake supervisor (light run)"
  role_arn    = data.aws_iam_role.snowlake_eventbridge_step_functions.arn

  state               = try(local.eventbridge_sfn_supervisor[terraform.workspace].critical_run.state, "DISABLED")
  schedule_expression = try(local.eventbridge_sfn_supervisor[terraform.workspace].critical_run.cron, local.eventbridge_sfn_supervisor.default)

  tags = {
    Name = "Snowlake"
  }
}

/* Supervisor : Light run */

resource "aws_cloudwatch_event_target" "sfn_event_target_supervisor_light_run" {
  arn      = aws_sfn_state_machine.supervisor.arn
  rule     = aws_cloudwatch_event_rule.sfn_event_rule_supervisor_light_run.id
  role_arn = data.aws_iam_role.snowlake_eventbridge_step_functions.arn

  input = jsonencode({
    "clean_bronze" : true,
    "clean_bronze_targets" : [
      {
        "s3_folders" : "",
        "table_schemas" : "scream,screamcle,sogetask"
      },
      {
        "s3_folders" : "",
        "table_schemas" : "ar"
      }
    ],
    "ingest_rds" : true,
    "ingest_rds_applications" : [
      "scream",
      "referencement"
    ],
    "run_workflow_silver_gold" : true
  })
}

resource "aws_cloudwatch_event_rule" "sfn_event_rule_supervisor_light_run" {
  name        = "${terraform.workspace}_snowlake_sfn_rule_supervisor_light_run"
  description = "Event Rule to trigger datalake supervisor (scream run)"
  role_arn    = data.aws_iam_role.snowlake_eventbridge_step_functions.arn

  state               = try(local.eventbridge_sfn_supervisor[terraform.workspace].light_run.state, "DISABLED")
  schedule_expression = try(local.eventbridge_sfn_supervisor[terraform.workspace].light_run.cron, local.eventbridge_sfn_supervisor.default)

  tags = {
    Name = "Snowlake"
  }
}

resource "aws_iam_policy" "allow_glue_job_control" {
  name        = "${terraform.workspace}_sfn_allow_glue_jobs"
  description = "Allow Step Function to start and manage Glue jobs"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun"
        ],
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_glue_policy_to_sfn_role" {
  role       = data.aws_iam_role.snowlake_sfn_supervisor.name
  policy_arn = aws_iam_policy.allow_glue_job_control.arn
}

/* Supervisor : Planful run */
resource "aws_cloudwatch_event_target" "sfn_event_target_supervisor_hot_run" {
  arn      = aws_sfn_state_machine.run_jobs.arn
  rule     = aws_cloudwatch_event_rule.sfn_event_rule_supervisor_hot_run.id
  role_arn = data.aws_iam_role.snowlake_eventbridge_step_functions.arn
}

resource "aws_cloudwatch_event_rule" "sfn_event_rule_supervisor_hot_run" {
  name        = "${terraform.workspace}_snowlake_sfn_rule_supervisor_hot_run"
  description = "Event Rule to trigger multiple glue jobs"
  role_arn    = data.aws_iam_role.snowlake_eventbridge_step_functions.arn

  state               = try(local.eventbridge_sfn_supervisor[terraform.workspace].hot_run.state, "DISABLED")
  schedule_expression = try(local.eventbridge_sfn_supervisor[terraform.workspace].hot_run.cron, local.eventbridge_sfn_supervisor.default)

  tags = {
    Name = "Snowlake"
  }
}
