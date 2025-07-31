locals {
  name = "snowlake"

  # SFN Supervisor

  eventbridge_sfn_supervisor = {
    default = "cron(59 10 ? * MON 2020)",
    prod = {
      critical_run = {
        state = "ENABLED",
        cron  = "cron(59 9 ? * MON-SAT *)"
      },
      full_run = {
        state = "ENABLED",
        # cron = "cron(59 1,20 ? * MON-SUN *)"
        cron = "cron(59 20 ? * MON-SUN *)"
      },
      hot_run = {
        state = "ENABLED",
        cron  = "cron(0 0 31 2 ? *)"
      }
    },
    staging = {
      full_run = {
        state = "ENABLED",
        cron  = "cron(59 10 ? * 2#2 *)"
      }
    },
    inte = {
      light_run = {
        state = "ENABLED",
        cron  = "cron(59 4 ? * FRI *)"
      },
      hot_run = {
        state = "ENABLED",
        cron  = "cron(0 0 31 2 ? *)"
      }
    }
  }

  max_concurrency_exports = {
    inte    = 1,
    staging = 1,
    prod    = 3
  }

  # Glue Jobs

  profile_config = {
    default = {
      cold = {
        worker_type       = "G.1X",
        number_of_workers = 2
      }
    },
    prod = {
      hot = {
        worker_type       = "G.2X",
        number_of_workers = 5
      },
      medium = {
        worker_type       = "G.2X",
        number_of_workers = 2
      },
      cold = {
        worker_type       = "G.1X",
        number_of_workers = 2
      }
    },
    staging = {
      hot = {
        worker_type       = "G.1X",
        number_of_workers = 2
      },
      medium = {
        worker_type       = "G.1X",
        number_of_workers = 2
      },
      cold = {
        worker_type       = "G.1X",
        number_of_workers = 2
      }
    },
    inte = {
      hot = {
        worker_type       = "G.1X",
        number_of_workers = 2
      },
      medium = {
        worker_type       = "G.1X",
        number_of_workers = 2
      },
      cold = {
        worker_type       = "G.1X",
        number_of_workers = 2
      }
    }
  }

  # Jar Libs
  jar_libs = {
    "sedona-spark-shaded" = {
      filename = "sedona-spark-shaded-3.0_2.12-1.6.1.jar"
      url      = "http://nexus.main.forge/repository/maven-group-all/org/apache/sedona/sedona-spark-shaded-3.0_2.12/1.6.1/sedona-spark-shaded-3.0_2.12-1.6.1.jar",
    },
    "geotools-wrapper" = {
      filename = "geotools-wrapper-1.6.1-28.2.jar"
      url      = "http://nexus.main.forge/repository/maven-group-all/org/datasyslab/geotools-wrapper/1.6.1-28.2/geotools-wrapper-1.6.1-28.2.jar"
    }
  }

  glue-common                = "../../../../../snowlake-glue-common/target/snowlake-common-glue.zip"
  glue-external-python       = "../../../../../snowlake-glue-common/target/whl"
  lambda-export-sync         = "../../../../../snowlake-lambda-export-sync/target/snowlake-lambda-export-sync.zip"
  lambda-instance-identifier = "../../../../../snowlake-lambda-instance-identifier/target/snowlake-lambda-instance-identifier.zip"
  lambda-send-notification   = "../../../../../snowlake-lambda-send-notification"
  lambda-dispatch-pbi        = "../../../../../snowlake-lambda-dispatch-pbi"
  lambda-check-time          = "../../../../../snowlake-lambda-check-time"

  # Jobs Tech

  job-tech-blank          = "../../../../../snowlake-glue-jobs/00_tech/tech_blank.py"
  job-tech-jman-external  = "../../../../../snowlake-glue-jobs/00_tech/tech_jman_external.py"
  job-catalog             = "../../../../../snowlake-glue-jobs/00_tech/catalog.py"
  job-backup-table        = "../../../../../snowlake-glue-jobs/00_tech/backup_table.py"
  job-deactivate-afas     = "../../../../../snowlake-glue-jobs/00_tech/tech_deactivate_afas.py"
  job-deactivate-netsuite = "../../../../../snowlake-glue-jobs/00_tech/tech_deactivate_netsuite.py"
  job-deactivate-visma    = "../../../../../snowlake-glue-jobs/00_tech/tech_deactivate_visma.py"

  # Jobs Bronze

  job-bronze-ems-entitlementusage            = "../../../../../snowlake-glue-jobs/01_bronze/bronze_ems_entitlementusage.py"
  job-bronze-ems-geodesial                   = "../../../../../snowlake-glue-jobs/01_bronze/bronze_ems_geodesial.py"
  job-bronze-flydoc-ondemand                 = "../../../../../snowlake-glue-jobs/01_bronze/bronze_flydoc_ondemand.py"
  job-bronze-jira-issue                      = "../../../../../snowlake-glue-jobs/01_bronze/bronze_jira_issue.py"
  job-bronze-jira-project                    = "../../../../../snowlake-glue-jobs/01_bronze/bronze_jira_project.py"
  job-bronze-jira-worklog                    = "../../../../../snowlake-glue-jobs/01_bronze/bronze_jira_worklog.py"
  job-bronze-jira-worklog-deleted            = "../../../../../snowlake-glue-jobs/01_bronze/bronze_jira_worklog_deleted.py"
  job-bronze-jira-team-member                = "../../../../../snowlake-glue-jobs/01_bronze/bronze_jira_team_member.py"
  job-bronze-lucca-leave                     = "../../../../../snowlake-glue-jobs/01_bronze/bronze_lucca_leave.py"
  job-bronze-matomo-city                     = "../../../../../snowlake-glue-jobs/01_bronze/bronze_matomo_city.py"
  job-bronze-matomo-country                  = "../../../../../snowlake-glue-jobs/01_bronze/bronze_matomo_country.py"
  job-bronze-matomo-forms                    = "../../../../../snowlake-glue-jobs/01_bronze/bronze_matomo_forms.py"
  job-bronze-matomo-forms-pages              = "../../../../../snowlake-glue-jobs/01_bronze/bronze_matomo_forms_pages.py"
  job-bronze-matomo-funnels                  = "../../../../../snowlake-glue-jobs/01_bronze/bronze_matomo_funnels.py"
  job-bronze-matomo-funnels-flow             = "../../../../../snowlake-glue-jobs/01_bronze/bronze_matomo_funnels_flow.py"
  job-bronze-matomo-funnels-metrics          = "../../../../../snowlake-glue-jobs/01_bronze/bronze_matomo_funnels_metrics.py"
  job-bronze-matomo-pages                    = "../../../../../snowlake-glue-jobs/01_bronze/bronze_matomo_pages.py"
  job-bronze-matomo-region                   = "../../../../../snowlake-glue-jobs/01_bronze/bronze_matomo_region.py"
  job-bronze-matomo-visitfrequency           = "../../../../../snowlake-glue-jobs/01_bronze/bronze_matomo_visitfrequency.py"
  job-bronze-matomo-visitorinterest-count    = "../../../../../snowlake-glue-jobs/01_bronze/bronze_matomo_visitorinterest_count.py"
  job-bronze-matomo-visitorinterest-days     = "../../../../../snowlake-glue-jobs/01_bronze/bronze_matomo_visitorinterest_days.py"
  job-bronze-matomo-visitorinterest-duration = "../../../../../snowlake-glue-jobs/01_bronze/bronze_matomo_visitorinterest_duration.py"
  job-bronze-matomo-visitorinterest-page     = "../../../../../snowlake-glue-jobs/01_bronze/bronze_matomo_visitorinterest_page.py"
  job-bronze-matomo-visittime                = "../../../../../snowlake-glue-jobs/01_bronze/bronze_matomo_visittime.py"
  job-bronze-netsuite                        = "../../../../../snowlake-glue-jobs/01_bronze/bronze_netsuite.py"
  job-bronze-ref-auth-compte                 = "../../../../../snowlake-glue-jobs/01_bronze/bronze_ref_auth_compte.py"
  job-bronze-ref-emetteur                    = "../../../../../snowlake-glue-jobs/01_bronze/bronze_ref_emetteur.py"
  job-bronze-scodify-project                 = "../../../../../snowlake-glue-jobs/01_bronze/bronze_scodify_project.py"
  job-bronze-superoffice                     = "../../../../../snowlake-glue-jobs/01_bronze/bronze_superoffice.py"
  job-bronze-visma-customerinvoice           = "../../../../../snowlake-glue-jobs/01_bronze/bronze_visma_customerinvoice.py"
  job-bronze-visma-employee                  = "../../../../../snowlake-glue-jobs/01_bronze/bronze_visma_employee.py"
  job-bronze-visma-inventory                 = "../../../../../snowlake-glue-jobs/01_bronze/bronze_visma_inventory.py"
  job-bronze-visma-glt                       = "../../../../../snowlake-glue-jobs/01_bronze/bronze_visma_general_ledger_transaction.py"
  job-bronze-visma-ledger                    = "../../../../../snowlake-glue-jobs/01_bronze/bronze_visma_ledger.py"
  job-bronze-visma-salesorder                = "../../../../../snowlake-glue-jobs/01_bronze/bronze_visma_salesorder.py"
  job-bronze-visma-subscription              = "../../../../../snowlake-glue-jobs/01_bronze/bronze_visma_subscription.py"

  # Jobs Silver

  job-silver-ar-gu                 = "../../../../../snowlake-glue-jobs/02_silver/silver_ar_gu.py"
  job-silver-ar-contact            = "../../../../../snowlake-glue-jobs/02_silver/silver_ar_contact.py"
  job-silver-ar-pouvoir            = "../../../../../snowlake-glue-jobs/02_silver/silver_ar_pouvoir.py"
  job-silver-ar-historique-pouvoir = "../../../../../snowlake-glue-jobs/02_silver/silver_ar_historique_pouvoir.py"

  job-silver-archives-declaration-choix-destinataire = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_declaration_choix_destinataire.py"
  job-silver-archives-declaration-consultation       = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_declaration_consultation.py"
  job-silver-archives-declaration-declaration        = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_declaration_declaration.py"
  job-silver-archives-declaration-declaration-data   = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_declaration_declaration_data.py"
  job-silver-archives-declaration-declaration-geom   = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_declaration_declaration_geom.py"
  job-silver-archives-declaration-demande-precisions = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_declaration_demande_precisions.py"
  job-silver-archives-declaration-document           = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_declaration_document.py"
  job-silver-archives-declaration-relance            = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_declaration_relance.py"
  job-silver-archives-declaration-reponse            = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_declaration_reponse.py"

  job-silver-archives-reponse-declaration      = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_reponse_declaration.py"
  job-silver-archives-reponse-declaration-data = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_reponse_declaration_data.py"
  job-silver-archives-reponse-declaration-geom = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_reponse_declaration_geom.py"
  job-silver-archives-reponse-dossier          = "../../../../../snowlake-glue-jobs/02_silver/silver_reponse_dossier_archives.py"
  job-silver-archives-reponse-envoi            = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_reponse_envoi.py"
  job-silver-archives-reponse-reponse          = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_reponse_reponse.py"
  job-silver-archives-reponse-reponse-data     = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_reponse_reponse_data.py"

  job-silver-archives-declaration-choix-destinataire-test = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_declaration_choix_destinataire_test.py"
  job-silver-archives-declaration-consultation-test       = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_declaration_consultation_test.py"
  job-silver-archives-declaration-declaration-test        = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_declaration_declaration_test.py"
  job-silver-archives-declaration-declaration-data-test   = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_declaration_declaration_data_test.py"
  job-silver-archives-declaration-declaration-geom-test   = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_declaration_declaration_geom_test.py"
  job-silver-archives-declaration-demande-precisions-test = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_declaration_demande_precisions_test.py"
  job-silver-archives-declaration-document-test           = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_declaration_document_test.py"
  job-silver-archives-declaration-relance-test            = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_declaration_relance_test.py"
  job-silver-archives-declaration-reponse-test            = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_declaration_reponse_test.py"

  job-silver-archives-reponse-declaration-test      = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_reponse_declaration_test.py"
  job-silver-archives-reponse-declaration-data-test = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_reponse_declaration_data_test.py"
  job-silver-archives-reponse-declaration-geom-test = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_reponse_declaration_geom_test.py"
  job-silver-archives-reponse-dossier-test          = "../../../../../snowlake-glue-jobs/02_silver/silver_reponse_dossier_archives_test.py"
  job-silver-archives-reponse-envoi-test            = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_reponse_envoi_test.py"
  job-silver-archives-reponse-reponse-test          = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_reponse_reponse_test.py"
  job-silver-archives-reponse-reponse-data-test     = "../../../../../snowlake-glue-jobs/02_silver/silver_archives_reponse_reponse_data_test.py"


  job-silver-atlog-affaire              = "../../../../../snowlake-glue-jobs/02_silver/silver_atlog_affaire.py"
  job-silver-atlog-client-arb           = "../../../../../snowlake-glue-jobs/02_silver/silver_atlog_client_arb.py"
  job-silver-atlog-client-ligne         = "../../../../../snowlake-glue-jobs/02_silver/silver_atlog_client_ligne.py"
  job-silver-atlog-client               = "../../../../../snowlake-glue-jobs/02_silver/silver_atlog_client.py"
  job-silver-atlog-facture-client-liste = "../../../../../snowlake-glue-jobs/02_silver/silver_atlog_facture_client_liste.py"
  job-silver-atlog-facture-client       = "../../../../../snowlake-glue-jobs/02_silver/silver_atlog_facture_client.py"
  job-silver-atlog-ordref               = "../../../../../snowlake-glue-jobs/02_silver/silver_atlog_ordref.py"
  job-silver-atlog-tresorerie           = "../../../../../snowlake-glue-jobs/02_silver/silver_atlog_tresorerie.py"

  job-silver-auth-app                 = "../../../../../snowlake-glue-jobs/02_silver/silver_auth_app.py"
  job-silver-auth-compte              = "../../../../../snowlake-glue-jobs/02_silver/silver_auth_compte.py"
  job-silver-auth-compte-notification = "../../../../../snowlake-glue-jobs/02_silver/silver_auth_compte_notification.py"
  job-silver-auth-compte-to-app       = "../../../../../snowlake-glue-jobs/02_silver/silver_auth_compte_to_app.py"
  job-silver-auth-compte-to-client    = "../../../../../snowlake-glue-jobs/02_silver/silver_auth_compte_to_client.py"
  job-silver-auth-compte-to-profil    = "../../../../../snowlake-glue-jobs/02_silver/silver_auth_compte_to_profil.py"
  job-silver-auth-droit-app           = "../../../../../snowlake-glue-jobs/02_silver/silver_auth_droit_app.py"
  job-silver-auth-droit-app-to-profil = "../../../../../snowlake-glue-jobs/02_silver/silver_auth_droit_app_to_profil.py"
  job-silver-auth-profil              = "../../../../../snowlake-glue-jobs/02_silver/silver_auth_profil.py"

  job-silver-da-dpa = "../../../../../snowlake-glue-jobs/02_silver/silver_da_dpa.py"

  job-silver-declaration-choix-destinataire   = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_choix_destinataire.py"
  job-silver-declaration-consultation         = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_consultation.py"
  job-silver-declaration-declaration          = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_declaration.py"
  job-silver-declaration-declaration-data     = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_declaration_data.py"
  job-silver-declaration-declaration-geom     = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_declaration_geom.py"
  job-silver-declaration-demande-precisions   = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_demande_precisions.py"
  job-silver-declaration-document             = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_document.py"
  job-silver-declaration-parametre-agence     = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_parametre_agence.py"
  job-silver-declaration-piece-jointe         = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_piece_jointe.py"
  job-silver-declaration-piece-jointe-reponse = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_piece_jointe_reponse.py"
  job-silver-declaration-relance              = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_relance.py"
  job-silver-declaration-reponse              = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_reponse.py"

  job-silver-declaration-choix-destinataire-test   = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_choix_destinataire_test.py"
  job-silver-declaration-consultation-test         = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_consultation_test.py"
  job-silver-declaration-declaration-test          = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_declaration_test.py"
  job-silver-declaration-declaration-data-test     = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_declaration_data_test.py"
  job-silver-declaration-declaration-geom-test     = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_declaration_geom_test.py"
  job-silver-declaration-demande-precisions-test   = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_demande_precisions_test.py"
  job-silver-declaration-document-test             = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_document_test.py"
  job-silver-declaration-parametre-agence-test     = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_parametre_agence_test.py"
  job-silver-declaration-piece-jointe-test         = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_piece_jointe_test.py"
  job-silver-declaration-piece-jointe-reponse-test = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_piece_jointe_reponse_test.py"
  job-silver-declaration-relance-test              = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_relance_test.py"
  job-silver-declaration-reponse-test              = "../../../../../snowlake-glue-jobs/02_silver/silver_declaration_reponse_test.py"
  job-silver-demat-document                        = "../../../../../snowlake-glue-jobs/02_silver/silver_demat_document.py"
  job-silver-demat-document-history                = "../../../../../snowlake-glue-jobs/02_silver/silver_demat_document_history.py"
  job-silver-ems-entitlementusage                  = "../../../../../snowlake-glue-jobs/02_silver/silver_ems_entitlementusage.py"

  job-silver-formulaire-document   = "../../../../../snowlake-glue-jobs/02_silver/silver_formulaire_document.py"
  job-silver-formulaire-envoi      = "../../../../../snowlake-glue-jobs/02_silver/silver_formulaire_envoi.py"
  job-silver-formulaire-formulaire = "../../../../../snowlake-glue-jobs/02_silver/silver_formulaire_formulaire.py"
  job-silver-formulaire-modele     = "../../../../../snowlake-glue-jobs/02_silver/silver_formulaire_modele.py"

  job-silver-jira-issue       = "../../../../../snowlake-glue-jobs/02_silver/silver_jira_issue.py"
  job-silver-jira-project     = "../../../../../snowlake-glue-jobs/02_silver/silver_jira_project.py"
  job-silver-jira-team-member = "../../../../../snowlake-glue-jobs/02_silver/silver_jira_team_member.py"
  job-silver-jira-worklog     = "../../../../../snowlake-glue-jobs/02_silver/silver_jira_worklog.py"

  job-silver-log-auth-log-connexion = "../../../../../snowlake-glue-jobs/02_silver/silver_log_auth_log_connexion.py"

  job-silver-lucca-leave = "../../../../../snowlake-glue-jobs/02_silver/silver_lucca_leave.py"

  job-silver-netsuite-charge           = "../../../../../snowlake-glue-jobs/02_silver/silver_netsuite_charge.py"
  job-silver-netsuite-invoice          = "../../../../../snowlake-glue-jobs/02_silver/silver_netsuite_invoice.py"
  job-silver-netsuite-item             = "../../../../../snowlake-glue-jobs/02_silver/silver_netsuite_item.py"
  job-silver-netsuite-revenue          = "../../../../../snowlake-glue-jobs/02_silver/silver_netsuite_revenue.py"
  job-silver-netsuite-sales-order      = "../../../../../snowlake-glue-jobs/02_silver/silver_netsuite_sales_order.py"
  job-silver-netsuite-transaction-line = "../../../../../snowlake-glue-jobs/02_silver/silver_netsuite_transaction_line.py"

  job-silver-ref = "../../../../../snowlake-glue-jobs/02_silver/silver_ref.py"

  job-silver-ref-iceberg = "../../../../../snowlake-glue-jobs/02_silver/silver_ref_iceberg.py"

  job-silver-reponse-declaration      = "../../../../../snowlake-glue-jobs/02_silver/silver_reponse_declaration.py"
  job-silver-reponse-declaration-data = "../../../../../snowlake-glue-jobs/02_silver/silver_reponse_declaration_data.py"
  job-silver-reponse-declaration-geom = "../../../../../snowlake-glue-jobs/02_silver/silver_reponse_declaration_geom.py"
  job-silver-reponse-envoi            = "../../../../../snowlake-glue-jobs/02_silver/silver_reponse_envoi.py"
  job-silver-reponse-reponse          = "../../../../../snowlake-glue-jobs/02_silver/silver_reponse_reponse.py"
  job-silver-reponse-reponse-data     = "../../../../../snowlake-glue-jobs/02_silver/silver_reponse_reponse_data.py"
  job-silver-reponse-type-modele      = "../../../../../snowlake-glue-jobs/02_silver/silver_reponse_type_modele.py"

  job-silver-reponse-declaration-test      = "../../../../../snowlake-glue-jobs/02_silver/silver_reponse_declaration_test.py"
  job-silver-reponse-declaration-data-test = "../../../../../snowlake-glue-jobs/02_silver/silver_reponse_declaration_data_test.py"
  job-silver-reponse-declaration-geom-test = "../../../../../snowlake-glue-jobs/02_silver/silver_reponse_declaration_geom_test.py"
  job-silver-reponse-envoi-test            = "../../../../../snowlake-glue-jobs/02_silver/silver_reponse_envoi_test.py"
  job-silver-reponse-reponse-test          = "../../../../../snowlake-glue-jobs/02_silver/silver_reponse_reponse_test.py"
  job-silver-reponse-reponse-data-test     = "../../../../../snowlake-glue-jobs/02_silver/silver_reponse_reponse_data_test.py"
  job-silver-reponse-type-modele-test      = "../../../../../snowlake-glue-jobs/02_silver/silver_reponse_type_modele_test.py"

  job-silver-scodify-project = "../../../../../snowlake-glue-jobs/02_silver/silver_scodify_project.py"

  job-silver-scream-chapitre               = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_chapitre.py"
  job-silver-scream-client-to-produit      = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_client_to_produit.py"
  job-silver-scream-client-regle-factu     = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_client_regle_factu.py"
  job-silver-scream-condition-paiement     = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_condition_paiement.py"
  job-silver-scream-conso                  = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_conso.py"
  job-silver-scream-conso-achetee          = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_conso_achetee.py"
  job-silver-scream-conso-conso            = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_conso_conso.py"
  job-silver-scream-conso-consommee        = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_conso_consommee.py"
  job-silver-scream-conso-data             = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_conso_data.py"
  job-silver-scream-conso-old              = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_conso_old.py"
  job-silver-scream-conso-plan             = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_conso_plan.py"
  job-silver-scream-conso-to-produit       = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_conso_to_produit.py"
  job-silver-scream-contact                = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_contact.py"
  job-silver-scream-contact-to-entite      = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_contact_to_entite.py"
  job-silver-scream-entite                 = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_entite.py"
  job-silver-scream-entite-to-societe      = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_entite_to_societe.py"
  job-silver-scream-facturation-terme-echu = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_facturation_terme_echu.py"
  job-silver-scream-licence-client         = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_licence_client.py"
  job-silver-scream-offre                  = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_offre.py"
  job-silver-scream-origine                = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_origine.py"
  job-silver-scream-payeur-echeance        = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_payeur_echeance.py"
  job-silver-scream-produit                = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_produit.py"
  job-silver-scream-produit-to-produit     = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_produit_to_produit.py"
  job-silver-scream-regle-facturation      = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_regle_facturation.py"
  job-silver-scream-secteur                = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_secteur.py"
  job-silver-scream-societe                = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_societe.py"
  job-silver-scream-societe-regle-factu    = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_societe_regle_factu.py"
  job-silver-scream-sous-chapitre          = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_sous_chapitre.py"
  job-silver-scream-unite-conso            = "../../../../../snowlake-glue-jobs/02_silver/silver_scream_unite_conso.py"

  job-silver-sogetask-affaire                     = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_affaire.py"
  job-silver-sogetask-activite-tache              = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_activite_tache.py"
  job-silver-sogetask-bdc-ratio-co                = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_bdc_ratio_co.py"
  job-silver-sogetask-bon-commande                = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_bon_commande.py"
  job-silver-sogetask-campagne                    = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_campagne.py"
  job-silver-sogetask-categorie-tache             = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_categorie_tache.py"
  job-silver-sogetask-commentaire-tache-evenement = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_commentaire_tache_evenement.py"
  job-silver-sogetask-echeance                    = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_echeance.py"
  job-silver-sogetask-facture                     = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_facture.py"
  job-silver-sogetask-file-tache                  = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_file_tache.py"
  job-silver-sogetask-devis                       = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_devis.py"
  job-silver-sogetask-devise                      = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_devise.py"
  job-silver-sogetask-encaissement                = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_encaissement.py"
  job-silver-sogetask-motif-affaire               = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_motif_affaire.py"
  job-silver-sogetask-produit-devis               = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_produit_devis.py"
  job-silver-sogetask-produit-facture             = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_produit_facture.py"
  job-silver-sogetask-recouvrement                = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_recouvrement.py"
  job-silver-sogetask-regle-calcul-envoi          = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_regle_calcul_envoi.py"
  job-silver-sogetask-tache                       = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_tache.py"
  job-silver-sogetask-tache-to-compte             = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_tache_to_compte.py"
  job-silver-sogetask-tache-to-contact            = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_tache_to_contact.py"
  job-silver-sogetask-tache-to-detail             = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_tache_to_detail.py"
  job-silver-sogetask-tache-to-devis              = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_tache_to_devis.py"
  job-silver-sogetask-tva-taux                    = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_tva_taux.py"
  job-silver-sogetask-tva-code                    = "../../../../../snowlake-glue-jobs/02_silver/silver_sogetask_tva_code.py"

  job-silver-stats-billing-fr             = "../../../../../snowlake-glue-jobs/02_silver/silver_stats_billing_fr.py"
  job-silver-stats-billing-nl             = "../../../../../snowlake-glue-jobs/02_silver/silver_stats_billing_nl.py"
  job-silver-stats-billing-no             = "../../../../../snowlake-glue-jobs/02_silver/silver_stats_billing_no.py"
  job-silver-stats-consumption-fr         = "../../../../../snowlake-glue-jobs/02_silver/silver_stats_consumption_fr.py"
  job-silver-stats-consumption-nl         = "../../../../../snowlake-glue-jobs/02_silver/silver_stats_consumption_nl.py"
  job-silver-stats-opportunity-fr         = "../../../../../snowlake-glue-jobs/02_silver/silver_stats_opportunity_fr.py"
  job-silver-stats-opportunity-nl         = "../../../../../snowlake-glue-jobs/02_silver/silver_stats_opportunity_nl.py"
  job-silver-stats-opportunity-no         = "../../../../../snowlake-glue-jobs/02_silver/silver_stats_opportunity_no.py"
  job-silver-stats-opportunity-salesforce = "../../../../../snowlake-glue-jobs/02_silver/silver_stats_opportunity_salesforce.py"

  # job-silver-stats-pipeline                = "../../../../../snowlake-glue-jobs/02_silver/silver_stats_pipeline.py"
  job-silver-stats-factu-gds               = "../../../../../snowlake-glue-jobs/02_silver/silver_stats_factu_gds.py"
  job-silver-stats-factu-gx                = "../../../../../snowlake-glue-jobs/02_silver/silver_stats_factu_gx.py"
  job-silver-stats-factu-sglk              = "../../../../../snowlake-glue-jobs/02_silver/silver_stats_factu_sglk.py"
  job-silver-stats-vente                   = "../../../../../snowlake-glue-jobs/02_silver/silver_stats_vente.py"
  job-silver-stats-vente-couplage          = "../../../../../snowlake-glue-jobs/02_silver/silver_stats_vente_couplage.py"
  job-silver-stats-funnel                  = "../../../../../snowlake-glue-jobs/02_silver/silver_stats_funnel.py"
  job-silver-stats-funnel-salesforce       = "../../../../../snowlake-glue-jobs/02_silver/silver_stats_funnel_salesforce.py"
  job-silver-stats-tache-delais-reponse    = "../../../../../snowlake-glue-jobs/02_silver/silver_stats_tache_delais_reponse.py"
  job-silver-stats-tache-delais-traitement = "../../../../../snowlake-glue-jobs/02_silver/silver_stats_tache_delais_traitement.py"
  job-silver-salesforce                    = "../../../../../snowlake-glue-jobs/02_silver/silver_salesforce.py"

  job-silver-statistic-etat-stock        = "../../../../../snowlake-glue-jobs/02_silver/silver_statistic_etat_stock.py"
  job-silver-statistic-conso-agence      = "../../../../../snowlake-glue-jobs/02_silver/silver_statistic_conso_agence.py"
  job-silver-statistic-conso-emetteur    = "../../../../../snowlake-glue-jobs/02_silver/silver_statistic_conso_emetteur.py"
  job-silver-statistic-conso-utilisateur = "../../../../../snowlake-glue-jobs/02_silver/silver_statistic_conso_utilisateur.py"
  job-silver-statistic-nb-envoi-jour     = "../../../../../snowlake-glue-jobs/02_silver/silver_statistic_nb_envoi_jour.py"
  job-silver-statistic-stocks-client     = "../../../../../snowlake-glue-jobs/02_silver/silver_statistic_stocks_client.py"

  job-silver-superoffice-person  = "../../../../../snowlake-glue-jobs/02_silver/silver_superoffice_person.py"
  job-silver-superoffice-contact = "../../../../../snowlake-glue-jobs/02_silver/silver_superoffice_contact.py"

  job-silver-tribe-opportunity-line = "../../../../../snowlake-glue-jobs/02_silver/silver_tribe_opportunity_line.py"

  job-silver-visma-customerinvoice = "../../../../../snowlake-glue-jobs/02_silver/silver_visma_customerinvoice.py"
  job-silver-visma-salesorder      = "../../../../../snowlake-glue-jobs/02_silver/silver_visma_salesorder.py"

  # Jobs Gold

  job-gold-access-app          = "../../../../../snowlake-glue-jobs/03_gold/gold_access_app.py"
  job-gold-account             = "../../../../../snowlake-glue-jobs/03_gold/gold_account.py"
  job-gold-account-action      = "../../../../../snowlake-glue-jobs/03_gold/gold_account_action.py"
  job-gold-agence              = "../../../../../snowlake-glue-jobs/03_gold/gold_agence.py"
  job-gold-ar-amiante          = "../../../../../snowlake-glue-jobs/03_gold/gold_ar_amiante.py"
  job-gold-ar-contact          = "../../../../../snowlake-glue-jobs/03_gold/gold_ar_contact.py"
  job-gold-billing             = "../../../../../snowlake-glue-jobs/03_gold/gold_billing.py"
  job-gold-bon-commande        = "../../../../../snowlake-glue-jobs/03_gold/gold_bon_commande.py"
  job-gold-client              = "../../../../../snowlake-glue-jobs/03_gold/gold_client.py"
  job-gold-client-to-produit   = "../../../../../snowlake-glue-jobs/03_gold/gold_client_to_produit.py"
  job-gold-connection          = "../../../../../snowlake-glue-jobs/03_gold/gold_connection.py"
  job-gold-consovalo           = "../../../../../snowlake-glue-jobs/03_gold/gold_consovalo.py"
  job-gold-contact             = "../../../../../snowlake-glue-jobs/03_gold/gold_contact.py"
  job-gold-consumption         = "../../../../../snowlake-glue-jobs/03_gold/gold_consumption.py"
  job-gold-da-dpa              = "../../../../../snowlake-glue-jobs/03_gold/gold_da_dpa.py"
  job-gold-deal                = "../../../../../snowlake-glue-jobs/03_gold/gold_deal.py"
  job-gold-declarant           = "../../../../../snowlake-glue-jobs/03_gold/gold_declarant.py"
  job-gold-declarant-archives  = "../../../../../snowlake-glue-jobs/03_gold/gold_declarant_archives.py"
  job-gold-devis               = "../../../../../snowlake-glue-jobs/03_gold/gold_devis.py"
  job-gold-demat-document      = "../../../../../snowlake-glue-jobs/03_gold/gold_demat_document.py"
  job-gold-dmc                 = "../../../../../snowlake-glue-jobs/03_gold/gold_dmc.py"
  job-gold-dossier             = "../../../../../snowlake-glue-jobs/03_gold/gold_dossier.py"
  job-gold-employee            = "../../../../../snowlake-glue-jobs/03_gold/gold_employee.py"
  job-gold-entitlementusage    = "../../../../../snowlake-glue-jobs/03_gold/gold_entitlementusage.py"
  job-gold-enquiry             = "../../../../../snowlake-glue-jobs/03_gold/gold_enquiry.py"
  job-gold-etat-stock          = "../../../../../snowlake-glue-jobs/03_gold/gold_etat_stock.py"
  job-gold-event               = "../../../../../snowlake-glue-jobs/03_gold/gold_event.py"
  job-gold-exploitant          = "../../../../../snowlake-glue-jobs/03_gold/gold_exploitant.py"
  job-gold-exploitant-archives = "../../../../../snowlake-glue-jobs/03_gold/gold_exploitant_archives.py"
  job-gold-flydoc              = "../../../../../snowlake-glue-jobs/03_gold/gold_flydoc.py"
  job-gold-groupe-client       = "../../../../../snowlake-glue-jobs/03_gold/gold_groupe_client.py"
  job-gold-invoice             = "../../../../../snowlake-glue-jobs/03_gold/gold_invoice.py"
  job-gold-issue               = "../../../../../snowlake-glue-jobs/03_gold/gold_issue.py"
  job-gold-issue-worklog       = "../../../../../snowlake-glue-jobs/03_gold/gold_issue_worklog.py"
  job-gold-leave               = "../../../../../snowlake-glue-jobs/03_gold/gold_leave.py"
  job-gold-log                 = "../../../../../snowlake-glue-jobs/03_gold/gold_log.py"
  job-gold-opportunity         = "../../../../../snowlake-glue-jobs/03_gold/gold_opportunity.py"
  job-gold-organization        = "../../../../../snowlake-glue-jobs/03_gold/gold_organization.py"
  job-gold-pmsr                = "../../../../../snowlake-glue-jobs/03_gold/gold_pmsr.py"
  job-gold-project             = "../../../../../snowlake-glue-jobs/03_gold/gold_project.py"
  job-gold-produit             = "../../../../../snowlake-glue-jobs/03_gold/gold_produit.py"
  # job-gold-pipeline            = "../../../../../snowlake-glue-jobs/03_gold/gold_pipeline.py"
  # job-gold-pipeline-event      = "../../../../../snowlake-glue-jobs/03_gold/gold_pipeline_event.py"
  job-gold-remaining          = "../../../../../snowlake-glue-jobs/03_gold/gold_remaining.py"
  job-gold-renew-conso        = "../../../../../snowlake-glue-jobs/03_gold/gold_renew_conso.py"
  job-gold-renew-licence      = "../../../../../snowlake-glue-jobs/03_gold/gold_renew_licence.py"
  job-gold-revenue            = "../../../../../snowlake-glue-jobs/03_gold/gold_revenue.py"
  job-gold-funnel             = "../../../../../snowlake-glue-jobs/03_gold/gold_funnel.py"
  job-gold-funnel-salesforce  = "../../../../../snowlake-glue-jobs/03_gold/gold_funnel_salesforce.py"
  job-gold-sales-activity     = "../../../../../snowlake-glue-jobs/03_gold/gold_sales_activity.py"
  job-gold-sending            = "../../../../../snowlake-glue-jobs/03_gold/gold_sending.py"
  job-gold-sending-agg        = "../../../../../snowlake-glue-jobs/03_gold/gold_sending_agg.py"
  job-gold-sending-init       = "../../../../../snowlake-glue-jobs/03_gold/gold_sending_init.py"
  job-gold-sending-mobile     = "../../../../../snowlake-glue-jobs/03_gold/gold_sending_mobile.py"
  job-gold-sending-to-produit = "../../../../../snowlake-glue-jobs/03_gold/gold_sending_to_produit.py"
  job-gold-societe-entite     = "../../../../../snowlake-glue-jobs/03_gold/gold_societe_entite.py"
  job-gold-stock              = "../../../../../snowlake-glue-jobs/03_gold/gold_stock.py"
  job-gold-support            = "../../../../../snowlake-glue-jobs/03_gold/gold_support.py"
  job-gold-switcher           = "../../../../../snowlake-glue-jobs/03_gold/gold_switcher.py"
  job-gold-tache-to-detail    = "../../../../../snowlake-glue-jobs/03_gold/gold_tache_to_detail.py"
  job-gold-team-member        = "../../../../../snowlake-glue-jobs/03_gold/gold_team_member.py"
  job-gold-transaction        = "../../../../../snowlake-glue-jobs/03_gold/gold_transaction.py"

  job-bronze-webkua        = "../../../../../snowlake-glue-jobs/01_bronze/bronze_webkua.py"
  job-bronze-tribe         = "../../../../../snowlake-glue-jobs/01_bronze/bronze_tribe.py"
  job-bronze-afas-contract = "../../../../../snowlake-glue-jobs/01_bronze/bronze_afas_contract.py"
  job-bronze-afas-gl       = "../../../../../snowlake-glue-jobs/01_bronze/bronze_afas_general_ledger.py"
  job-bronze-afas-customer = "../../../../../snowlake-glue-jobs/01_bronze/bronze_afas_customer.py"
  job-bronze-afas-mrr      = "../../../../../snowlake-glue-jobs/01_bronze/bronze_afas_mrr.py"
  job-bronze-ems-test      = "../../../../../snowlake-glue-jobs/01_bronze/bronze_ems_test.py"

  job-gold-test = "../../../../../snowlake-glue-jobs/03_gold/gold_test.py"


  // ROLE IAM
  lakeformation_role = "ServiceRoleForLakeformationAccess"

  // SECRET MANAGER
  salesforce = {
    ACCESS_TOKEN   = data.vault_kv_secret_v2.salesforce.data.ACCESS_TOKEN
    JWT_TOKEN      = data.vault_kv_secret_v2.salesforce.data.JWT_TOKEN
    salesforce-crt = data.vault_kv_secret_v2.salesforce.data.salesforce-crt
  }
}
