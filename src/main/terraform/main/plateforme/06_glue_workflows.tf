# Workflow ingest Entitlement Usage

# resource "aws_glue_workflow" "snowlake_workflow_ingest_entitlementusage" {
#   name = "${terraform.workspace}_snowlake_workflow_ingest_entitlementusage"
# }

# resource "aws_glue_trigger" "trigger_bronze_entitlementusage" {
#   name          = "${terraform.workspace}_trigger_bronze_entitlementusage"
#   type          = "ON_DEMAND"
#   workflow_name = aws_glue_workflow.snowlake_workflow_ingest_entitlementusage.name

#   actions {
#     job_name = module.bronze_ems_entitlementusage.name
#   }
# }

# resource "aws_glue_trigger" "trigger_silver_entitlementusage" {
#   name          = "${terraform.workspace}_trigger_silver_ems_entitlementusage"
#   type          = "ON_DEMAND"
#   workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

#   predicate {
#     conditions {
#       job_name = module.silver_scream_entite_to_societe.name
#       state    = "SUCCEEDED"
#     }
#     conditions {
#       job_name = module.silver_scream_entite.name
#       state    = "SUCCEEDED"
#     }
#   }
#   actions {
#     job_name = module.silver_ems_entitlementusage.name
#   }
# }

# resource "aws_glue_trigger" "trigger_gold_entitlementusage" {
#   name          = "${terraform.workspace}_trigger_gold_ems_entitlementusage"
#   type          = "ON_DEMAND"
#   workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

#   predicate {
#     conditions {
#       job_name = module.silver_ems_entitlementusage.name
#       state    = "SUCCEEDED"
#     }
#   }

#   actions {
#     job_name = module.gold_entitlementusage.name
#   }
# }

# Workflow Ingest External

resource "aws_glue_workflow" "snowlake_workflow_ingest_external" {
  name = "${terraform.workspace}_snowlake_workflow_ingest_external"
}

resource "aws_glue_trigger" "trigger_bronze_external" {
  name          = "${terraform.workspace}_trigger_bronze_external"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.snowlake_workflow_ingest_external.name

  actions {
    job_name = module.bronze_flydoc_ondemand.name
  }
}


# Workflow Ingest Visma

resource "aws_glue_workflow" "snowlake_workflow_ingest_visma" {
  name = "${terraform.workspace}_snowlake_workflow_ingest_visma"
}

resource "aws_glue_trigger" "trigger_bronze_visma" {
  name          = "${terraform.workspace}_trigger_bronze_visma"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.snowlake_workflow_ingest_visma.name

  actions {
    job_name = module.bronze_visma_customerinvoice.name
  }
  actions {
    job_name = module.bronze_visma_employee.name
  }
  actions {
    job_name = module.bronze_visma_inventory.name
  }
  actions {
    job_name = module.bronze_visma_salesorder.name
  }
  actions {
    job_name = module.bronze_visma_general_ledger_transaction.name
  }
}


# Workflow Ingest Jira

resource "aws_glue_workflow" "snowlake_workflow_ingest_jira" {
  name = "${terraform.workspace}_snowlake_workflow_ingest_jira"
}

resource "aws_glue_trigger" "trigger_bronze_jira" {
  name          = "${terraform.workspace}_trigger_bronze_jira"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.snowlake_workflow_ingest_jira.name

  actions {
    job_name = module.bronze_jira_issue.name
  }
  actions {
    job_name = module.bronze_jira_project.name
  }
  actions {
    job_name = module.bronze_jira_worklog.name
  }
  actions {
    job_name = module.bronze_jira_worklog_deleted.name
  }
  actions {
    job_name = module.bronze_jira_team_member.name
  }
}

# Workflow Ingest Lucca

resource "aws_glue_workflow" "snowlake_workflow_ingest_lucca" {
  name = "${terraform.workspace}_snowlake_workflow_ingest_lucca"
}

resource "aws_glue_trigger" "trigger_bronze_lucca" {
  name          = "${terraform.workspace}_trigger_bronze_lucca"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.snowlake_workflow_ingest_lucca.name

  actions {
    job_name = module.bronze_lucca_leave.name
  }
}

# Workflow Ingest Matomo

resource "aws_glue_workflow" "snowlake_workflow_ingest_matomo" {
  name = "${terraform.workspace}_snowlake_workflow_ingest_matomo"
}

resource "aws_glue_trigger" "trigger_bronze_matomo_1" {
  name          = "${terraform.workspace}_trigger_bronze_matomo_1"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.snowlake_workflow_ingest_matomo.name

  actions {
    job_name = module.bronze_matomo_forms.name
  }
  actions {
    job_name = module.bronze_matomo_funnels.name
  }
  actions {
    job_name = module.bronze_matomo_pages.name
  }
  actions {
    job_name = module.bronze_matomo_visitfrequency.name
  }
  actions {
    job_name = module.bronze_matomo_visitorinterest_count.name
  }
  actions {
    job_name = module.bronze_matomo_visitorinterest_days.name
  }
  actions {
    job_name = module.bronze_matomo_visitorinterest_duration.name
  }
  actions {
    job_name = module.bronze_matomo_visitorinterest_page.name
  }
  actions {
    job_name = module.bronze_matomo_visittime.name
  }
}

resource "aws_glue_trigger" "trigger_bronze_matomo_2" {
  name          = "${terraform.workspace}_trigger_bronze_matomo_2"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_ingest_matomo.name

  predicate {
    conditions {
      job_name = module.bronze_matomo_forms.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.bronze_matomo_funnels.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.bronze_matomo_city.name
  }
  actions {
    job_name = module.bronze_matomo_country.name
  }
  actions {
    job_name = module.bronze_matomo_forms_pages.name
  }
  actions {
    job_name = module.bronze_matomo_funnels_flow.name
  }
  actions {
    job_name = module.bronze_matomo_funnels_metrics.name
  }
  actions {
    job_name = module.bronze_matomo_region.name
  }
}

# Workflow Ingest Netsuite

resource "aws_glue_workflow" "snowlake_workflow_ingest_netsuite" {
  name = "${terraform.workspace}_snowlake_workflow_ingest_netsuite"
}

resource "aws_glue_trigger" "trigger_bronze_netsuite_1" {
  name          = "${terraform.workspace}_trigger_bronze_netsuite_1"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.snowlake_workflow_ingest_netsuite.name
  actions {
    job_name = module.bronze_netsuite_account.name
  }
  actions {
    job_name = module.bronze_netsuite_accountingperiod.name
  }
  actions {
    job_name = module.bronze_netsuite_charge.name
  }
  actions {
    job_name = module.bronze_netsuite_consolidatedexchangerate.name
  }
  actions {
    job_name = module.bronze_netsuite_customer.name
  }
  actions {
    job_name = module.bronze_netsuite_employee.name
  }
  actions {
    job_name = module.bronze_netsuite_entity.name
  }
  actions {
    job_name = module.bronze_netsuite_item.name
  }
  actions {
    job_name = module.bronze_netsuite_puk.name
  }
}

resource "aws_glue_trigger" "trigger_bronze_netsuite_2" {
  name          = "${terraform.workspace}_trigger_bronze_netsuite_2"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_ingest_netsuite.name

  predicate {
    conditions {
      job_name = module.bronze_netsuite_charge.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.bronze_netsuite_consolidatedexchangerate.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.bronze_netsuite_customer.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.bronze_netsuite_employee.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.bronze_netsuite_entity.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.bronze_netsuite_item.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.bronze_netsuite_revenueelement.name
  }
  actions {
    job_name = module.bronze_netsuite_revenueplan.name
  }
  actions {
    job_name = module.bronze_netsuite_subsidiary.name
  }
  actions {
    job_name = module.bronze_netsuite_subscription.name
  }
  actions {
    job_name = module.bronze_netsuite_subscriptionline.name
  }
  actions {
    job_name = module.bronze_netsuite_transaction.name
  }
}

resource "aws_glue_trigger" "trigger_bronze_netsuite_3" {
  name          = "${terraform.workspace}_trigger_bronze_netsuite_3"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_ingest_netsuite.name

  predicate {
    conditions {
      job_name = module.bronze_netsuite_revenueelement.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.bronze_netsuite_revenueplan.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.bronze_netsuite_subsidiary.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.bronze_netsuite_subscription.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.bronze_netsuite_subscriptionline.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.bronze_netsuite_transaction.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.bronze_netsuite_transactionline.name
  }
}

resource "aws_glue_trigger" "trigger_bronze_netsuite_4" {
  name          = "${terraform.workspace}_trigger_bronze_netsuite_4"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_ingest_netsuite.name

  predicate {
    conditions {
      job_name = module.bronze_netsuite_transactionline.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.bronze_netsuite_revenueplanplannedrevenue.name
  }
}

# Workflow Ingest Superoffice

resource "aws_glue_workflow" "snowlake_workflow_ingest_superoffice" {
  name = "${terraform.workspace}_snowlake_workflow_ingest_superoffice"
}

resource "aws_glue_trigger" "trigger_bronze_superoffice" {
  name          = "${terraform.workspace}_trigger_bronze_superoffice"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.snowlake_workflow_ingest_superoffice.name

  actions {
    job_name = module.bronze_superoffice.name
  }
}

# Workflow Ingest Tribe

resource "aws_glue_workflow" "snowlake_workflow_ingest_tribe" {
  name = "${terraform.workspace}_snowlake_workflow_ingest_tribe"
}

resource "aws_glue_trigger" "trigger_bronze_tribe" {
  name          = "${terraform.workspace}_trigger_bronze_tribe"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.snowlake_workflow_ingest_tribe.name

  actions {
    job_name = module.bronze_tribe.name
  }
}

# Workflow Ingest Afas

resource "aws_glue_workflow" "snowlake_workflow_ingest_afas" {
  name = "${terraform.workspace}_snowlake_workflow_ingest_afas"
}

resource "aws_glue_trigger" "trigger_bronze_afas" {
  name          = "${terraform.workspace}_trigger_bronze_afas"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.snowlake_workflow_ingest_afas.name

  actions {
    job_name = module.bronze_afas_contract.name
  }

  actions {
    job_name = module.bronze_afas_customer.name
  }

  actions {
    job_name = module.bronze_afas_mrr.name
  }
}

# Workflow Ingest Webkua

resource "aws_glue_workflow" "snowlake_workflow_ingest_webkua" {
  name = "${terraform.workspace}_snowlake_workflow_ingest_webkua"
}

resource "aws_glue_trigger" "trigger_bronze_webkua" {
  name          = "${terraform.workspace}_trigger_bronze_webkua"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.snowlake_workflow_ingest_webkua.name

  actions {
    job_name = module.bronze_webkua.name
  }
}

# Workflow Bronze Crawlers

resource "aws_glue_workflow" "snowlake_workflow_bronze_crawlers" {
  name = "${terraform.workspace}_snowlake_workflow_bronze_crawlers"
}

resource "aws_glue_trigger" "start_workflow_bronze_crawlers" {
  name          = "${terraform.workspace}_start_workflow_bronze_crawlers"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.snowlake_workflow_bronze_crawlers.name

  actions {
    job_name = module.tech_blank.name
  }
}

resource "aws_glue_trigger" "trigger_bronze_crawlers_rds_1" {
  name          = "${terraform.workspace}_trigger_bronze_crawlers_rds_1"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_bronze_crawlers.name

  predicate {
    conditions {
      job_name = module.tech_blank.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    crawler_name = "${terraform.workspace}_snowlake_crawler_scream"
  }
}

resource "aws_glue_trigger" "trigger_bronze_crawlers_rds_2" {
  name          = "${terraform.workspace}_trigger_bronze_crawlers_rds_2"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_bronze_crawlers.name

  predicate {
    conditions {
      job_name = module.tech_blank.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    crawler_name = "${terraform.workspace}_snowlake_crawler_referentiel"
  }
  actions {
    crawler_name = "${terraform.workspace}_snowlake_crawler_cbyd"
  }
}

# Workflow Referentiel Scodify

resource "aws_glue_workflow" "snowlake_workflow_referentiel_scodify" {
  name = "${terraform.workspace}_snowlake_workflow_referentiel_scodify"
}

resource "aws_glue_trigger" "trigger_bronze_referentiel_scodify" {
  name          = "${terraform.workspace}_trigger_bronze_referentiel_scodify"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.snowlake_workflow_referentiel_scodify.name

  actions {
    job_name = module.bronze_ref_auth_compte.name
  }
  actions {
    job_name = module.bronze_ref_emetteur.name
  }
  actions {
    job_name = module.bronze_scodify_project.name
  }
}

# Workflow Silver et Gold Jobs

resource "aws_glue_workflow" "snowlake_workflow_silver_gold_jobs" {
  name = "${terraform.workspace}_snowlake_workflow_silver_gold_jobs"
}

resource "aws_glue_trigger" "start_workflow_silver_gold_jobs" {
  name          = "${terraform.workspace}_start_workflow_silver_gold_jobs"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  actions {
    job_name = module.tech_blank.name
  }
}

# resource "aws_glue_trigger" "trigger_silver_archives_declaration" {
#   name          = "${terraform.workspace}_trigger_silver_archives_declaration"
#   type          = "CONDITIONAL"
#   workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

#   predicate {
#     conditions {
#       job_name = module.silver_declaration_choix_destinataire.name
#       state    = "SUCCEEDED"
#     }
#     conditions {
#       job_name = module.silver_declaration_consultation.name
#       state    = "SUCCEEDED"
#     }
#     conditions {
#       job_name = module.silver_declaration_declaration.name
#       state    = "SUCCEEDED"
#     }
#     conditions {
#       job_name = module.silver_declaration_declaration_data.name
#       state    = "SUCCEEDED"
#     }
#     conditions {
#       job_name = module.silver_declaration_declaration_geom.name
#       state    = "SUCCEEDED"
#     }
#     conditions {
#       job_name = module.silver_declaration_document.name
#       state    = "SUCCEEDED"
#     }
#     conditions {
#       job_name = module.silver_declaration_relance.name
#       state    = "SUCCEEDED"
#     }
#     conditions {
#       job_name = module.silver_declaration_reponse.name
#       state    = "SUCCEEDED"
#     }
#   }

#   actions {
#     job_name = module.silver_archives_declaration_choix_destinataire.name
#   }
#   actions {
#     job_name = module.silver_archives_declaration_consultation.name
#   }
#   actions {
#     job_name = module.silver_archives_declaration_declaration.name
#   }
#   actions {
#     job_name = module.silver_archives_declaration_declaration_data.name
#   }
#   actions {
#     job_name = module.silver_archives_declaration_declaration_geom.name
#   }
#   actions {
#     job_name = module.silver_archives_declaration_document.name
#   }
#   actions {
#     job_name = module.silver_archives_declaration_relance.name
#   }
#   actions {
#     job_name = module.silver_archives_declaration_reponse.name
#   }
# }

# resource "aws_glue_trigger" "trigger_silver_archives_reponse" {
#   name          = "${terraform.workspace}_trigger_silver_archives_reponse"
#   type          = "CONDITIONAL"
#   workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

#   predicate {
#     conditions {
#       job_name = module.silver_reponse_declaration.name
#       state    = "SUCCEEDED"
#     }
#     conditions {
#       job_name = module.silver_reponse_declaration_data.name
#       state    = "SUCCEEDED"
#     }
#     conditions {
#       job_name = module.silver_reponse_declaration_geom.name
#       state    = "SUCCEEDED"
#     }
#     conditions {
#       job_name = module.silver_reponse_envoi.name
#       state    = "SUCCEEDED"
#     }
#     conditions {
#       job_name = module.silver_reponse_reponse.name
#       state    = "SUCCEEDED"
#     }
#     conditions {
#       job_name = module.silver_reponse_reponse_data.name
#       state    = "SUCCEEDED"
#     }
#   }

#   actions {
#     job_name = module.silver_archives_reponse_declaration.name
#   }
#   actions {
#     job_name = module.silver_archives_reponse_declaration_data.name
#   }
#   actions {
#     job_name = module.silver_archives_reponse_declaration_geom.name
#   }
#   actions {
#     job_name = module.silver_archives_reponse_envoi.name
#   }
#   actions {
#     job_name = module.silver_archives_reponse_reponse.name
#   }
#   actions {
#     job_name = module.silver_archives_reponse_reponse_data.name
#   }
# }

resource "aws_glue_trigger" "trigger_silver_declaration" {
  name          = "${terraform.workspace}_trigger_silver_declaration"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.tech_blank.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.silver_declaration_choix_destinataire.name
  }
  actions {
    job_name = module.silver_declaration_consultation.name
  }
  actions {
    job_name = module.silver_declaration_declaration.name
  }
  actions {
    job_name = module.silver_declaration_declaration_data.name
  }
  actions {
    job_name = module.silver_declaration_declaration_geom.name
  }
  actions {
    job_name = module.silver_declaration_document.name
  }
  actions {
    job_name = module.silver_declaration_parametre_agence.name
  }
  actions {
    job_name = module.silver_declaration_piece_jointe.name
  }
  actions {
    job_name = module.silver_declaration_piece_jointe_reponse.name
  }
  actions {
    job_name = module.silver_declaration_relance.name
  }
  actions {
    job_name = module.silver_declaration_reponse.name
  }
  actions {
    job_name = module.silver_da_dpa.name
  }
}

resource "aws_glue_trigger" "trigger_silver_formulaire" {
  name          = "${terraform.workspace}_trigger_silver_formulaire"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.tech_blank.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.silver_formulaire_formulaire.name
  }
  actions {
    job_name = module.silver_formulaire_modele.name
  }
  actions {
    job_name = module.silver_formulaire_document.name
  }
  actions {
    job_name = module.silver_formulaire_envoi.name
  }
}

resource "aws_glue_trigger" "trigger_silver_jira" {
  name          = "${terraform.workspace}_trigger_silver_jira"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.tech_blank.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.silver_jira_issue.name
  }
  actions {
    job_name = module.silver_jira_project.name
  }
  actions {
    job_name = module.silver_jira_team_member.name
  }
  actions {
    job_name = module.silver_jira_worklog.name
  }
}

resource "aws_glue_trigger" "trigger_silver_reponse" {
  name          = "${terraform.workspace}_trigger_silver_reponse"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.tech_blank.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.silver_reponse_declaration.name
  }
  actions {
    job_name = module.silver_reponse_declaration_data.name
  }
  actions {
    job_name = module.silver_reponse_declaration_geom.name
  }
  actions {
    job_name = module.silver_reponse_envoi.name
  }
  actions {
    job_name = module.silver_reponse_reponse.name
  }
  actions {
    job_name = module.silver_reponse_reponse_data.name
  }
  actions {
    job_name = module.silver_reponse_type_modele.name
  }
}

resource "aws_glue_trigger" "trigger_silver_referentiel" {
  name          = "${terraform.workspace}_trigger_silver_referentiel"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.tech_blank.name
      state    = "SUCCEEDED"
    }
  }

  # AR
  actions {
    job_name = module.silver_ar_contact.name
  }
  actions {
    job_name = module.silver_ar_gu.name
  }

  actions {
    job_name = module.silver_ar_pouvoir.name
  }
  # actions {
  #   job_name = module.silver_ar_historique_pouvoir.name
  # }

  # Auth
  actions {
    job_name = module.silver_auth_app.name
  }
  actions {
    job_name = module.silver_auth_compte.name
  }
  actions {
    job_name = module.silver_auth_compte_notification.name
  }
  actions {
    job_name = module.silver_auth_compte_to_app.name
  }
  actions {
    job_name = module.silver_auth_compte_to_client.name
  }
  actions {
    job_name = module.silver_auth_compte_to_profil.name
  }
  actions {
    job_name = module.silver_auth_droit_app.name
  }
  actions {
    job_name = module.silver_auth_droit_app_to_profil.name
  }
  actions {
    job_name = module.silver_auth_profil.name
  }
  actions {
    job_name = module.silver_log_auth_log_connexion.name
  }

  # Ref
  actions {
    job_name = module.silver_ref.name
  }

  actions {
    job_name = module.silver_ref_iceberg.name
  }
}

# resource "aws_glue_trigger" "trigger_silver_scodify" {
#   name          = "${terraform.workspace}_trigger_silver_scodify"
#   type          = "CONDITIONAL"
#   workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

#   predicate {
#     conditions {
#       job_name = module.tech_blank.name
#       state    = "SUCCEEDED"
#     }
#   }

#   actions {
#     job_name = module.silver_scodify_project.name
#   }
# }

resource "aws_glue_trigger" "trigger_silver_scream" {
  name          = "${terraform.workspace}_trigger_silver_scream"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.tech_blank.name
      state    = "SUCCEEDED"
    }
  }

  # Scream
  actions {
    job_name = module.silver_scream_chapitre.name
  }
  actions {
    job_name = module.silver_scream_condition_paiement.name
  }
  actions {
    job_name = module.silver_scream_client_to_produit.name
  }
  actions {
    job_name = module.silver_scream_client_regle_factu.name
  }
  actions {
    job_name = module.silver_scream_societe_regle_factu.name
  }
  actions {
    job_name = module.silver_scream_conso.name
  }
  actions {
    job_name = module.silver_scream_conso_plan.name
  }
  actions {
    job_name = module.silver_scream_conso_to_produit.name
  }
  actions {
    job_name = module.silver_scream_contact.name
  }
  actions {
    job_name = module.silver_scream_contact_to_entite.name
  }
  actions {
    job_name = module.silver_scream_entite_to_societe.name
  }
  actions {
    job_name = module.silver_scream_entite.name
  }
  actions {
    job_name = module.silver_scream_facturation_terme_echu.name
  }
  actions {
    job_name = module.silver_scream_licence_client.name
  }
  actions {
    job_name = module.silver_scream_offre.name
  }
  actions {
    job_name = module.silver_scream_origine.name
  }
  actions {
    job_name = module.silver_scream_payeur_echeance.name
  }
  actions {
    job_name = module.silver_scream_produit.name
  }
  actions {
    job_name = module.silver_scream_produit_to_produit.name
  }
  actions {
    job_name = module.silver_scream_regle_facturation.name
  }
  actions {
    job_name = module.silver_scream_secteur.name
  }
  actions {
    job_name = module.silver_scream_societe.name
  }
  actions {
    job_name = module.silver_scream_sous_chapitre.name
  }
  actions {
    job_name = module.silver_scream_unite_conso.name
  }

  # Sogetask
  actions {
    job_name = module.silver_sogetask_activite_tache.name
  }
  actions {
    job_name = module.silver_sogetask_affaire.name
  }
  actions {
    job_name = module.silver_sogetask_bon_commande.name
  }
  actions {
    job_name = module.silver_sogetask_bdc_ratio_co.name
  }
  actions {
    job_name = module.silver_sogetask_campagne.name
  }
  actions {
    job_name = module.silver_sogetask_categorie_tache.name
  }
  actions {
    job_name = module.silver_sogetask_commentaire_tache_evenement.name
  }
  actions {
    job_name = module.silver_sogetask_devis.name
  }
  actions {
    job_name = module.silver_sogetask_devise.name
  }
  actions {
    job_name = module.silver_sogetask_echeance.name
  }
  actions {
    job_name = module.silver_sogetask_encaissement.name
  }
  actions {
    job_name = module.silver_sogetask_facture.name
  }
  actions {
    job_name = module.silver_sogetask_file_tache.name
  }
  actions {
    job_name = module.silver_sogetask_motif_affaire.name
  }
  actions {
    job_name = module.silver_sogetask_produit_devis.name
  }
  actions {
    job_name = module.silver_sogetask_produit_facture.name
  }
  actions {
    job_name = module.silver_sogetask_recouvrement.name
  }
  actions {
    job_name = module.silver_sogetask_regle_calcul_envoi.name
  }
  actions {
    job_name = module.silver_sogetask_tache.name
  }
  actions {
    job_name = module.silver_stats_tache_delais_reponse.name
  }
  # actions {
  #   job_name = module.silver_stats_tache_delais_traitement.name
  # }
  actions {
    job_name = module.silver_sogetask_tache_to_compte.name
  }
  actions {
    job_name = module.silver_sogetask_tache_to_contact.name
  }
  actions {
    job_name = module.silver_sogetask_tache_to_detail.name
  }
  actions {
    job_name = module.silver_sogetask_tache_to_devis.name
  }
  actions {
    job_name = module.silver_sogetask_tva_taux.name
  }
  actions {
    job_name = module.silver_sogetask_tva_code.name
  }
}

resource "aws_glue_trigger" "trigger_silver_lucca_leave" {
  name          = "${terraform.workspace}_trigger_silver_lucca_leave"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.tech_blank.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.silver_lucca_leave.name
  }
}

resource "aws_glue_trigger" "trigger_silver_superoffice" {
  name          = "${terraform.workspace}_trigger_silver_superoffice"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.tech_blank.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.silver_superoffice_person.name
  }
  actions {
    job_name = module.silver_superoffice_contact.name
  }

}

resource "aws_glue_trigger" "trigger_silver_tribe" {
  name          = "${terraform.workspace}_trigger_silver_tribe"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.tech_blank.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.silver_tribe_opportunity_line.name
  }
}

resource "aws_glue_trigger" "trigger_silver_stats_vente_couplage" {
  name          = "${terraform.workspace}_trigger_silver_stats_vente_couplage"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_scream_produit.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_facture.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_produit_facture.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.silver_stats_vente_couplage.name
  }
}

resource "aws_glue_trigger" "trigger_silver_stats_vente" {
  name          = "${terraform.workspace}_trigger_silver_stats_vente"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_auth_compte.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_ref.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_chapitre.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_entite_to_societe.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_entite.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_origine.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_produit.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_regle_facturation.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_secteur.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_societe.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_sous_chapitre.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_unite_conso.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_devis.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_produit_facture.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_tache.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_echeance.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_stats_vente_couplage.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.silver_stats_vente.name
  }
}

resource "aws_glue_trigger" "trigger_silver_stats_funnel" {
  name          = "${terraform.workspace}_trigger_silver_stats_funnel"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_netsuite_invoice.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_ref.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_produit_to_produit.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_produit.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_origine.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_societe.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_secteur.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_devis.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_produit_devis.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_affaire.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_campagne.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_motif_affaire.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_tache.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_activite_tache.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_devise.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_bon_commande.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_facture.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_produit_facture.name
      state    = "SUCCEEDED"
    }
    # conditions {
    #   job_name = module.bronze_visma_salesorder.name
    #   state    = "SUCCEEDED"
    # }
    # conditions {
    #   job_name = module.bronze_webkua.name
    #   state    = "SUCCEEDED"
    # }
    # conditions {
    #   job_name = module.bronze_superoffice.name
    #   state    = "SUCCEEDED"
    # }
  }

  actions {
    job_name = module.silver_stats_funnel.name
  }
}

resource "aws_glue_trigger" "trigger_silver_stats_funnel_salesforce" {
  name          = "${terraform.workspace}_trigger_silver_stats_funnel_salesforce"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_netsuite_invoice.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_ref.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_produit_to_produit.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_produit.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_origine.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_societe.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_secteur.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_devis.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_produit_devis.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_affaire.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_campagne.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_motif_affaire.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_tache.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_activite_tache.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_devise.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_bon_commande.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_facture.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_produit_facture.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_account.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_campaign.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_opportunity.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_opportunity_line_item.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_opportunity_split.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_opportunity_team_member.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_product.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_quote.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_quote_line_item.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_referential_information.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.silver_stats_funnel_salesforce.name
  }
}

resource "aws_glue_trigger" "trigger_silver_visma" {
  name          = "${terraform.workspace}_trigger_silver_visma"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.tech_blank.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.silver_visma_customerinvoice.name
  }
  actions {
    job_name = module.silver_visma_salesorder.name
  }

}

resource "aws_glue_trigger" "trigger_gold_devis" {
  name          = "${terraform.workspace}_trigger_gold_devis"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_auth_compte.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_ref.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_entite_to_societe.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_secteur.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_societe.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_affaire.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_devis.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_produit_devis.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_tache.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_devis.name
  }
}

resource "aws_glue_trigger" "trigger_silver_stats_factu_sglk" {
  name          = "${terraform.workspace}_trigger_silver_stats_factu_sglk"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_stats_vente.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.silver_stats_factu_sglk.name
  }
}

resource "aws_glue_trigger" "trigger_silver_netsuite" {
  name          = "${terraform.workspace}_trigger_silver_netsuite"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.tech_blank.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.silver_netsuite_charge.name
  }
  actions {
    job_name = module.silver_netsuite_item.name
  }
  actions {
    job_name = module.silver_netsuite_invoice.name
  }
  actions {
    job_name = module.silver_netsuite_revenue.name
  }
  actions {
    job_name = module.silver_netsuite_sales_order.name
  }
}

resource "aws_glue_trigger" "trigger_silver_statistic" {
  name          = "${terraform.workspace}_trigger_silver_statistic"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.tech_blank.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.silver_statistic_etat_stock.name
  }
  actions {
    job_name = module.silver_statistic_conso_agence.name
  }
  actions {
    job_name = module.silver_statistic_conso_emetteur.name
  }
  actions {
    job_name = module.silver_statistic_conso_utilisateur.name
  }
  actions {
    job_name = module.silver_statistic_nb_envoi_jour.name
  }
  actions {
    job_name = module.silver_statistic_stocks_client.name
  }
}

## Stats billing
resource "aws_glue_trigger" "trigger_silver_stats_billing" {
  name          = "${terraform.workspace}_trigger_silver_stat_billing"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.tech_blank.name
      state    = "SUCCEEDED"
    }
  }
  actions {
    job_name = module.silver_stats_billing_fr.name
  }
  actions {
    job_name = module.silver_stats_billing_nl.name
  }
  actions {
    job_name = module.silver_stats_billing_no.name
  }
}

## Stats Consumption
# resource "aws_glue_trigger" "trigger_silver_stats_consumption_fr" {
#   name          = "${terraform.workspace}_trigger_silver_stats_consumption_fr"
#   type          = "CONDITIONAL"
#   workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

#   predicate {
#     conditions {
#       job_name = module.silver_scream_client_to_produit.name
#       state    = "SUCCEEDED"
#     }
#     conditions {
#       job_name = module.silver_scream_unite_conso.name
#       state    = "SUCCEEDED"
#     }
#     conditions {
#       job_name = module.silver_ref.name
#       state    = "SUCCEEDED"
#     }
#     conditions {
#       job_name = module.silver_sogetask_produit_facture.name
#       state    = "SUCCEEDED"
#     }
#     conditions {
#       job_name = module.silver_sogetask_produit_devis.name
#       state    = "SUCCEEDED"
#     }
#   }
#   actions {
#     job_name = module.silver_stats_consumption_fr.name
#   }
# }

# resource "aws_glue_trigger" "trigger_silver_stats_consumption_nl" {
#   name          = "${terraform.workspace}_trigger_silver_stats_consumption_nl"
#   type          = "CONDITIONAL"
#   workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

#   predicate {
#     conditions {
#       job_name = module.tech_blank.name
#       state    = "SUCCEEDED"
#     }
#   }
#   actions {
#     job_name = module.silver_stats_consumption_nl.name
#   }
# }

## Stats opportunity
resource "aws_glue_trigger" "trigger_silver_stats_opportunity_fr" {
  name          = "${terraform.workspace}_trigger_silver_stats_opportunity_fr"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_sogetask_affaire.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_bon_commande.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_produit_devis.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_produit.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_tache.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_societe.name
      state    = "SUCCEEDED"
    }
  }
  actions {
    job_name = module.silver_stats_opportunity_fr.name
  }
}

# resource "aws_glue_trigger" "trigger_silver_stats_opportunity_nl" {
#   name          = "${terraform.workspace}_trigger_silver_stats_opportunity_nl"
#   type          = "CONDITIONAL"
#   workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name
#   predicate {
#     conditions {
#       job_name = module.tech_blank.name
#       state    = "SUCCEEDED"
#     }
#   }
#   actions {
#     job_name = module.silver_stats_opportunity_nl.name
#   }
# }

resource "aws_glue_trigger" "trigger_silver_stats_opportunity_no" {
  name          = "${terraform.workspace}_trigger_silver_stats_opportunity_no"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name
  predicate {
    conditions {
      job_name = module.tech_blank.name
      state    = "SUCCEEDED"
    }
  }
  actions {
    job_name = module.silver_stats_opportunity_no.name
  }
}

## Salesforce
resource "aws_glue_trigger" "trigger_silver_salesforce_1" {
  name          = "${terraform.workspace}_trigger_silver_salesforce_1"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name
  predicate {
    conditions {
      job_name = module.tech_blank.name
      state    = "SUCCEEDED"
    }
  }
  actions {
    job_name = module.silver_salesforce_account.name
  }
  actions {
    job_name = module.silver_salesforce_campaign.name
  }
  actions {
    job_name = module.silver_salesforce_opportunity.name
  }
  actions {
    job_name = module.silver_salesforce_opportunity_line_item.name
  }
}

resource "aws_glue_trigger" "trigger_silver_salesforce_2" {
  name          = "${terraform.workspace}_trigger_silver_salesforce_2"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name
  predicate {
    conditions {
      job_name = module.silver_salesforce_account.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_campaign.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_opportunity.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_opportunity_line_item.name
      state    = "SUCCEEDED"
    }
  }
  actions {
    job_name = module.silver_salesforce_opportunity_split.name
  }
  actions {
    job_name = module.silver_salesforce_opportunity_team_member.name
  }
  actions {
    job_name = module.silver_salesforce_product.name
  }
}

resource "aws_glue_trigger" "trigger_silver_salesforce_3" {
  name          = "${terraform.workspace}_trigger_silver_salesforce_3"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name
  predicate {
    conditions {
      job_name = module.silver_salesforce_opportunity_split.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_opportunity_team_member.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_product.name
      state    = "SUCCEEDED"
    }
  }
  actions {
    job_name = module.silver_salesforce_quote.name
  }
  actions {
    job_name = module.silver_salesforce_quote_line_item.name
  }
  actions {
    job_name = module.silver_salesforce_referential_information.name
  }
}

resource "aws_glue_trigger" "trigger_silver_stats_opportunity_salesforce" {
  name          = "${terraform.workspace}_trigger_silver_stats_opportunity_salesforce"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name
  predicate {
    conditions {
      job_name = module.silver_salesforce_opportunity.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_opportunity_line_item.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_account.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_opportunity_split.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_campaign.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_quote.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_quote_line_item.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_product.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_salesforce_referential_information.name
      state    = "SUCCEEDED"
    }
  }
  actions {
    job_name = module.silver_stats_opportunity_salesforce.name
  }
}

## Access_app
resource "aws_glue_trigger" "trigger_gold_access_app" {
  name          = "${terraform.workspace}_trigger_gold_access_app"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_auth_compte.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_auth_compte_to_profil.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_auth_profil.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_auth_app.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_access_app.name
  }
}

## Ar_contact
resource "aws_glue_trigger" "trigger_gold_ar_contact" {
  name          = "${terraform.workspace}_trigger_gold_ar_contact"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_ar_contact.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_ar_gu.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_ref.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_ar_contact.name
  }
}


## Billing
resource "aws_glue_trigger" "trigger_gold_billing" {
  name          = "${terraform.workspace}_trigger_gold_billing"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_stats_billing_fr.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_stats_billing_nl.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_stats_billing_no.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_billing.name
  }
}

## Consumption
resource "aws_glue_trigger" "trigger_gold_consumption" {
  name          = "${terraform.workspace}_trigger_gold_consumption"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_stats_consumption_fr.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_stats_consumption_nl.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_consumption.name
  }
}

## Pipe
resource "aws_glue_trigger" "trigger_gold_funnel" {
  name          = "trigger_${terraform.workspace}_gold_funnel"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_stats_funnel.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_funnel.name
  }
}

# Pipe Salesforce
resource "aws_glue_trigger" "trigger_gold_funnel_salesforce" {
  name          = "trigger_${terraform.workspace}_gold_funnel_salesforce"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_stats_funnel_salesforce.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_funnel_salesforce.name
  }
}

## Societe entite
resource "aws_glue_trigger" "trigger_gold_societe_entite" {
  name          = "${terraform.workspace}_trigger_gold_societe_entite"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_scream_societe.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_secteur.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_regle_facturation.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_entite_to_societe.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_client_regle_factu.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_ref.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_auth_compte.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_ar_contact.name
      state    = "SUCCEEDED"
    }

    conditions {
      job_name = module.silver_superoffice_contact.name
      state    = "SUCCEEDED"
    }

  }

  actions {
    job_name = module.gold_societe_entite.name
  }
}

## Opportunity
resource "aws_glue_trigger" "trigger_gold_opportunity" {
  name          = "${terraform.workspace}_trigger_gold_opportunity"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_stats_opportunity_fr.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_stats_opportunity_salesforce.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_stats_opportunity_no.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_opportunity.name
  }
}
## Organization
resource "aws_glue_trigger" "trigger_gold_organization" {
  name          = "${terraform.workspace}_trigger_gold_organization"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_scream_entite_to_societe.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_societe.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_ref.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_organization.name
  }
}

## Renew
resource "aws_glue_trigger" "trigger_gold_renew_conso" {
  name          = "${terraform.workspace}_trigger_gold_renew_conso"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_scream_client_to_produit.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_payeur_echeance.name
      state    = "SUCCEEDED"
    }

    conditions {
      job_name = module.silver_scream_unite_conso.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_facturation_terme_echu.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_facture.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_produit_facture.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_renew_conso.name
  }
}

resource "aws_glue_trigger" "trigger_gold_renew_licence" {
  name          = "${terraform.workspace}_trigger_gold_renew_licence"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_scream_licence_client.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_entite_to_societe.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_devis.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_affaire.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_produit_devis.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_renew_licence.name
  }
}

## Revenue
resource "aws_glue_trigger" "trigger_gold_revenue" {
  name          = "${terraform.workspace}_trigger_gold_revenue"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_netsuite_revenue.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_revenue.name
  }
}

## Sending

resource "aws_glue_trigger" "trigger_gold_sending" {
  name          = "${terraform.workspace}_trigger_gold_sending"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_scream_conso.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_auth_compte.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_ref.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_sending.name
  }
}

## Sending agg

resource "aws_glue_trigger" "trigger_gold_sending_agg" {
  name          = "${terraform.workspace}_trigger_gold_sending_agg"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.gold_sending.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_sending_agg.name
  }
}

## Sending_to_produit

resource "aws_glue_trigger" "trigger_gold_sending_to_produit" {
  name          = "${terraform.workspace}_trigger_gold_sending_to_produit"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_scream_conso_to_produit.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_auth_compte.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_ref.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_sending_to_produit.name
  }
}

## Support
resource "aws_glue_trigger" "trigger_gold_support" {
  name          = "${terraform.workspace}_trigger_gold_support"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_sogetask_tache.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_stats_tache_delais_reponse.name
      state    = "SUCCEEDED"
    }
    # conditions {
    #  job_name = module.silver_stats_tache_delais_traitement.name
    #  state    = "SUCCEEDED"
    # }
    conditions {
      job_name = module.silver_sogetask_tache_to_contact.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_support.name
  }
}


## Connection
resource "aws_glue_trigger" "trigger_gold_connection" {
  name          = "${terraform.workspace}_trigger_gold_connection"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_auth_compte_to_app.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_auth_app.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_auth_compte_to_client.name
      state    = "SUCCEEDED"
    }

  }

  actions {
    job_name = module.gold_connection.name
  }
}


## Enquiry
resource "aws_glue_trigger" "trigger_gold_enquiry" {
  name          = "${terraform.workspace}_trigger_gold_enquiry"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    # job_name = module.silver_sogetask_tache_to_tache N'EXISTE PAS (ENCORE)
    conditions {
      job_name = module.silver_scream_origine.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_produit.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_societe.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_affaire.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_campagne.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_devis.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_motif_affaire.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_produit_devis.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_tache.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_stats_tache_delais_reponse.name
      state    = "SUCCEEDED"
    }
    # conditions {
    #  job_name = module.silver_stats_tache_delais_traitement.name
    #  state    = "SUCCEEDED"
    # }
  }

  actions {
    job_name = module.gold_enquiry.name
  }
}

## Produit

resource "aws_glue_trigger" "trigger_gold_produit" {
  name          = "${terraform.workspace}_trigger_gold_produit"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_scream_produit.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_netsuite_item.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_unite_conso.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_produit.name
  }
}

## Event

resource "aws_glue_trigger" "trigger_gold_event" {
  name          = "${terraform.workspace}_trigger_gold_event"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_sogetask_commentaire_tache_evenement.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_tache.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_entite_to_societe.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_societe.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_event.name
  }
}

## Compte interne

resource "aws_glue_trigger" "trigger_gold_employee" {
  name          = "${terraform.workspace}_trigger_gold_employee"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_auth_compte.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_auth_compte_to_client.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_superoffice_person.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_employee.name
  }
}

## Leave

resource "aws_glue_trigger" "trigger_gold_leave" {
  name          = "${terraform.workspace}_trigger_gold_leave"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_lucca_leave.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_auth_compte.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_jira_issue.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_leave.name
  }
}

## Agence

resource "aws_glue_trigger" "trigger_gold_agence" {
  name          = "${terraform.workspace}_trigger_gold_agence"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_ref.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_declaration_parametre_agence.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_agence.name
  }
}

## Client

resource "aws_glue_trigger" "trigger_gold_client" {
  name          = "${terraform.workspace}_trigger_gold_client"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_ref.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_auth_compte.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_societe.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_entite_to_societe.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_secteur.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_superoffice_contact.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_client.name
  }
}

## Client_to_produit

resource "aws_glue_trigger" "trigger_gold_client_to_produit" {
  name          = "${terraform.workspace}_trigger_gold_client_to_produit"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_scream_client_to_produit.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_produit.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_unite_conso.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_produit_facture.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_produit_devis.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_client_to_produit.name
  }
}

## Account

resource "aws_glue_trigger" "trigger_gold_account" {
  name          = "${terraform.workspace}_trigger_gold_account"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_auth_compte.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_auth_compte_to_client.name
      state    = "SUCCEEDED"
    }

  }

  actions {
    job_name = module.gold_account.name
  }
  actions {
    job_name = module.gold_account_action.name
  }
}

## Invoice

resource "aws_glue_trigger" "trigger_gold_invoice" {
  name          = "${terraform.workspace}_trigger_gold_invoice"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_stats_factu_sglk.name
      state    = "SUCCEEDED"
    }

    conditions {
      job_name = module.silver_netsuite_invoice.name
      state    = "SUCCEEDED"
    }

    conditions {
      job_name = module.silver_scream_societe.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_invoice.name
  }
}

## Remaining

resource "aws_glue_trigger" "trigger_gold_remaining" {
  name          = "trigger_${terraform.workspace}_gold_remaining"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_netsuite_sales_order.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_netsuite_invoice.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_netsuite_charge.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_remaining.name
  }
}

## DMC

resource "aws_glue_trigger" "trigger_gold_dmc" {
  name          = "${terraform.workspace}_trigger_gold_dmc"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_ref.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_dmc.name
  }
}

## Bon_commande

resource "aws_glue_trigger" "trigger_gold_bon_commande" {
  name          = "trigger_${terraform.workspace}_gold_bon_commande"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_sogetask_bon_commande.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_devis.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_affaire.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_tache.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_societe.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_ref.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_bon_commande.name
  }
}

## Groupe client

resource "aws_glue_trigger" "trigger_gold_groupe_client" {
  name          = "trigger_${terraform.workspace}_gold_groupe_client"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_ref.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_groupe_client.name
  }
}

## Jira

resource "aws_glue_trigger" "trigger_gold_jira" {
  name          = "${terraform.workspace}_trigger_gold_jira"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_jira_issue.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_jira_project.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_jira_worklog.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_jira_team_member.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_issue.name
  }
  actions {
    job_name = module.gold_project.name
  }
  actions {
    job_name = module.gold_team_member.name
  }
  actions {
    job_name = module.gold_issue_worklog.name
  }
}

## PMSR
resource "aws_glue_trigger" "trigger_gold_pmsr" {
  name          = "trigger_${terraform.workspace}_gold_pmsr"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_sogetask_devis.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_pmsr.name
  }
}


## DA-DPA
resource "aws_glue_trigger" "trigger_gold_da_dpa" {
  name          = "trigger_${terraform.workspace}_gold_da_dpa"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_da_dpa.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_da_dpa.name
  }
}

## Sales_activity

resource "aws_glue_trigger" "trigger_gold_sales_activity" {
  name          = "trigger_${terraform.workspace}_gold_sales_activity"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_sogetask_tache.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_entite_to_societe.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_secteur.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_societe.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_devis.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_affaire.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_campagne.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_sogetask_motif_affaire.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_origine.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_sales_activity.name
  }
}

resource "aws_glue_trigger" "trigger_gold_cbyd" {
  name          = "${terraform.workspace}_trigger_gold_cbyd"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    // Déclaration
    conditions {
      job_name = module.silver_declaration_choix_destinataire.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_declaration_consultation.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_declaration_declaration.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_declaration_declaration_data.name
      state    = "SUCCEEDED"
    }
    # conditions {
    #   job_name = module.silver_declaration_declaration_geom.name
    #   state    = "SUCCEEDED"
    # }
    conditions {
      job_name = module.silver_declaration_document.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_declaration_parametre_agence.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_declaration_relance.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_declaration_reponse.name
      state    = "SUCCEEDED"
    }

    // Réponse
    conditions {
      job_name = module.silver_reponse_declaration.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_reponse_declaration_data.name
      state    = "SUCCEEDED"
    }
    # conditions {
    #   job_name = module.silver_reponse_declaration_geom.name
    #   state    = "SUCCEEDED"
    # }
    conditions {
      job_name = module.silver_reponse_envoi.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_reponse_reponse.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_reponse_reponse_data.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_reponse_type_modele.name
      state    = "SUCCEEDED"
    }

    // Archives Déclaration
    # conditions {
    #   job_name = module.silver_archives_declaration_choix_destinataire.name
    #   state    = "SUCCEEDED"
    # }
    # conditions {
    #   job_name = module.silver_archives_declaration_consultation.name
    #   state    = "SUCCEEDED"
    # }
    # conditions {
    #   job_name = module.silver_archives_declaration_declaration.name
    #   state    = "SUCCEEDED"
    # }
    # conditions {
    #   job_name = module.silver_archives_declaration_declaration_data.name
    #   state    = "SUCCEEDED"
    # }
    # conditions {
    #   job_name = module.silver_archives_declaration_declaration_geom.name
    #   state    = "SUCCEEDED"
    # }
    # conditions {
    #   job_name = module.silver_archives_declaration_document.name
    #   state    = "SUCCEEDED"
    # }
    # conditions {
    #   job_name = module.silver_archives_declaration_relance.name
    #   state    = "SUCCEEDED"
    # }
    # conditions {
    #   job_name = module.silver_archives_declaration_reponse.name
    #   state    = "SUCCEEDED"
    # }

    // Archives Réponse
    # conditions {
    #   job_name = module.silver_archives_reponse_declaration.name
    #   state    = "SUCCEEDED"
    # }
    # conditions {
    #   job_name = module.silver_archives_reponse_declaration_data.name
    #   state    = "SUCCEEDED"
    # }
    # conditions {
    #   job_name = module.silver_archives_reponse_declaration_geom.name
    #   state    = "SUCCEEDED"
    # }
    # conditions {
    #   job_name = module.silver_archives_reponse_envoi.name
    #   state    = "SUCCEEDED"
    # }
    # conditions {
    #   job_name = module.silver_archives_reponse_reponse.name
    #   state    = "SUCCEEDED"
    # }
    # conditions {
    #   job_name = module.silver_archives_reponse_reponse_data.name
    #   state    = "SUCCEEDED"
    # }
  }

  actions {
    job_name = module.gold_declarant.name
  }
  actions {
    job_name = module.gold_exploitant.name
  }
}

## Contact
resource "aws_glue_trigger" "trigger_gold_contact" {
  name          = "trigger_${terraform.workspace}_gold_contact"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_scream_contact.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_contact.name
  }
}

## Stock
# resource "aws_glue_trigger" "trigger_gold_stock" {
#   name          = "trigger_${terraform.workspace}_gold_stock"
#   type          = "CONDITIONAL"
#   workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

#   predicate {
#     conditions {
#       job_name = module.silver_ref.name
#       state    = "SUCCEEDED"
#     }
#     conditions {
#       job_name = module.silver_scream_client_to_produit.name
#       state    = "SUCCEEDED"
#     }
#     conditions {
#       job_name = module.silver_scream_produit.name
#       state    = "SUCCEEDED"
#     }
#   }

#   actions {
#     job_name = module.gold_stock.name
#   }
# }

## Etat stock
resource "aws_glue_trigger" "trigger_gold_etat_stock" {
  name          = "${terraform.workspace}_trigger_gold_etat_stock"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_scream_client_to_produit.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_unite_conso.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_scream_produit.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_etat_stock.name
  }
}


## Deal
resource "aws_glue_trigger" "trigger_gold_deal" {
  name          = "${terraform.workspace}_trigger_gold_deal"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_stats_funnel.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_netsuite_item.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.gold_revenue.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_deal.name
  }
}

## Log
resource "aws_glue_trigger" "trigger_gold_log" {
  name          = "${terraform.workspace}_trigger_gold_log"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_log_auth_log_connexion.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_log.name
  }
}

## Sending_mobile
resource "aws_glue_trigger" "trigger_gold_sending_mobile" {
  name          = "${terraform.workspace}_trigger_gold_sending_mobile"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.gold_declarant.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_formulaire_formulaire.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_formulaire_modele.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_formulaire_document.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_formulaire_envoi.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.silver_ref.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_sending_mobile.name
  }
}

## Switcher
resource "aws_glue_trigger" "trigger_gold_switcher" {
  name          = "${terraform.workspace}_trigger_gold_switcher"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.gold_declarant.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = module.gold_exploitant.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_switcher.name
  }
}

## tache_to_detail
resource "aws_glue_trigger" "trigger_gold_tache_to_detail" {
  name          = "${terraform.workspace}_trigger_gold_tache_to_detail"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.silver_sogetask_tache_to_detail.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_tache_to_detail.name
  }
}

## tache_to_detail
resource "aws_glue_trigger" "trigger_gold_flydoc" {
  name          = "${terraform.workspace}_trigger_gold_flydoc"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.tech_blank.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_flydoc.name
  }
}

## transaction
resource "aws_glue_trigger" "trigger_gold_transaction" {
  name          = "${terraform.workspace}_trigger_gold_transaction"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.snowlake_workflow_silver_gold_jobs.name

  predicate {
    conditions {
      job_name = module.gold_sending_to_produit.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = module.gold_transaction.name
  }
}

# Workflow Tech

resource "aws_glue_workflow" "snowlake_workflow_tech_jobs" {
  name = "${terraform.workspace}_snowlake_workflow_tech_jobs"
}

resource "aws_glue_trigger" "start_workflow_tech_jobs" {
  name          = "${terraform.workspace}_start_workflow_tech_jobs"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.snowlake_workflow_tech_jobs.name

  actions {
    job_name = module.catalog.name
  }
}


# Workflow Deactivate
resource "aws_glue_workflow" "snowlake_workflow_tech_deactivate_jobs" {
  name = "${terraform.workspace}_snowlake_workflow_tech_deactivate_jobs"
}

resource "aws_glue_trigger" "trigger_deactivate_afas_1" {
  name          = "${terraform.workspace}_trigger_deactivate_afas_1"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.snowlake_workflow_tech_deactivate_jobs.name

  actions {
    job_name = module.tech_deactivate_afas_generalledgerdata.name
  }
}

resource "aws_glue_trigger" "trigger_deactivate_netsuite_1" {
  name          = "${terraform.workspace}_trigger_deactivate_netsuite_1"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.snowlake_workflow_tech_deactivate_jobs.name

  actions {
    job_name = module.tech_deactivate_netsuite_transaction.name
  }

  actions {
    job_name = module.tech_deactivate_netsuite_transactionaccountingline.name
  }

  actions {
    job_name = module.tech_deactivate_netsuite_transactionline.name
  }
}

resource "aws_glue_trigger" "trigger_deactivate_visma_1" {
  name          = "${terraform.workspace}_trigger_deactivate_visma_1"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.snowlake_workflow_tech_deactivate_jobs.name

  actions {
    job_name = module.tech_deactivate_visma_generalledgertransactions.name
  }
}
