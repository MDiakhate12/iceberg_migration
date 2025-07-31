# -*- coding: utf-8 -*-
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    BooleanType,
    TimestampType,
    LongType,
)
TRIBE_SALE_COLUMNS = [
    'EntityName',
    'PrimaryKey',
    'activeErpLinks',
    'amount',
    'amountInBaseCurrency',
    'amountWeighted',
    'amountWeightedInBaseCurrency',
    'associateId',
    'competitor',
    'completed',
    'contactId',
    'createdByWorkflow',
    'credited',
    'currency',
    'currencyId',
    'date',
    'description',
    'earning',
    'earningPercent',
    'hasGuide',
    'hasQuote',
    'hasStakeholders',
    'heading',
    'icon',
    'lossReason',
    'nextDueDate',
    'originalStage',
    'personId',
    'probPercent',
    'projectId',
    'registeredBy',
    'registeredByFullName',
    'registeredDate',
    'reopenDate',
    'saleId',
    'saleNumber',
    'saleStatus',
    'saleType',
    'saleTypeCategory',
    'soldReason',
    'source',
    'stage',
    'stageRank',
    'stalledComment',
    'text',
    'time',
    'type',
    'updatedBy',
    'updatedByFullName',
    'updatedDate',
    'userGroup',
    'visibleFor',
    'who',

    'person/personId',
    'person/firstName',
    'person/lastName',
    'person/middleName',
    'person/fullName',
    'person/contactId',
    'person/retired',

    'person/personAssociate/personId',
    'person/personAssociate/associateDbId',
    'person/personAssociate/contactName',
    'person/personAssociate/contactDepartment',
    'person/personAssociate/usergroup',
    'person/personAssociate/contactFullName',
    'person/personAssociate/isActive',


    'contact/contactId',
    'contact/name',
    'contact/department',
    'contact/nameDepartment',

    'associate/personId',
    'associate/associateDbId',
    'associate/contactId',
    'associate/contactName',
    'associate/contactDepartment',
    'associate/usergroup',
    'associate/contactFullName',
    'associate/isActive',
    'associate/fisrtName',
    'associate/lastName',
    'associate/middleName',
    'associate/fullName',

    'saleUdef/SuperOffice:1',
]

TRIBE_PERSON_COLUMNS = [
    'EntityName',
    'PrimaryKey',
    'associateType',
    'birthDay',
    'birthMonth',
    'birthYear',
    'birthdate',
    'consentSourceEmarketing',
    'consentSourceStore',
    'contactId',
    'createdByForm',
    'firstName',
    'fullName',
    'fullNameWithContact',
    'hasCompany',
    'hasEmarketingConsent',
    'hasInfoText',
    'hasInterests',
    'hasStoreConsent',
    'isMailingRecipient',
    'kanaFirstName',
    'kanaLastName',
    'lastName',
    'legalBaseEmarketing',
    'legalBaseStore',
    'middleName',
    'mrMrs',
    'personActiveErpLinks',
    'personAssociateFullName',
    'personAssociateId',
    'personBusiness',
    'personCategory',
    'personCountry',
    'personCountryId',
    'personDeletedDate',
    'personHasInterests',
    'personId',
    'personInterestIds',
    'personNoMail',
    'personNumber',
    'personRegisteredBy',
    'personRegisteredByFullName',
    'personRegisteredDate',
    'personSource',
    'personUpdatedBy',
    'personUpdatedByFullName',
    'personUpdatedDate',
    'portraitThumbnail',
    'position',
    'rank',
    'retired',
    'subscription',
    'supportAssociate',
    'supportAssociateFullName',
    'supportLanguage',
    'ticketPriority',
    'title',
    'updatedByWorkflow',
    'useAsMailingAddress',
    'whenUpdatedByWorkflow',
    'withdrawnEmarketingConsent',
    'withdrawnStoreConsent',

    'correspondingAssociate/firstName',
    'correspondingAssociate/lastName',
    'correspondingAssociate/middleName',
    'correspondingAssociate/fullName',
    'correspondingAssociate/contactId',
    'correspondingAssociate/personId',
    'correspondingAssociate/mrMrs',
    'correspondingAssociate/title',
    'correspondingAssociate/associateDbId',
    'correspondingAssociate/contactName',
    'correspondingAssociate/contactDepartment',
    'correspondingAssociate/usergroup',
    'correspondingAssociate/contactFullName',
    'correspondingAssociate/contactCategory',
    'correspondingAssociate/role',
    'correspondingAssociate/isActive',
]


# Links will always be returned so no need to select it (it will raise a Bad Request)
NETSUITE_ACCOUNT_QUERY = """SELECT
        COALESCE(TO_CHAR(acc.accountsearchdisplayname), ' ') AS accountsearchdisplayname
        , COALESCE(TO_CHAR(acc.accountsearchdisplaynamecopy), ' ') AS accountsearchdisplaynamecopy
        , COALESCE(TO_CHAR(acc.acctnumber), ' ') AS acctnumber
        , COALESCE(TO_CHAR(acc.accttype), ' ') AS accttype
        , COALESCE(TO_CHAR(acc.availablebalance), ' ') AS availablebalance
        , COALESCE(TO_CHAR(acc.cashflowrate), ' ') AS cashflowrate
        , COALESCE(TO_CHAR(acc.currency), ' ') AS currency
        , COALESCE(TO_CHAR(acc.custrecord_acct_type_pbi), ' ') AS custrecord_acct_type_pbi
        , COALESCE(TO_CHAR(acc.custrecord_fam_account_showinfixedasset), ' ') AS custrecord_fam_account_showinfixedasset
        , COALESCE(TO_CHAR(acc.custrecord_ff_sc_acc_srvc_for_taxation), ' ') AS custrecord_ff_sc_acc_srvc_for_taxation
        , COALESCE(TO_CHAR(acc.custrecord_glm_include), ' ') AS custrecord_glm_include
        , COALESCE(TO_CHAR(acc.custrecord_nov_das2nature), ' ') AS custrecord_nov_das2nature
        , COALESCE(TO_CHAR(acc.custrecord_ste_taxaccount_stecode), ' ') AS custrecord_ste_taxaccount_stecode
        , COALESCE(TO_CHAR(acc.deferralacct), ' ') AS deferralacct
        , COALESCE(TO_CHAR(acc.description), ' ') AS description
        , COALESCE(TO_CHAR(acc.displaynamewithhierarchy), ' ') AS displaynamewithhierarchy
        , COALESCE(TO_CHAR(acc.eliminate), ' ') AS eliminate
        , COALESCE(TO_CHAR(acc.externalid), ' ') AS externalid
        , COALESCE(TO_CHAR(acc.fullname), ' ') AS fullname
        , COALESCE(TO_CHAR(acc.generalrate), ' ') AS generalrate
        , COALESCE(TO_CHAR(acc.id), ' ') AS id
        , COALESCE(TO_CHAR(acc.includechildren), ' ') AS includechildren
        , COALESCE(TO_CHAR(acc.inventory), ' ') AS inventory
        , COALESCE(TO_CHAR(acc.isinactive), ' ') AS isinactive
        , COALESCE(TO_CHAR(acc.issummary), ' ') AS issummary
        , COALESCE(TO_CHAR(acc.lastmodifieddate), ' ') AS lastmodifieddate
        --, COALESCE(TO_CHAR(acc.links), ' ') AS links
        , COALESCE(TO_CHAR(acc.location), ' ') AS location
        , COALESCE(TO_CHAR(acc.reconcilewithmatching), ' ') AS reconcilewithmatching
        , COALESCE(TO_CHAR(acc.revalue), ' ') AS revalue
        , COALESCE(TO_CHAR(acc.sspecacct), ' ') AS sspecacct
        , COALESCE(TO_CHAR(acc.subsidiary), ' ') AS subsidiary
    FROM account acc
"""

# NETSUITE_ACCOUNT_QUERY ="""
#     SELECT *
#     FROM account acc
# """

NETSUITE_ACCOUNTINGPERIOD_SQL_QUERY = """SELECT
        COALESCE(TO_CHAR(accp.alllocked), ' ') AS alllocked
        , COALESCE(TO_CHAR(accp.allownonglchanges), ' ') AS allownonglchanges
        , COALESCE(TO_CHAR(accp.aplocked), ' ') AS aplocked
        , COALESCE(TO_CHAR(accp.arlocked), ' ') AS arlocked
        , COALESCE(TO_CHAR(accp.closed), ' ') AS closed
        , COALESCE(TO_CHAR(accp.closedondate), ' ') AS closedondate
        , COALESCE(TO_CHAR(accp.enddate), ' ') AS enddate
        , COALESCE(TO_CHAR(accp.id), ' ') AS id
        , COALESCE(TO_CHAR(accp.isadjust), ' ') AS isadjust
        , COALESCE(TO_CHAR(accp.isinactive), ' ') AS isinactive
        , COALESCE(TO_CHAR(accp.isposting), ' ') AS isposting
        , COALESCE(TO_CHAR(accp.isquarter), ' ') AS isquarter
        , COALESCE(TO_CHAR(accp.isyear), ' ') AS isyear
        , COALESCE(TO_CHAR(accp.lastmodifieddate), ' ') AS lastmodifieddate
        , COALESCE(TO_CHAR(accp.periodname), ' ') AS periodname
        , COALESCE(TO_CHAR(accp.startdate), ' ') AS startdate
    FROM accountingperiod accp"""

NETSUITE_CHARGE_SQL_QUERY = """SELECT
        COALESCE(TO_CHAR(cha.amount), ' ') AS amount
        , COALESCE(TO_CHAR(cha.billdate), ' ') AS billdate
        , COALESCE(TO_CHAR(cha.billingaccount), ' ') AS billingaccount
        , COALESCE(TO_CHAR(cha.billingitem), ' ') AS billingitem
        , COALESCE(TO_CHAR(cha.billingmode), ' ') AS billingmode
        , COALESCE(TO_CHAR(cha.billingschedule), ' ') AS billingschedule
        , COALESCE(TO_CHAR(cha.billto), ' ') AS billto
        , COALESCE(TO_CHAR(cha.chargedate), ' ') AS chargedate
        , COALESCE(TO_CHAR(cha.chargetype), ' ') AS chargetype
        , COALESCE(TO_CHAR(cha.createddate), ' ') AS createddate
        , COALESCE(TO_CHAR(cha.creditmemo), ' ') AS creditmemo
        , COALESCE(TO_CHAR(cha.currency), ' ') AS currency
        , COALESCE(TO_CHAR(cha.description), ' ') AS description
        , COALESCE(TO_CHAR(cha.grouporder), ' ') AS grouporder
        , COALESCE(TO_CHAR(cha.id), ' ') AS id
        , COALESCE(TO_CHAR(cha.invoice), ' ') AS invoice
        , COALESCE(TO_CHAR(cha.invoiceline), ' ') AS invoiceline
        , COALESCE(TO_CHAR(cha.invoiceln), ' ') AS invoiceln
        , COALESCE(TO_CHAR(cha.lastmodifieddate), ' ') AS lastmodifieddate
        , COALESCE(TO_CHAR(cha.quantity), ' ') AS quantity
        , COALESCE(TO_CHAR(cha.rate), ' ') AS rate
        , COALESCE(TO_CHAR(cha.runid), ' ') AS runid
        , COALESCE(TO_CHAR(cha.serviceenddate), ' ') AS serviceenddate
        , COALESCE(TO_CHAR(cha.servicestartdate), ' ') AS servicestartdate
        , COALESCE(TO_CHAR(cha.stage), ' ') AS stage
        , COALESCE(TO_CHAR(cha.subscriptionline), ' ') AS subscriptionline
        , COALESCE(TO_CHAR(cha.subsidiary), ' ') AS subsidiary
        , COALESCE(TO_CHAR(cha.use), ' ') AS use
        , COALESCE(TO_CHAR(cha.memo), ' ') AS memo
        , COALESCE(TO_CHAR(cha.discountamount), ' ') AS discountamount
        , COALESCE(TO_CHAR(cha.creditmemoline), ' ') AS creditmemoline
    FROM charge cha"""

NETSUITE_CONSOLIDATEDEXCHANGERATE_SQL_QUERY = """SELECT
        COALESCE(TO_CHAR(cer.accountingbook), ' ') AS accountingbook
        , COALESCE(TO_CHAR(cer.averagerate), ' ') AS averagerate
        , COALESCE(TO_CHAR(cer.currentrate), ' ') AS currentrate
        , COALESCE(TO_CHAR(cer.fromcurrency), ' ') AS fromcurrency
        , COALESCE(TO_CHAR(cer.fromsubsidiary), ' ') AS fromsubsidiary
        , COALESCE(TO_CHAR(cer.historicalrate), ' ') AS historicalrate
        , COALESCE(TO_CHAR(cer.id), ' ') AS id
        , COALESCE(TO_CHAR(cer.postingperiod), ' ') AS postingperiod
        , COALESCE(TO_CHAR(cer.tocurrency), ' ') AS tocurrency
        , COALESCE(TO_CHAR(cer.tosubsidiary), ' ') AS tosubsidiary
        , COALESCE(TO_CHAR(ap.startdate), ' ') AS startdate
        , COALESCE(TO_CHAR(ap.enddate), ' ') AS enddate
        , COALESCE(TO_CHAR(sub1.country), ' ') AS fromcountry
        , COALESCE(TO_CHAR(sub2.country), ' ') AS tocountry
    FROM consolidatedexchangerate cer
    LEFT JOIN accountingperiod ap ON cer.postingperiod = ap.id
    LEFT JOIN subsidiary sub1 ON cer.fromsubsidiary = sub1.id
    LEFT JOIN subsidiary sub2 ON cer.tosubsidiary = sub2.id"""

NETSUITE_CUSTOMER_SQL_QUERY = """SELECT
        COALESCE(TO_CHAR(cus.alcoholrecipienttype), ' ') AS alcoholrecipienttype
        , COALESCE(TO_CHAR(cus.altname), ' ') AS altname
        , COALESCE(TO_CHAR(cus.balancesearch), ' ') AS balancesearch
        , COALESCE(TO_CHAR(cus.companyname), ' ') AS companyname
        , COALESCE(TO_CHAR(cus.consolbalancesearch), ' ') AS consolbalancesearch
        , COALESCE(TO_CHAR(cus.consoldaysoverduesearch), ' ') AS consoldaysoverduesearch
        , COALESCE(TO_CHAR(cus.consoloverduebalancesearch), ' ') AS consoloverduebalancesearch
        , COALESCE(TO_CHAR(cus.consolunbilledorderssearch), ' ') AS consolunbilledorderssearch
        , COALESCE(TO_CHAR(cus.creditholdoverride), ' ') AS creditholdoverride
        , COALESCE(TO_CHAR(cus.cseg_nov_cust_cat), ' ') AS cseg_nov_cust_cat
        , COALESCE(TO_CHAR(cus.currency), ' ') AS currency
        , COALESCE(TO_CHAR(cus.custentity_2663_customer_refund), ' ') AS custentity_2663_customer_refund
        , COALESCE(TO_CHAR(cus.custentity_2663_direct_debit), ' ') AS custentity_2663_direct_debit
        , COALESCE(TO_CHAR(cus.custentity_9572_custref_file_format), ' ') AS custentity_9572_custref_file_format
        , COALESCE(TO_CHAR(cus.custentity_9572_dd_file_format), ' ') AS custentity_9572_dd_file_format
        , COALESCE(TO_CHAR(cus.custentity_9572_ddcust_entitybank_sub), ' ') AS custentity_9572_ddcust_entitybank_sub
        , COALESCE(TO_CHAR(cus.custentity_9572_ddcust_entitybnkformat), ' ') AS custentity_9572_ddcust_entitybnkformat
        , COALESCE(TO_CHAR(cus.custentity_9572_refcust_entitybnkformat), ' ') AS custentity_9572_refcust_entitybnkformat
        , COALESCE(TO_CHAR(cus.custentity_9572_refundcust_entitybnk_sub), ' ') AS custentity_9572_refundcust_entitybnk_sub
        , COALESCE(TO_CHAR(cus.custentity_9997_dd_file_format), ' ') AS custentity_9997_dd_file_format
        , COALESCE(TO_CHAR(cus.custentity_alf_company_reg_num), ' ') AS custentity_alf_company_reg_num
        , COALESCE(TO_CHAR(cus.custentity_alf_cust_hide_service_periods), ' ') AS custentity_alf_cust_hide_service_periods
        , COALESCE(TO_CHAR(cus.custentity_alf_customer_hide_total_vat), ' ') AS custentity_alf_customer_hide_total_vat
        , COALESCE(TO_CHAR(cus.custentity_alf_customer_store_pdf), ' ') AS custentity_alf_customer_store_pdf
        , COALESCE(TO_CHAR(cus.custentity_atlas_customer_invoice_email), ' ') AS custentity_atlas_customer_invoice_email
        , COALESCE(TO_CHAR(cus.custentity_atlas_customer_probability), ' ') AS custentity_atlas_customer_probability
        , COALESCE(TO_CHAR(cus.custentity_bs_entityname), ' ') AS custentity_bs_entityname
        , COALESCE(TO_CHAR(cus.custentity_edoc_gen_trans_pdf), ' ') AS custentity_edoc_gen_trans_pdf
        , COALESCE(TO_CHAR(cus.custentity_emea_company_reg_num), ' ') AS custentity_emea_company_reg_num
        , COALESCE(TO_CHAR(cus.custentity_erpff_p2p_auto_send_document), ' ') AS custentity_erpff_p2p_auto_send_document
        , COALESCE(TO_CHAR(cus.custentity_id_scream), ' ') AS custentity_id_scream
        , COALESCE(TO_CHAR(cus.custentity_naw_trans_need_approval), ' ') AS custentity_naw_trans_need_approval
        , COALESCE(TO_CHAR(cus.custentity_nov_clean_search), ' ') AS custentity_nov_clean_search
        , COALESCE(TO_CHAR(cus.custentity_nov_company_name), ' ') AS custentity_nov_company_name
        , COALESCE(TO_CHAR(cus.custentity_nov_dun_1st_dunning), ' ') AS custentity_nov_dun_1st_dunning
        , COALESCE(TO_CHAR(cus.custentity_nov_dun_1st_dunning_date), ' ') AS custentity_nov_dun_1st_dunning_date
        , COALESCE(TO_CHAR(cus.custentity_nov_dun_1st_dunning_processed), ' ') AS custentity_nov_dun_1st_dunning_processed
        , COALESCE(TO_CHAR(cus.custentity_nov_dun_2nd_dunning), ' ') AS custentity_nov_dun_2nd_dunning
        , COALESCE(TO_CHAR(cus.custentity_nov_dun_2nd_dunning_date), ' ') AS custentity_nov_dun_2nd_dunning_date
        , COALESCE(TO_CHAR(cus.custentity_nov_dun_2nd_dunning_processed), ' ') AS custentity_nov_dun_2nd_dunning_processed
        , COALESCE(TO_CHAR(cus.custentity_nov_dun_3rd_dunning), ' ') AS custentity_nov_dun_3rd_dunning
        , COALESCE(TO_CHAR(cus.custentity_nov_dun_3rd_dunning_date), ' ') AS custentity_nov_dun_3rd_dunning_date
        , COALESCE(TO_CHAR(cus.custentity_nov_dun_3rd_dunning_processed), ' ') AS custentity_nov_dun_3rd_dunning_processed
        , COALESCE(TO_CHAR(cus.custentity_nov_dun_dunning_email), ' ') AS custentity_nov_dun_dunning_email
        , COALESCE(TO_CHAR(cus.custentity_nov_dun_exclude_from_dunning), ' ') AS custentity_nov_dun_exclude_from_dunning
        , COALESCE(TO_CHAR(cus.custentity_nov_dun_print_letter), ' ') AS custentity_nov_dun_print_letter
        , COALESCE(TO_CHAR(cus.custentity_nov_dun_send_email), ' ') AS custentity_nov_dun_send_email
        , COALESCE(TO_CHAR(cus.custentity_nov_num_client_autodesk), ' ') AS custentity_nov_num_client_autodesk
        , COALESCE(TO_CHAR(cus.custentity_nov_zone_tva), ' ') AS custentity_nov_zone_tva
        , COALESCE(TO_CHAR(cus.custentity_novdun_cus_4th_dunning), ' ') AS custentity_novdun_cus_4th_dunning
        , COALESCE(TO_CHAR(cus.custentity_novdun_cus_4th_dunning_proc), ' ') AS custentity_novdun_cus_4th_dunning_proc
        , COALESCE(TO_CHAR(cus.custentity_novdun_cus_5th_dunning), ' ') AS custentity_novdun_cus_5th_dunning
        , COALESCE(TO_CHAR(cus.custentity_novdun_cus_5th_dunning_proc), ' ') AS custentity_novdun_cus_5th_dunning_proc
        , COALESCE(TO_CHAR(cus.custentity_novdun_dunning_balance_store), ' ') AS custentity_novdun_dunning_balance_store
        , COALESCE(TO_CHAR(cus.custentity_novdun_last_balance_update), ' ') AS custentity_novdun_last_balance_update
        , COALESCE(TO_CHAR(cus.custentity_psg_ei_auto_select_temp_sm), ' ') AS custentity_psg_ei_auto_select_temp_sm
        , COALESCE(TO_CHAR(cus.custentity_psg_ei_entity_edoc_standard), ' ') AS custentity_psg_ei_entity_edoc_standard
        , COALESCE(TO_CHAR(cus.custentitycustentity_nov_dunnemail), ' ') AS custentitycustentity_nov_dunnemail
        , COALESCE(TO_CHAR(cus.custentitycustentity_nov_invemail), ' ') AS custentitycustentity_nov_invemail
        , COALESCE(TO_CHAR(cus.dateclosed), ' ') AS dateclosed
        , COALESCE(TO_CHAR(cus.datecreated), ' ') AS datecreated
        , COALESCE(TO_CHAR(cus.defaultbillingaddress), ' ') AS defaultbillingaddress
        , COALESCE(TO_CHAR(cus.defaultshippingaddress), ' ') AS defaultshippingaddress
        , COALESCE(TO_CHAR(cus.defaulttaxreg), ' ') AS defaulttaxreg
        , COALESCE(TO_CHAR(cus.draccount), ' ') AS draccount
        , COALESCE(TO_CHAR(cus.duplicate), ' ') AS duplicate
        , COALESCE(TO_CHAR(cus.email), ' ') AS email
        , COALESCE(TO_CHAR(cus.emailpreference), ' ') AS emailpreference
        , COALESCE(TO_CHAR(cus.emailtransactions), ' ') AS emailtransactions
        , COALESCE(TO_CHAR(cus.entityid), ' ') AS entityid
        , COALESCE(TO_CHAR(cus.entitynumber), ' ') AS entitynumber
        , COALESCE(TO_CHAR(cus.entitystatus), ' ') AS entitystatus
        , COALESCE(TO_CHAR(cus.entitytitle), ' ') AS entitytitle
        , COALESCE(TO_CHAR(cus.externalid), ' ') AS externalid
        , COALESCE(TO_CHAR(cus.fax), ' ') AS fax
        , COALESCE(TO_CHAR(cus.faxtransactions), ' ') AS faxtransactions
        , COALESCE(TO_CHAR(cus.firstorderdate), ' ') AS firstorderdate
        , COALESCE(TO_CHAR(cus.firstsaledate), ' ') AS firstsaledate
        , COALESCE(TO_CHAR(cus.id), ' ') AS id
        , COALESCE(TO_CHAR(cus.isautogeneratedrepresentingentity), ' ') AS isautogeneratedrepresentingentity
        , COALESCE(TO_CHAR(cus.isbudgetapproved), ' ') AS isbudgetapproved
        , COALESCE(TO_CHAR(cus.isinactive), ' ') AS isinactive
        , COALESCE(TO_CHAR(cus.isperson), ' ') AS isperson
        , COALESCE(TO_CHAR(cus.language), ' ') AS language
        , COALESCE(TO_CHAR(cus.lastmodifieddate), ' ') AS lastmodifieddate
        , COALESCE(TO_CHAR(cus.lastorderdate), ' ') AS lastorderdate
        , COALESCE(TO_CHAR(cus.lastsaledate), ' ') AS lastsaledate
        , COALESCE(TO_CHAR(cus.oncredithold), ' ') AS oncredithold
        , COALESCE(TO_CHAR(cus.overduebalancesearch), ' ') AS overduebalancesearch
        , COALESCE(TO_CHAR(cus.phone), ' ') AS phone
        , COALESCE(TO_CHAR(cus.printtransactions), ' ') AS printtransactions
        , COALESCE(TO_CHAR(cus.probability), ' ') AS probability
        , COALESCE(TO_CHAR(cus.receivablesaccount), ' ') AS receivablesaccount
        , COALESCE(TO_CHAR(cus.representingsubsidiary), ' ') AS representingsubsidiary
        , COALESCE(TO_CHAR(cus.searchstage), ' ') AS searchstage
        , COALESCE(TO_CHAR(cus.shipcomplete), ' ') AS shipcomplete
        , COALESCE(TO_CHAR(cus.shippingcarrier), ' ') AS shippingcarrier
        , COALESCE(TO_CHAR(cus.terms), ' ') AS terms
        , COALESCE(TO_CHAR(cus.unbilledorderssearch), ' ') AS unbilledorderssearch
        , COALESCE(TO_CHAR(cus.url), ' ') AS url
        , COALESCE(TO_CHAR(cus.weblead), ' ') AS weblead
        , COALESCE(TO_CHAR(cus.comments), ' ') AS comments
        , COALESCE(TO_CHAR(cus.custentity_alf_mop_default), ' ') AS custentity_alf_mop_default
        , COALESCE(TO_CHAR(cus.custentity_erpff_p2p_basw_tax_id), ' ') AS custentity_erpff_p2p_basw_tax_id
        , COALESCE(TO_CHAR(cus.custentity_erpff_p2p_basw_tax_scheme), ' ') AS custentity_erpff_p2p_basw_tax_scheme
        , COALESCE(TO_CHAR(cus.custentity_erpff_p2p_basw_tax_scheme_id), ' ') AS custentity_erpff_p2p_basw_tax_scheme_id
    FROM customer cus"""

NETSUITE_EMPLOYEE_SQL_QUERY = """SELECT
        COALESCE(TO_CHAR(emp.btemplate), ' ') AS btemplate
        , COALESCE(TO_CHAR(emp.currency), ' ') AS currency
        , COALESCE(TO_CHAR(emp.custentity_2663_payment_method), ' ') AS custentity_2663_payment_method
        , COALESCE(TO_CHAR(emp.custentity_nov_app_del_end_date), ' ') AS custentity_nov_app_del_end_date
        , COALESCE(TO_CHAR(emp.custentity_nov_app_del_start_date), ' ') AS custentity_nov_app_del_start_date
        , COALESCE(TO_CHAR(emp.custentity_nov_approver_delegate), ' ') AS custentity_nov_approver_delegate
        , COALESCE(TO_CHAR(emp.custentity_nov_emp_service), ' ') AS custentity_nov_emp_service
        , COALESCE(TO_CHAR(emp.custentity_nov_empl_mult_serv), ' ') AS custentity_nov_empl_mult_serv
        , COALESCE(TO_CHAR(emp.datecreated), ' ') AS datecreated
        , COALESCE(TO_CHAR(emp.defaultexpensereportcurrency), ' ') AS defaultexpensereportcurrency
        , COALESCE(TO_CHAR(emp.email), ' ') AS email
        , COALESCE(TO_CHAR(emp.entityid), ' ') AS entityid
        , COALESCE(TO_CHAR(emp.externalid), ' ') AS externalid
        , COALESCE(TO_CHAR(emp.firstname), ' ') AS firstname
        , COALESCE(TO_CHAR(emp.gender), ' ') AS gender
        , COALESCE(TO_CHAR(emp.giveaccess), ' ') AS giveaccess
        , COALESCE(TO_CHAR(emp.hiredate), ' ') AS hiredate
        , COALESCE(TO_CHAR(emp.i9verified), ' ') AS i9verified
        , COALESCE(TO_CHAR(emp.id), ' ') AS id
        , COALESCE(TO_CHAR(emp.initials), ' ') AS initials
        , COALESCE(TO_CHAR(emp.isinactive), ' ') AS isinactive
        , COALESCE(TO_CHAR(emp.issalesrep), ' ') AS issalesrep
        , COALESCE(TO_CHAR(emp.issupportrep), ' ') AS issupportrep
        , COALESCE(TO_CHAR(emp.lastmodifieddate), ' ') AS lastmodifieddate
        , COALESCE(TO_CHAR(emp.lastname), ' ') AS lastname
        , COALESCE(TO_CHAR(emp.purchaseorderlimit), ' ') AS purchaseorderlimit
        , COALESCE(TO_CHAR(emp.rolesforsearch), ' ') AS rolesforsearch
        , COALESCE(TO_CHAR(emp.salutation), ' ') AS salutation
        , COALESCE(TO_CHAR(emp.subsidiary), ' ') AS subsidiary
        , COALESCE(TO_CHAR(emp.workcalendar), ' ') AS workcalendar
    FROM employee emp"""

NETSUITE_ENTITY_SQL_QUERY = """SELECT
        COALESCE(TO_CHAR(ent.altname), ' ') AS altname
        , COALESCE(TO_CHAR(ent.comments), ' ') AS comments
        , COALESCE(TO_CHAR(ent.contact), ' ') AS contact
        , COALESCE(TO_CHAR(ent.customer), ' ') AS customer
        , COALESCE(TO_CHAR(ent.datecreated), ' ') AS datecreated
        , COALESCE(TO_CHAR(ent.defaulttaxreg), ' ') AS defaulttaxreg
        , COALESCE(TO_CHAR(ent.email), ' ') AS email
        , COALESCE(TO_CHAR(ent.employee), ' ') AS employee
        , COALESCE(TO_CHAR(ent.entityid), ' ') AS entityid
        , COALESCE(TO_CHAR(ent.entitynumber), ' ') AS entitynumber
        , COALESCE(TO_CHAR(ent.entitytitle), ' ') AS entitytitle
        , COALESCE(TO_CHAR(ent.externalid), ' ') AS externalid
        , COALESCE(TO_CHAR(ent.fax), ' ') AS fax
        , COALESCE(TO_CHAR(ent.firstname), ' ') AS firstname
        , COALESCE(TO_CHAR(ent.group), ' ') AS group
        , COALESCE(TO_CHAR(ent.id), ' ') AS id
        , COALESCE(TO_CHAR(ent.isinactive), ' ') AS isinactive
        , COALESCE(TO_CHAR(ent.isperson), ' ') AS isperson
        , COALESCE(TO_CHAR(ent.lastmodifieddate), ' ') AS lastmodifieddate
        , COALESCE(TO_CHAR(ent.lastname), ' ') AS lastname
        , COALESCE(TO_CHAR(ent.othername), ' ') AS othername
        , COALESCE(TO_CHAR(ent.phone), ' ') AS phone
        , COALESCE(TO_CHAR(ent.toplevelparent), ' ') AS toplevelparent
        , COALESCE(TO_CHAR(ent.type), ' ') AS type
        , COALESCE(TO_CHAR(ent.vendor), ' ') AS vendor
        , COALESCE(TO_CHAR(ent.salutation), ' ') AS salutation
        , COALESCE(TO_CHAR(ent.parent), ' ') AS parent
    FROM entity ent"""

NETSUITE_ITEM_SQL_QUERY = """SELECT
        COALESCE(TO_CHAR(itm.assetaccount), ' ') AS assetaccount
        , COALESCE(TO_CHAR(itm.atpmethod), ' ') AS atpmethod
        , COALESCE(TO_CHAR(itm.autoexpandkitforrevenuemgmt), ' ') AS autoexpandkitforrevenuemgmt
        , COALESCE(TO_CHAR(itm.averagecost), ' ') AS averagecost
        , COALESCE(TO_CHAR(itm.billexchratevarianceacct), ' ') AS billexchratevarianceacct
        , COALESCE(TO_CHAR(itm.billpricevarianceacct), ' ') AS billpricevarianceacct
        , COALESCE(TO_CHAR(itm.billqtyvarianceacct), ' ') AS billqtyvarianceacct
        , COALESCE(TO_CHAR(itm.copydescription), ' ') AS copydescription
        , COALESCE(TO_CHAR(itm.cost), ' ') AS cost
        , COALESCE(TO_CHAR(itm.costestimatetype), ' ') AS costestimatetype
        , COALESCE(TO_CHAR(itm.costingmethod), ' ') AS costingmethod
        , COALESCE(TO_CHAR(itm.costingmethoddisplay), ' ') AS costingmethoddisplay
        , COALESCE(TO_CHAR(itm.createddate), ' ') AS createddate
        , COALESCE(TO_CHAR(itm.createrevenueplanson), ' ') AS createrevenueplanson
        , COALESCE(TO_CHAR(itm.cseg_nov_bus_unit), ' ') AS cseg_nov_bus_unit
        , COALESCE(TO_CHAR(itm.cseg_nov_prod_fam), ' ') AS cseg_nov_prod_fam
        , COALESCE(TO_CHAR(itm.cseg_nov_prod_group), ' ') AS cseg_nov_prod_group
        , COALESCE(TO_CHAR(itm.cseg_nov_prod_key), ' ') AS cseg_nov_prod_key
        , COALESCE(TO_CHAR(itm.cseg_nov_prod_type), ' ') AS cseg_nov_prod_type
        , COALESCE(TO_CHAR(itm.cseg_nov_puk_cat), ' ') AS cseg_nov_puk_cat
        , COALESCE(TO_CHAR(itm.cseg_nov_rev_type), ' ') AS cseg_nov_rev_type
        , COALESCE(TO_CHAR(itm.cseg_nov_subcat), ' ') AS cseg_nov_subcat
        , COALESCE(TO_CHAR(itm.custitem1), ' ') AS custitem1
        , COALESCE(TO_CHAR(itm.custitem2), ' ') AS custitem2
        , COALESCE(TO_CHAR(itm.custitem_alf_print_item_name), ' ') AS custitem_alf_print_item_name
        , COALESCE(TO_CHAR(itm.custitem_code_analytique_scream), ' ') AS custitem_code_analytique_scream
        , COALESCE(TO_CHAR(itm.custitem_code_entite_scream), ' ') AS custitem_code_entite_scream
        , COALESCE(TO_CHAR(itm.custitem_code_tva_scream), ' ') AS custitem_code_tva_scream
        , COALESCE(TO_CHAR(itm.custitem_financement_scream), ' ') AS custitem_financement_scream
        , COALESCE(TO_CHAR(itm.custitem_nov_chapitre_scream), ' ') AS custitem_nov_chapitre_scream
        , COALESCE(TO_CHAR(itm.custitem_nov_ensemble), ' ') AS custitem_nov_ensemble
        , COALESCE(TO_CHAR(itm.custitem_nov_famille_scream), ' ') AS custitem_nov_famille_scream
        , COALESCE(TO_CHAR(itm.custitem_nov_intercompany), ' ') AS custitem_nov_intercompany
        , COALESCE(TO_CHAR(itm.custitem_nov_item_code), ' ') AS custitem_nov_item_code
        , COALESCE(TO_CHAR(itm.custitem_nov_purchase_account), ' ') AS custitem_nov_purchase_account
        , COALESCE(TO_CHAR(itm.custitem_nov_synchro_scream), ' ') AS custitem_nov_synchro_scream
        , COALESCE(TO_CHAR(itm.custitem_nov_type_subscription), ' ') AS custitem_nov_type_subscription
        , COALESCE(TO_CHAR(itm.custitem_nov_unite_consommation_scream), ' ') AS custitem_nov_unite_consommation_scream
        , COALESCE(TO_CHAR(itm.custitem_remise_scream), ' ') AS custitem_remise_scream
        , COALESCE(TO_CHAR(itm.custitem_sous_chapitre_scream), ' ') AS custitem_sous_chapitre_scream
        , COALESCE(TO_CHAR(itm.custitem_ste_item_taxitem_type), ' ') AS custitem_ste_item_taxitem_type
        , COALESCE(TO_CHAR(itm.custitem_ui_code_pays), ' ') AS custitem_ui_code_pays
        , COALESCE(TO_CHAR(itm.custitem_ui_date_obligatoire), ' ') AS custitem_ui_date_obligatoire
        , COALESCE(TO_CHAR(itm.deferredrevenueaccount), ' ') AS deferredrevenueaccount
        , COALESCE(TO_CHAR(itm.deferrevrec), ' ') AS deferrevrec
        , COALESCE(TO_CHAR(itm.description), ' ') AS description
        , COALESCE(TO_CHAR(itm.directrevenueposting), ' ') AS directrevenueposting
        , COALESCE(TO_CHAR(itm.displayname), ' ') AS displayname
        , COALESCE(TO_CHAR(itm.enforceminqtyinternally), ' ') AS enforceminqtyinternally
        , COALESCE(TO_CHAR(itm.excludefromsitemap), ' ') AS excludefromsitemap
        , COALESCE(TO_CHAR(itm.expenseaccount), ' ') AS expenseaccount
        , COALESCE(TO_CHAR(itm.externalid), ' ') AS externalid
        , COALESCE(TO_CHAR(itm.froogleproductfeed), ' ') AS froogleproductfeed
        , COALESCE(TO_CHAR(itm.fullname), ' ') AS fullname
        , COALESCE(TO_CHAR(itm.fxcost), ' ') AS fxcost
        , COALESCE(TO_CHAR(itm.generateaccruals), ' ') AS generateaccruals
        , COALESCE(TO_CHAR(itm.id), ' ') AS id
        , COALESCE(TO_CHAR(itm.includechildren), ' ') AS includechildren
        , COALESCE(TO_CHAR(itm.incomeaccount), ' ') AS incomeaccount
        , COALESCE(TO_CHAR(itm.isdropshipitem), ' ') AS isdropshipitem
        , COALESCE(TO_CHAR(itm.isfulfillable), ' ') AS isfulfillable
        , COALESCE(TO_CHAR(itm.isinactive), ' ') AS isinactive
        , COALESCE(TO_CHAR(itm.islotitem), ' ') AS islotitem
        , COALESCE(TO_CHAR(itm.isonline), ' ') AS isonline
        , COALESCE(TO_CHAR(itm.isserialitem), ' ') AS isserialitem
        , COALESCE(TO_CHAR(itm.isspecialorderitem), ' ') AS isspecialorderitem
        , COALESCE(TO_CHAR(itm.itemid), ' ') AS itemid
        , COALESCE(TO_CHAR(itm.itemtype), ' ') AS itemtype
        , COALESCE(TO_CHAR(itm.lastmodifieddate), ' ') AS lastmodifieddate
        , COALESCE(TO_CHAR(itm.lastpurchaseprice), ' ') AS lastpurchaseprice
        , COALESCE(TO_CHAR(itm.location), ' ') AS location
        , COALESCE(TO_CHAR(itm.matchbilltoreceipt), ' ') AS matchbilltoreceipt
        , COALESCE(TO_CHAR(itm.nextagproductfeed), ' ') AS nextagproductfeed
        , COALESCE(TO_CHAR(itm.printitems), ' ') AS printitems
        , COALESCE(TO_CHAR(itm.purchasedescription), ' ') AS purchasedescription
        , COALESCE(TO_CHAR(itm.purchaseunit), ' ') AS purchaseunit
        , COALESCE(TO_CHAR(itm.revenuerecognitionrule), ' ') AS revenuerecognitionrule
        , COALESCE(TO_CHAR(itm.revrecforecastrule), ' ') AS revrecforecastrule
        , COALESCE(TO_CHAR(itm.saleunit), ' ') AS saleunit
        , COALESCE(TO_CHAR(itm.seasonaldemand), ' ') AS seasonaldemand
        , COALESCE(TO_CHAR(itm.shipindividually), ' ') AS shipindividually
        , COALESCE(TO_CHAR(itm.shoppingproductfeed), ' ') AS shoppingproductfeed
        , COALESCE(TO_CHAR(itm.shopzillaproductfeed), ' ') AS shopzillaproductfeed
        , COALESCE(TO_CHAR(itm.stockunit), ' ') AS stockunit
        , COALESCE(TO_CHAR(itm.subsidiary), ' ') AS subsidiary
        , COALESCE(TO_CHAR(itm.subtype), ' ') AS subtype
        , COALESCE(TO_CHAR(itm.supplyreplenishmentmethod), ' ') AS supplyreplenishmentmethod
        , COALESCE(TO_CHAR(itm.totalquantityonhand), ' ') AS totalquantityonhand
        , COALESCE(TO_CHAR(itm.totalvalue), ' ') AS totalvalue
        , COALESCE(TO_CHAR(itm.unitstype), ' ') AS unitstype
        , COALESCE(TO_CHAR(itm.usemarginalrates), ' ') AS usemarginalrates
        , COALESCE(TO_CHAR(itm.vendorname), ' ') AS vendorname
        , COALESCE(TO_CHAR(itm.weightunits), ' ') AS weightunits
        , COALESCE(TO_CHAR(itm.yahooproductfeed), ' ') AS yahooproductfeed
        , COALESCE(TO_CHAR(cnbu.name), ' ') AS bus_unit
        , COALESCE(TO_CHAR(cnpf.name), ' ') AS prod_fam
        , COALESCE(TO_CHAR(cnpg.name), ' ') AS prod_group
        , COALESCE(TO_CHAR(cnpk.name), ' ') AS prod_key
        , COALESCE(TO_CHAR(cnpt.name), ' ') AS prod_type
        , COALESCE(TO_CHAR(cnpc.name), ' ') AS puk_cat
        , COALESCE(TO_CHAR(cnrt.name), ' ') AS rev_type
        , COALESCE(TO_CHAR(cns.name), ' ') AS subcat
        , COALESCE(TO_CHAR(ces.name), ' ') AS code_entite_scream
        , COALESCE(TO_CHAR(cts.name), ' ') AS code_tva_scream
        , COALESCE(TO_CHAR(fs.name), ' ') AS financement_scream
        , COALESCE(TO_CHAR(ncs.name), ' ') AS chapitre_scream
        , COALESCE(TO_CHAR(nfs.name), ' ') AS famille_scream
        , COALESCE(TO_CHAR(nucs.name), ' ') AS unite_consommation_scream
        , COALESCE(TO_CHAR(scs.name), ' ') AS sous_chapitre_scream
        , COALESCE(TO_CHAR(nts.name), ' ') AS type_subscription
    FROM item AS itm
    -- Custom records CSEG
    LEFT JOIN customrecord_cseg_nov_bus_unit AS cnbu ON itm.cseg_nov_bus_unit = cnbu.id
    LEFT JOIN customrecord_cseg_nov_prod_fam AS cnpf ON itm.cseg_nov_prod_fam = cnpf.id
    LEFT JOIN customrecord_cseg_nov_prod_group AS cnpg ON itm.cseg_nov_prod_group = cnpg.id
    LEFT JOIN customrecord_cseg_nov_prod_key AS cnpk ON itm.cseg_nov_prod_key = cnpk.id
    LEFT JOIN customrecord_cseg_nov_prod_type AS cnpt ON itm.cseg_nov_prod_type = cnpt.id
    LEFT JOIN customrecord_cseg_nov_puk_cat AS cnpc ON itm.cseg_nov_puk_cat = cnpc.id
    LEFT JOIN customrecord_cseg_nov_rev_type AS cnrt ON itm.cseg_nov_rev_type = cnrt.id
    LEFT JOIN customrecord_cseg_nov_subcat AS cns ON itm.cseg_nov_subcat = cns.id
    -- Custom records Scream
    LEFT JOIN customrecord_code_entite_scream ces ON itm.custitem_code_entite_scream = ces.id
    LEFT JOIN customlist_code_tva_scream cts ON itm.custitem_code_tva_scream = cts.id
    LEFT JOIN customlist_financement_scream fs ON itm.custitem_financement_scream = fs.id
    LEFT JOIN customrecord_chapitre_scream ncs ON itm.custitem_nov_chapitre_scream = ncs.id
    LEFT JOIN customlist_nov_famille_scream nfs ON itm.custitem_nov_famille_scream = nfs.id
    LEFT JOIN customlist_nov_unite_consommation_scre nucs ON itm.custitem_nov_unite_consommation_scream = nucs.id
    LEFT JOIN customrecord_sous_chapitre_scream scs ON itm.custitem_sous_chapitre_scream = scs.id
    -- Others custom records
    LEFT JOIN customlist_nov_type_subscription nts ON itm.custitem_nov_type_subscription = nts.id"""

NETSUITE_REVENUEELEMENT_SQL_QUERY = """SELECT
        COALESCE(TO_CHAR(reve.accountingbook), ' ') AS accountingbook
        , COALESCE(TO_CHAR(reve.allocationamount), ' ') AS allocationamount
        , COALESCE(TO_CHAR(reve.alternatequantity), ' ') AS alternatequantity
        , COALESCE(TO_CHAR(reve.alternateunits), ' ') AS alternateunits
        , COALESCE(TO_CHAR(reve.alternateunitstype), ' ') AS alternateunitstype
        , COALESCE(TO_CHAR(reve.createrevenueplanson), ' ') AS createrevenueplanson
        , COALESCE(TO_CHAR(reve.cseg_nov_bus_unit), ' ') AS cseg_nov_bus_unit
        , COALESCE(TO_CHAR(reve.cseg_nov_cust_cat), ' ') AS cseg_nov_cust_cat
        , COALESCE(TO_CHAR(reve.cseg_nov_prod_fam), ' ') AS cseg_nov_prod_fam
        , COALESCE(TO_CHAR(reve.cseg_nov_prod_group), ' ') AS cseg_nov_prod_group
        , COALESCE(TO_CHAR(reve.cseg_nov_prod_key), ' ') AS cseg_nov_prod_key
        , COALESCE(TO_CHAR(reve.cseg_nov_prod_type), ' ') AS cseg_nov_prod_type
        , COALESCE(TO_CHAR(reve.cseg_nov_puk_cat), ' ') AS cseg_nov_puk_cat
        , COALESCE(TO_CHAR(reve.cseg_nov_rev_categ), ' ') AS cseg_nov_rev_categ
        , COALESCE(TO_CHAR(reve.cseg_nov_rev_type), ' ') AS cseg_nov_rev_type
        , COALESCE(TO_CHAR(reve.cseg_nov_subcat), ' ') AS cseg_nov_subcat
        , COALESCE(TO_CHAR(reve.cseg_nov_type_aff), ' ') AS cseg_nov_type_aff
        , COALESCE(TO_CHAR(reve.currency), ' ') AS currency
        , COALESCE(TO_CHAR(reve.deferralaccount), ' ') AS deferralaccount
        , COALESCE(TO_CHAR(reve.discountedsalesamount), ' ') AS discountedsalesamount
        , COALESCE(TO_CHAR(reve.elementdate), ' ') AS elementdate
        , COALESCE(TO_CHAR(reve.entity), ' ') AS entity
        , COALESCE(TO_CHAR(reve.exchangerate), ' ') AS exchangerate
        , COALESCE(TO_CHAR(reve.forecastenddate), ' ') AS forecastenddate
        , COALESCE(TO_CHAR(reve.forecaststartdate), ' ') AS forecaststartdate
        , COALESCE(TO_CHAR(reve.fullname), ' ') AS fullname
        , COALESCE(TO_CHAR(reve.fxproratediscsalesamt), ' ') AS fxproratediscsalesamt
        , COALESCE(TO_CHAR(reve.id), ' ') AS id
        , COALESCE(TO_CHAR(reve.isbomitemtype), ' ') AS isbomitemtype
        , COALESCE(TO_CHAR(reve.item), ' ') AS item
        , COALESCE(TO_CHAR(reve.itemisautoexpand), ' ') AS itemisautoexpand
        , COALESCE(TO_CHAR(reve.lastmodifieddate), ' ') AS lastmodifieddate
        , COALESCE(TO_CHAR(reve.postingdiscountapplied), ' ') AS postingdiscountapplied
        , COALESCE(TO_CHAR(reve.proratediscsalesamt), ' ') AS proratediscsalesamt
        , COALESCE(TO_CHAR(reve.quantity), ' ') AS quantity
        , COALESCE(TO_CHAR(reve.recognitionaccount), ' ') AS recognitionaccount
        , COALESCE(TO_CHAR(reve.recordnumber), ' ') AS recordnumber
        , COALESCE(TO_CHAR(reve.referenceid), ' ') AS referenceid
        , COALESCE(TO_CHAR(reve.requiresrevenueplanupdate), ' ') AS requiresrevenueplanupdate
        , COALESCE(TO_CHAR(reve.returnofelement), ' ') AS returnofelement
        , COALESCE(TO_CHAR(reve.revenuearrangement), ' ') AS revenuearrangement
        , COALESCE(TO_CHAR(reve.revenueplanstatus), ' ') AS revenueplanstatus
        , COALESCE(TO_CHAR(reve.revenuerecognitionrule), ' ') AS revenuerecognitionrule
        , COALESCE(TO_CHAR(reve.revrecenddate), ' ') AS revrecenddate
        , COALESCE(TO_CHAR(reve.revrecforecastrule), ' ') AS revrecforecastrule
        , COALESCE(TO_CHAR(reve.revreclassfxaccount), ' ') AS revreclassfxaccount
        , COALESCE(TO_CHAR(reve.revrecstartdate), ' ') AS revrecstartdate
        , COALESCE(TO_CHAR(reve.salesamount), ' ') AS salesamount
        , COALESCE(TO_CHAR(reve.source), ' ') AS source
        , COALESCE(TO_CHAR(reve.sourcerecordtype), ' ') AS sourcerecordtype
        , COALESCE(TO_CHAR(reve.subscriptionline), ' ') AS subscriptionline
        , COALESCE(TO_CHAR(reve.subsidiary), ' ') AS subsidiary
        , COALESCE(TO_CHAR(reve.lastmergedfromarrangement), ' ') AS lastmergedfromarrangement
        , COALESCE(TO_CHAR(reve.lastmergetype), ' ') AS lastmergetype
        , COALESCE(TO_CHAR(reve.originalchangeorderdiscamount), ' ') AS originalchangeorderdiscamount
        , COALESCE(TO_CHAR(reve.originalchangeorderquantity), ' ') AS originalchangeorderquantity
        , COALESCE(TO_CHAR(reve.location), ' ') AS location
        , COALESCE(TO_CHAR(reve.units), ' ') AS units
    FROM revenueelement reve"""

NETSUITE_REVENUEPLAN_SQL_QUERY = """SELECT
        COALESCE(TO_CHAR(revp.amount), ' ') AS amount
        , COALESCE(TO_CHAR(revp.amountsource), ' ') AS amountsource
        , COALESCE(TO_CHAR(revp.createdfrom), ' ') AS createdfrom
        , COALESCE(TO_CHAR(revp.creationtriggeredbydisplay), ' ') AS creationtriggeredbydisplay
        , COALESCE(TO_CHAR(revp.enddatechangeimpactfordisplay), ' ') AS enddatechangeimpactfordisplay
        , COALESCE(TO_CHAR(revp.exchangerate), ' ') AS exchangerate
        , COALESCE(TO_CHAR(revp.holdrevenuerecognition), ' ') AS holdrevenuerecognition
        , COALESCE(TO_CHAR(revp.id), ' ') AS id
        , COALESCE(TO_CHAR(revp.iseliminate), ' ') AS iseliminate
        , COALESCE(TO_CHAR(revp.item), ' ') AS item
        , COALESCE(TO_CHAR(revp.lastmodifieddate), ' ') AS lastmodifieddate
        , COALESCE(TO_CHAR(revp.parentlinecurrency), ' ') AS parentlinecurrency
        , COALESCE(TO_CHAR(revp.recognitionmethod), ' ') AS recognitionmethod
        , COALESCE(TO_CHAR(revp.recordnumber), ' ') AS recordnumber
        , COALESCE(TO_CHAR(revp.remainingdeferredbalance), ' ') AS remainingdeferredbalance
        , COALESCE(TO_CHAR(revp.revenueplancurrency), ' ') AS revenueplancurrency
        , COALESCE(TO_CHAR(revp.revenueplantype), ' ') AS revenueplantype
        , COALESCE(TO_CHAR(revp.revenuerecognitionrule), ' ') AS revenuerecognitionrule
        , COALESCE(TO_CHAR(revp.revrecenddate), ' ') AS revrecenddate
        , COALESCE(TO_CHAR(revp.revrecenddatesource), ' ') AS revrecenddatesource
        , COALESCE(TO_CHAR(revp.revrecstartdate), ' ') AS revrecstartdate
        , COALESCE(TO_CHAR(revp.revrecstartdatesource), ' ') AS revrecstartdatesource
        , COALESCE(TO_CHAR(revp.statusfordisplay), ' ') AS statusfordisplay
        , COALESCE(TO_CHAR(revp.totalrecognized), ' ') AS totalrecognized
        , COALESCE(TO_CHAR(revp.reforecastmethod), ' ') AS reforecastmethod
        , COALESCE(TO_CHAR(revp.catchupperiod), ' ') AS catchupperiod
        , COALESCE(TO_CHAR(revp.terminmonths), ' ') AS terminmonths
    FROM revenueplan revp"""

NETSUITE_REVENUEPLANPLANNEDREVENUE_SQL_QUERY = """SELECT * FROM
    (
        SELECT
            COALESCE(TO_CHAR(rppr.amount), ' ') AS amount
            , COALESCE(TO_CHAR(rppr.dateexecuted), ' ') AS dateexecuted
            , COALESCE(TO_CHAR(rppr.deferredrevenueaccount), ' ') AS deferredrevenueaccount
            , COALESCE(TO_CHAR(rppr.exchangerate), ' ') AS exchangerate
            , COALESCE(TO_CHAR(rppr.id), ' ') AS id
            , COALESCE(TO_CHAR(rppr.isrecognized), ' ') AS isrecognized
            , COALESCE(TO_CHAR(rppr.journal), ' ') AS journal
            , COALESCE(TO_CHAR(rppr.percentrecognizedinperiod), ' ') AS percentrecognizedinperiod
            , COALESCE(TO_CHAR(rppr.percenttotalrecognized), ' ') AS percenttotalrecognized
            , COALESCE(TO_CHAR(rppr.plannedperiod), ' ') AS plannedperiod
            , COALESCE(TO_CHAR(rppr.plannedrevenuetype), ' ') AS plannedrevenuetype
            , COALESCE(TO_CHAR(rppr.postingperiod), ' ') AS postingperiod
            , COALESCE(TO_CHAR(rppr.recognitionaccount), ' ') AS recognitionaccount
            , COALESCE(TO_CHAR(rppr.revenueplan), ' ') AS revenueplan
            , COALESCE(TO_CHAR(rppr.totalrecognized), ' ') AS totalrecognized
            , COALESCE(TO_CHAR(rp.lastmodifieddate), ' ') AS lastmodifieddate
        FROM revenueplanplannedrevenue rppr
        LEFT JOIN revenueplan rp ON rp.id = rppr.revenueplan
    ) rppr"""

NETSUITE_SUBSCRIPTION_SQL_QUERY = """SELECT
        COALESCE(TO_CHAR(sub.advancerenewalperiodnumber), ' ') AS advancerenewalperiodnumber
        , COALESCE(TO_CHAR(sub.advancerenewalperiodunit), ' ') AS advancerenewalperiodunit
        , COALESCE(TO_CHAR(sub.alignchargewithsub), ' ') AS alignchargewithsub
        , COALESCE(TO_CHAR(sub.autorenewal), ' ') AS autorenewal
        , COALESCE(TO_CHAR(sub.billingaccount), ' ') AS billingaccount
        , COALESCE(TO_CHAR(sub.billingschedule), ' ') AS billingschedule
        , COALESCE(TO_CHAR(sub.billingsubscriptionstatus), ' ') AS billingsubscriptionstatus
        , COALESCE(TO_CHAR(sub.currency), ' ') AS currency
        , COALESCE(TO_CHAR(sub.customer), ' ') AS customer
        , COALESCE(TO_CHAR(sub.custrecord_import_contrats_en_cours), ' ') AS custrecord_import_contrats_en_cours
        , COALESCE(TO_CHAR(sub.custrecord_nov_code_service), ' ') AS custrecord_nov_code_service
        , COALESCE(TO_CHAR(sub.custrecord_nov_customer_category_sub), ' ') AS custrecord_nov_customer_category_sub
        , COALESCE(TO_CHAR(sub.custrecord_nov_date_indexation), ' ') AS custrecord_nov_date_indexation
        , COALESCE(TO_CHAR(sub.custrecord_nov_fusion), ' ') AS custrecord_nov_fusion
        , COALESCE(TO_CHAR(sub.custrecord_nov_num_contrat), ' ') AS custrecord_nov_num_contrat
        , COALESCE(TO_CHAR(sub.custrecord_nov_num_engagement), ' ') AS custrecord_nov_num_engagement
        , COALESCE(TO_CHAR(sub.custrecord_nov_num_marche), ' ') AS custrecord_nov_num_marche
        , COALESCE(TO_CHAR(sub.custrecord_nov_sub_end_customer), ' ') AS custrecord_nov_sub_end_customer
        , COALESCE(TO_CHAR(sub.custrecord_nov_sub_siret_number), ' ') AS custrecord_nov_sub_siret_number
        , COALESCE(TO_CHAR(sub.custrecord_nov_type_indexation), ' ') AS custrecord_nov_type_indexation
        , COALESCE(TO_CHAR(sub.defaultrenewalmethod), ' ') AS defaultrenewalmethod
        , COALESCE(TO_CHAR(sub.defaultrenewalplan), ' ') AS defaultrenewalplan
        , COALESCE(TO_CHAR(sub.defaultrenewalterm), ' ') AS defaultrenewalterm
        , COALESCE(TO_CHAR(sub.defaultrenewaltrantype), ' ') AS defaultrenewaltrantype
        , COALESCE(TO_CHAR(sub.enddate), ' ') AS enddate
        , COALESCE(TO_CHAR(sub.externalid), ' ') AS externalid
        , COALESCE(TO_CHAR(sub.frequency), ' ') AS frequency
        , COALESCE(TO_CHAR(sub.generatemodificationelements), ' ') AS generatemodificationelements
        , COALESCE(TO_CHAR(sub.id), ' ') AS id
        , COALESCE(TO_CHAR(sub.initialterm), ' ') AS initialterm
        , COALESCE(TO_CHAR(sub.lastbillcycledate), ' ') AS lastbillcycledate
        , COALESCE(TO_CHAR(sub.lastbilldate), ' ') AS lastbilldate
        , COALESCE(TO_CHAR(sub.lastmodifieddate), ' ') AS lastmodifieddate
        , COALESCE(TO_CHAR(sub.name), ' ') AS name
        , COALESCE(TO_CHAR(sub.nextbillcycledate), ' ') AS nextbillcycledate
        , COALESCE(TO_CHAR(sub.nextrenewalstartdate), ' ') AS nextrenewalstartdate
        , COALESCE(TO_CHAR(sub.pricebook), ' ') AS pricebook
        , COALESCE(TO_CHAR(sub.renewalnumber), ' ') AS renewalnumber
        , COALESCE(TO_CHAR(sub.rootsubscription), ' ') AS rootsubscription
        , COALESCE(TO_CHAR(sub.salesorder), ' ') AS salesorder
        , COALESCE(TO_CHAR(sub.startdate), ' ') AS startdate
        , COALESCE(TO_CHAR(sub.subscriptionplanname), ' ') AS subscriptionplanname
        , COALESCE(TO_CHAR(sub.subscriptionrevision), ' ') AS subscriptionrevision
        , COALESCE(TO_CHAR(sub.subsidiary), ' ') AS subsidiary
    FROM subscription sub"""

NETSUITE_SUBSCRIPTIONLINE_SQL_QUERY = """SELECT
        COALESCE(TO_CHAR(subl.alignchargewithsub), ' ') AS alignchargewithsub
        , COALESCE(TO_CHAR(subl.billingmode), ' ') AS billingmode
        , COALESCE(TO_CHAR(subl.catalogtype), ' ') AS catalogtype
        , NULL AS custrecord1414
        , COALESCE(TO_CHAR(subl.custrecord_nov_business_unit), ' ') AS custrecord_nov_business_unit
        , COALESCE(TO_CHAR(subl.custrecord_nov_customer_category_sub_lin), ' ') AS custrecord_nov_customer_category_sub_lin
        , COALESCE(TO_CHAR(subl.custrecord_nov_fusion_sub_line), ' ') AS custrecord_nov_fusion_sub_line
        , COALESCE(TO_CHAR(subl.custrecord_nov_memo_sub_line), ' ') AS custrecord_nov_memo_sub_line
        , COALESCE(TO_CHAR(subl.custrecord_nov_product_family), ' ') AS custrecord_nov_product_family
        , COALESCE(TO_CHAR(subl.custrecord_nov_product_group), ' ') AS custrecord_nov_product_group
        , COALESCE(TO_CHAR(subl.custrecord_nov_product_type), ' ') AS custrecord_nov_product_type
        , COALESCE(TO_CHAR(subl.custrecord_nov_puk), ' ') AS custrecord_nov_puk
        , COALESCE(TO_CHAR(subl.custrecord_nov_puk_category), ' ') AS custrecord_nov_puk_category
        , COALESCE(TO_CHAR(subl.custrecord_nov_puk_sub_category), ' ') AS custrecord_nov_puk_sub_category
        , COALESCE(TO_CHAR(subl.custrecord_nov_rev_categ), ' ') AS custrecord_nov_rev_categ
        , COALESCE(TO_CHAR(subl.custrecord_nov_revenue_type), ' ') AS custrecord_nov_revenue_type
        , COALESCE(TO_CHAR(subl.custrecord_nov_sales_order), ' ') AS custrecord_nov_sales_order
        , COALESCE(TO_CHAR(subl.custrecord_nov_so_line_num), ' ') AS custrecord_nov_so_line_num
        , COALESCE(TO_CHAR(subl.custrecord_nov_sub_line_end_customer), ' ') AS custrecord_nov_sub_line_end_customer
        , COALESCE(TO_CHAR(subl.custrecord_nov_sub_line_siret_number), ' ') AS custrecord_nov_sub_line_siret_number
        , COALESCE(TO_CHAR(subl.custrecord_nov_subline_tax_code), ' ') AS custrecord_nov_subline_tax_code
        , COALESCE(TO_CHAR(subl.custrecord_nov_type_aff), ' ') AS custrecord_nov_type_aff
        , COALESCE(TO_CHAR(subl.custrecord_po_number_add_on), ' ') AS custrecord_po_number_add_on
        , COALESCE(TO_CHAR(subl.enddate), ' ') AS enddate
        , COALESCE(TO_CHAR(subl.id), ' ') AS id
        , COALESCE(TO_CHAR(subl.includeinrenewal), ' ') AS includeinrenewal
        , COALESCE(TO_CHAR(subl.isincluded), ' ') AS isincluded
        , COALESCE(TO_CHAR(subl.item), ' ') AS item
        , COALESCE(TO_CHAR(subl.lastmodifieddate), ' ') AS lastmodifieddate
        , COALESCE(TO_CHAR(subl.linenumber), ' ') AS linenumber
        , COALESCE(TO_CHAR(subl.ponumber), ' ') AS ponumber
        , COALESCE(TO_CHAR(subl.prorateenddate), ' ') AS prorateenddate
        , COALESCE(TO_CHAR(subl.proratestartdate), ' ') AS proratestartdate
        , COALESCE(TO_CHAR(subl.quantity), ' ') AS quantity
        , COALESCE(TO_CHAR(subl.recurrencestartdate), ' ') AS recurrencestartdate
        , COALESCE(TO_CHAR(subl.revrecoption), ' ') AS revrecoption
        , COALESCE(TO_CHAR(subl.salesorder), ' ') AS salesorder
        , COALESCE(TO_CHAR(subl.salesorderlinenumber), ' ') AS salesorderlinenumber
        , COALESCE(TO_CHAR(subl.startdate), ' ') AS startdate
        , COALESCE(TO_CHAR(subl.subscription), ' ') AS subscription
        , COALESCE(TO_CHAR(subl.subscriptionlinestatus), ' ') AS subscriptionlinestatus
        , COALESCE(TO_CHAR(subl.subscriptionlinetype), ' ') AS subscriptionlinetype
        , COALESCE(TO_CHAR(subl.subscriptionplan), ' ') AS subscriptionplan
        , COALESCE(TO_CHAR(subl.subscriptionplanline), ' ') AS subscriptionplanline
        , COALESCE(TO_CHAR(subl.terminationdate), ' ') AS terminationdate
        , COALESCE(TO_CHAR(subl.total), ' ') AS total
    FROM subscriptionline subl"""

NETSUITE_SUBSIDIARY_SQL_QUERY = """SELECT
        COALESCE(TO_CHAR(sub.country), ' ') AS country
        , COALESCE(TO_CHAR(sub.currency), ' ') AS currency
        , COALESCE(TO_CHAR(sub.custrecord_alf_company_reg_num), ' ') AS custrecord_alf_company_reg_num
        , COALESCE(TO_CHAR(sub.custrecord_alf_mark_copy_or_duplicate), ' ') AS custrecord_alf_mark_copy_or_duplicate
        , COALESCE(TO_CHAR(sub.custrecord_alf_sub_hide_service_periods), ' ') AS custrecord_alf_sub_hide_service_periods
        , COALESCE(TO_CHAR(sub.custrecord_alf_sub_hide_total_vat), ' ') AS custrecord_alf_sub_hide_total_vat
        , COALESCE(TO_CHAR(sub.custrecord_alf_sub_lock_invoice), ' ') AS custrecord_alf_sub_lock_invoice
        , COALESCE(TO_CHAR(sub.custrecord_alf_sub_note_no_pay_dis), ' ') AS custrecord_alf_sub_note_no_pay_dis
        , COALESCE(TO_CHAR(sub.custrecord_alf_sub_print_bill_to_right), ' ') AS custrecord_alf_sub_print_bill_to_right
        , COALESCE(TO_CHAR(sub.custrecord_alf_tax_type), ' ') AS custrecord_alf_tax_type
        , COALESCE(TO_CHAR(sub.custrecord_alf_vat_summary_in_base_curr), ' ') AS custrecord_alf_vat_summary_in_base_curr
        , COALESCE(TO_CHAR(sub.custrecord_atlas_prowb_translations), ' ') AS custrecord_atlas_prowb_translations
        , COALESCE(TO_CHAR(sub.custrecord_ff_sc_migrated_to_sub_setup), ' ') AS custrecord_ff_sc_migrated_to_sub_setup
        , COALESCE(TO_CHAR(sub.custrecord_footer_po_1), ' ') AS custrecord_footer_po_1
        , COALESCE(TO_CHAR(sub.custrecord_footer_po_2), ' ') AS custrecord_footer_po_2
        , COALESCE(TO_CHAR(sub.custrecord_nov_email_facturation), ' ') AS custrecord_nov_email_facturation
        , COALESCE(TO_CHAR(sub.custrecord_nov_sub_capital), ' ') AS custrecord_nov_sub_capital
        , COALESCE(TO_CHAR(sub.custrecord_nov_sub_legalstatus), ' ') AS custrecord_nov_sub_legalstatus
        , COALESCE(TO_CHAR(sub.custrecord_nov_sub_rcs), ' ') AS custrecord_nov_sub_rcs
        , COALESCE(TO_CHAR(sub.custrecord_nov_sub_tax_reg_nb), ' ') AS custrecord_nov_sub_tax_reg_nb
        , COALESCE(TO_CHAR(sub.custrecord_nov_subsidiary_naf), ' ') AS custrecord_nov_subsidiary_naf
        , COALESCE(TO_CHAR(sub.custrecord_nov_training_org_nb), ' ') AS custrecord_nov_training_org_nb
        , COALESCE(TO_CHAR(sub.custrecord_novdun_sub_email), ' ') AS custrecord_novdun_sub_email
        , COALESCE(TO_CHAR(sub.custrecord_novdun_telephone), ' ') AS custrecord_novdun_telephone
        , COALESCE(TO_CHAR(sub.custrecord_psg_ei_disable_country), ' ') AS custrecord_psg_ei_disable_country
        , COALESCE(TO_CHAR(sub.custrecord_psg_ei_license_free_country), ' ') AS custrecord_psg_ei_license_free_country
        , COALESCE(TO_CHAR(sub.custrecord_psg_lc_test_mode), ' ') AS custrecord_psg_lc_test_mode
        , COALESCE(TO_CHAR(sub.dropdownstate), ' ') AS dropdownstate
        , COALESCE(TO_CHAR(sub.edition), ' ') AS edition
        , COALESCE(TO_CHAR(sub.email), ' ') AS email
        , COALESCE(TO_CHAR(sub.federalidnumber), ' ') AS federalidnumber
        , COALESCE(TO_CHAR(sub.fiscalcalendar), ' ') AS fiscalcalendar
        , COALESCE(TO_CHAR(sub.fullname), ' ') AS fullname
        , COALESCE(TO_CHAR(sub.glimpactlocking), ' ') AS glimpactlocking
        , COALESCE(TO_CHAR(sub.id), ' ') AS id
        , COALESCE(TO_CHAR(sub.iselimination), ' ') AS iselimination
        , COALESCE(TO_CHAR(sub.isinactive), ' ') AS isinactive
        , COALESCE(TO_CHAR(sub.languagelocale), ' ') AS languagelocale
        , COALESCE(TO_CHAR(sub.lastmodifieddate), ' ') AS lastmodifieddate
        , COALESCE(TO_CHAR(sub.legalname), ' ') AS legalname
        , COALESCE(TO_CHAR(sub.mainaddress), ' ') AS mainaddress
        , COALESCE(TO_CHAR(sub.name), ' ') AS name
        , COALESCE(TO_CHAR(sub.parent), ' ') AS parent
        , COALESCE(TO_CHAR(sub.purchaseorderamount), ' ') AS purchaseorderamount
        , COALESCE(TO_CHAR(sub.purchaseorderquantity), ' ') AS purchaseorderquantity
        , COALESCE(TO_CHAR(sub.receiptquantity), ' ') AS receiptquantity
        , COALESCE(TO_CHAR(sub.representingcustomer), ' ') AS representingcustomer
        , COALESCE(TO_CHAR(sub.representingvendor), ' ') AS representingvendor
        , COALESCE(TO_CHAR(sub.shippingaddress), ' ') AS shippingaddress
        , COALESCE(TO_CHAR(sub.showsubsidiaryname), ' ') AS showsubsidiaryname
        , COALESCE(TO_CHAR(sub.state), ' ') AS state
        , COALESCE(TO_CHAR(sub.traninternalprefix), ' ') AS traninternalprefix
        , COALESCE(TO_CHAR(sub.tranprefix), ' ') AS tranprefix
        , COALESCE(TO_CHAR(cur.name), ' ') AS currency_name
        , COALESCE(TO_CHAR(cur.displaysymbol), ' ') AS currency_symbol
    FROM subsidiary sub
    LEFT JOIN currency cur ON sub.currency = cur.id"""

NETSUITE_TRANSACTION_SQL_QUERY = """SELECT
        COALESCE(TO_CHAR(tran.abbrevtype), ' ') AS abbrevtype
        , COALESCE(TO_CHAR(tran.actualshipdate), ' ') AS actualshipdate
        , COALESCE(TO_CHAR(tran.approvalstatus), ' ') AS approvalstatus
        , COALESCE(TO_CHAR(tran.asofdate), ' ') AS asofdate
        , COALESCE(TO_CHAR(tran.balsegstatus), ' ') AS balsegstatus
        , COALESCE(TO_CHAR(tran.basetaxtotal), ' ') AS basetaxtotal
        , COALESCE(TO_CHAR(tran.basetotalaftertaxes), ' ') AS basetotalaftertaxes
        , COALESCE(TO_CHAR(tran.billingaccount), ' ') AS billingaccount
        , COALESCE(TO_CHAR(tran.billingaddress), ' ') AS billingaddress
        , COALESCE(TO_CHAR(tran.billingstatus), ' ') AS billingstatus
        , COALESCE(TO_CHAR(tran.bulkprocsubmission), ' ') AS bulkprocsubmission
        , COALESCE(TO_CHAR(tran.closedate), ' ') AS closedate
        , COALESCE(TO_CHAR(tran.createdby), ' ') AS createdby
        , COALESCE(TO_CHAR(tran.createddate), ' ') AS createddate
        , COALESCE(TO_CHAR(tran.cseg_nov_cust_cat), ' ') AS cseg_nov_cust_cat
        , COALESCE(TO_CHAR(tran.currency), ' ') AS currency
        , COALESCE(TO_CHAR(tran.custbody1), ' ') AS custbody1
        , COALESCE(TO_CHAR(tran.custbody_15699_exclude_from_ep_process), ' ') AS custbody_15699_exclude_from_ep_process
        , COALESCE(TO_CHAR(tran.custbody_alf_bank_det_to_print), ' ') AS custbody_alf_bank_det_to_print
        , COALESCE(TO_CHAR(tran.custbody_alf_cfg), ' ') AS custbody_alf_cfg
        , COALESCE(TO_CHAR(tran.custbody_alf_currency_symbol), ' ') AS custbody_alf_currency_symbol
        , COALESCE(TO_CHAR(tran.custbody_alf_cust_inv_translations), ' ') AS custbody_alf_cust_inv_translations
        , COALESCE(TO_CHAR(tran.custbody_alf_customers_country), ' ') AS custbody_alf_customers_country
        , COALESCE(TO_CHAR(tran.custbody_alf_hide_total_vat_in_compinf), ' ') AS custbody_alf_hide_total_vat_in_compinf
        , COALESCE(TO_CHAR(tran.custbody_alf_mop), ' ') AS custbody_alf_mop
        , COALESCE(TO_CHAR(tran.custbody_alf_nexus_country_in_ste), ' ') AS custbody_alf_nexus_country_in_ste
        , COALESCE(TO_CHAR(tran.custbody_alf_print_payment_installment), ' ') AS custbody_alf_print_payment_installment
        , COALESCE(TO_CHAR(tran.custbody_alf_subsidiary_address), ' ') AS custbody_alf_subsidiary_address
        , COALESCE(TO_CHAR(tran.custbody_alf_subsidiary_name), ' ') AS custbody_alf_subsidiary_name
        , COALESCE(TO_CHAR(tran.custbody_alf_trans_bank_details), ' ') AS custbody_alf_trans_bank_details
        , COALESCE(TO_CHAR(tran.custbody_alf_tx_hide_total_vat), ' ') AS custbody_alf_tx_hide_total_vat
        , COALESCE(TO_CHAR(tran.custbody_alf_tx_print_details), ' ') AS custbody_alf_tx_print_details
        , COALESCE(TO_CHAR(tran.custbody_bs_ddr_donot_reprocess), ' ') AS custbody_bs_ddr_donot_reprocess
        , COALESCE(TO_CHAR(tran.custbody_chorus_manuel), ' ') AS custbody_chorus_manuel
        , COALESCE(TO_CHAR(tran.custbody_document_date), ' ') AS custbody_document_date
        , COALESCE(TO_CHAR(tran.custbody_edoc_gen_trans_pdf), ' ') AS custbody_edoc_gen_trans_pdf
        , COALESCE(TO_CHAR(tran.custbody_ei_ds_txn_identifier), ' ') AS custbody_ei_ds_txn_identifier
        , COALESCE(TO_CHAR(tran.custbody_erpff_p2p_auto_send_document), ' ') AS custbody_erpff_p2p_auto_send_document
        , COALESCE(TO_CHAR(tran.custbody_erpff_p2p_document_sent), ' ') AS custbody_erpff_p2p_document_sent
        , COALESCE(TO_CHAR(tran.custbody_ff_br_exclude_transaction), ' ') AS custbody_ff_br_exclude_transaction
        , COALESCE(TO_CHAR(tran.custbody_fl_fec_ecriturelib), ' ') AS custbody_fl_fec_ecriturelib
        , COALESCE(TO_CHAR(tran.custbody_import_contrats_en_cours), ' ') AS custbody_import_contrats_en_cours
        , COALESCE(TO_CHAR(tran.custbody_nov_account_percentage), ' ') AS custbody_nov_account_percentage
        , COALESCE(TO_CHAR(tran.custbody_nov_acompte), ' ') AS custbody_nov_acompte
        , COALESCE(TO_CHAR(tran.custbody_nov_autorenewal), ' ') AS custbody_nov_autorenewal
        , COALESCE(TO_CHAR(tran.custbody_nov_cbbasware), ' ') AS custbody_nov_cbbasware
        , COALESCE(TO_CHAR(tran.custbody_nov_cbchorus), ' ') AS custbody_nov_cbchorus
        , COALESCE(TO_CHAR(tran.custbody_nov_cbemail), ' ') AS custbody_nov_cbemail
        , COALESCE(TO_CHAR(tran.custbody_nov_cbtradeshift), ' ') AS custbody_nov_cbtradeshift
        , COALESCE(TO_CHAR(tran.custbody_nov_chorus_status), ' ') AS custbody_nov_chorus_status
        , COALESCE(TO_CHAR(tran.custbody_nov_code_service), ' ') AS custbody_nov_code_service
        , COALESCE(TO_CHAR(tran.custbody_nov_cond_fin_remplies), ' ') AS custbody_nov_cond_fin_remplies
        , COALESCE(TO_CHAR(tran.custbody_nov_cond_fin_req), ' ') AS custbody_nov_cond_fin_req
        , COALESCE(TO_CHAR(tran.custbody_nov_contact_dest), ' ') AS custbody_nov_contact_dest
        , COALESCE(TO_CHAR(tran.custbody_nov_contact_emetteur_cmd), ' ') AS custbody_nov_contact_emetteur_cmd
        , COALESCE(TO_CHAR(tran.custbody_nov_costcat_app_purchord), ' ') AS custbody_nov_costcat_app_purchord
        , COALESCE(TO_CHAR(tran.custbody_nov_custom_form), ' ') AS custbody_nov_custom_form
        , COALESCE(TO_CHAR(tran.custbody_nov_date_deb_sous), ' ') AS custbody_nov_date_deb_sous
        , COALESCE(TO_CHAR(tran.custbody_nov_date_envoi), ' ') AS custbody_nov_date_envoi
        , COALESCE(TO_CHAR(tran.custbody_nov_date_fin_sous), ' ') AS custbody_nov_date_fin_sous
        , COALESCE(TO_CHAR(tran.custbody_nov_drop_ship_po), ' ') AS custbody_nov_drop_ship_po
        , COALESCE(TO_CHAR(tran.custbody_nov_email_sent), ' ') AS custbody_nov_email_sent
        , COALESCE(TO_CHAR(tran.custbody_nov_employee_service), ' ') AS custbody_nov_employee_service
        , COALESCE(TO_CHAR(tran.custbody_nov_end_customer), ' ') AS custbody_nov_end_customer
        , COALESCE(TO_CHAR(tran.custbody_nov_facturation_automatique), ' ') AS custbody_nov_facturation_automatique
        , COALESCE(TO_CHAR(tran.custbody_nov_financement_materiel), ' ') AS custbody_nov_financement_materiel
        , COALESCE(TO_CHAR(tran.custbody_nov_fusion_so), ' ') AS custbody_nov_fusion_so
        , COALESCE(TO_CHAR(tran.custbody_nov_hide_num_devis), ' ') AS custbody_nov_hide_num_devis
        , COALESCE(TO_CHAR(tran.custbody_nov_informations), ' ') AS custbody_nov_informations
        , COALESCE(TO_CHAR(tran.custbody_nov_interco_checkbox), ' ') AS custbody_nov_interco_checkbox
        , COALESCE(TO_CHAR(tran.custbody_nov_items_received_fulfilled), ' ') AS custbody_nov_items_received_fulfilled
        , COALESCE(TO_CHAR(tran.custbody_nov_num_commande_client), ' ') AS custbody_nov_num_commande_client
        , COALESCE(TO_CHAR(tran.custbody_nov_num_contrat), ' ') AS custbody_nov_num_contrat
        , COALESCE(TO_CHAR(tran.custbody_nov_num_depot), ' ') AS custbody_nov_num_depot
        , COALESCE(TO_CHAR(tran.custbody_nov_num_engagement), ' ') AS custbody_nov_num_engagement
        , COALESCE(TO_CHAR(tran.custbody_nov_num_marche), ' ') AS custbody_nov_num_marche
        , COALESCE(TO_CHAR(tran.custbody_nov_paperesker), ' ') AS custbody_nov_paperesker
        , COALESCE(TO_CHAR(tran.custbody_nov_sales_order), ' ') AS custbody_nov_sales_order
        , COALESCE(TO_CHAR(tran.custbody_nov_so_rejected), ' ') AS custbody_nov_so_rejected
        , COALESCE(TO_CHAR(tran.custbody_nov_statut_commande), ' ') AS custbody_nov_statut_commande
        , COALESCE(TO_CHAR(tran.custbody_nov_type_commande), ' ') AS custbody_nov_type_commande
        , COALESCE(TO_CHAR(tran.custbody_novdun_excl_dunning_trans), ' ') AS custbody_novdun_excl_dunning_trans
        , COALESCE(TO_CHAR(tran.custbody_po_preuve_paiement), ' ') AS custbody_po_preuve_paiement
        , COALESCE(TO_CHAR(tran.custbody_processing_date), ' ') AS custbody_processing_date
        , COALESCE(TO_CHAR(tran.custbody_psg_ei_content), ' ') AS custbody_psg_ei_content
        , COALESCE(TO_CHAR(tran.custbody_psg_ei_inb_txn_po_valid_bypas), ' ') AS custbody_psg_ei_inb_txn_po_valid_bypas
        , COALESCE(TO_CHAR(tran.custbody_psg_ei_sending_method), ' ') AS custbody_psg_ei_sending_method
        , COALESCE(TO_CHAR(tran.custbody_psg_ei_status), ' ') AS custbody_psg_ei_status
        , COALESCE(TO_CHAR(tran.custbody_psg_ei_template), ' ') AS custbody_psg_ei_template
        , COALESCE(TO_CHAR(tran.custbody_psg_ei_trans_edoc_standard), ' ') AS custbody_psg_ei_trans_edoc_standard
        , COALESCE(TO_CHAR(tran.custbody_so_before_merging), ' ') AS custbody_so_before_merging
        , COALESCE(TO_CHAR(tran.custbody_ste_economic_union), ' ') AS custbody_ste_economic_union
        , COALESCE(TO_CHAR(tran.custbody_ste_rcs_applicable), ' ') AS custbody_ste_rcs_applicable
        , COALESCE(TO_CHAR(tran.custbody_ste_ship_vat_from_country), ' ') AS custbody_ste_ship_vat_from_country
        , COALESCE(TO_CHAR(tran.custbody_ste_transaction_type), ' ') AS custbody_ste_transaction_type
        , COALESCE(TO_CHAR(tran.custbody_ste_use_tax), ' ') AS custbody_ste_use_tax
        , COALESCE(TO_CHAR(tran.custbody_str_nexuscountry), ' ') AS custbody_str_nexuscountry
        , COALESCE(TO_CHAR(tran.custbody_sub_originelle), ' ') AS custbody_sub_originelle
        , COALESCE(TO_CHAR(tran.custbodypaiement_dev_manuel), ' ') AS custbodypaiement_dev_manuel
        , COALESCE(TO_CHAR(tran.custbodyvendor_company_number), ' ') AS custbodyvendor_company_number
        , COALESCE(TO_CHAR(tran.customform), ' ') AS customform
        , COALESCE(TO_CHAR(tran.customtype), ' ') AS customtype
        , COALESCE(TO_CHAR(tran.daysopen), ' ') AS daysopen
        , COALESCE(TO_CHAR(tran.daysoverduesearch), ' ') AS daysoverduesearch
        , COALESCE(TO_CHAR(tran.duedate), ' ') AS duedate
        , COALESCE(TO_CHAR(tran.email), ' ') AS email
        , COALESCE(TO_CHAR(tran.employee), ' ') AS employee
        , COALESCE(TO_CHAR(tran.enddate), ' ') AS enddate
        , COALESCE(TO_CHAR(tran.entity), ' ') AS entity
        , COALESCE(TO_CHAR(tran.entitytaxregnum), ' ') AS entitytaxregnum
        , COALESCE(TO_CHAR(tran.estgrossprofit), ' ') AS estgrossprofit
        , COALESCE(TO_CHAR(tran.estgrossprofitpercent), ' ') AS estgrossprofitpercent
        , COALESCE(TO_CHAR(tran.exchangerate), ' ') AS exchangerate
        , COALESCE(TO_CHAR(tran.externalid), ' ') AS externalid
        , COALESCE(TO_CHAR(tran.fax), ' ') AS fax
        , COALESCE(TO_CHAR(tran.firmed), ' ') AS firmed
        , COALESCE(TO_CHAR(tran.foreignamountpaid), ' ') AS foreignamountpaid
        , COALESCE(TO_CHAR(tran.foreignamountunpaid), ' ') AS foreignamountunpaid
        , COALESCE(TO_CHAR(tran.foreigntotal), ' ') AS foreigntotal
        , COALESCE(TO_CHAR(tran.id), ' ') AS id
        , COALESCE(TO_CHAR(tran.includeinforecast), ' ') AS includeinforecast
        , COALESCE(TO_CHAR(tran.isfinchrg), ' ') AS isfinchrg
        , COALESCE(TO_CHAR(tran.isreversal), ' ') AS isreversal
        , COALESCE(TO_CHAR(tran.lastmodifiedby), ' ') AS lastmodifiedby
        , COALESCE(TO_CHAR(tran.lastmodifieddate), ' ') AS lastmodifieddate
        , COALESCE(TO_CHAR(tran.legacytax), ' ') AS legacytax
        , COALESCE(TO_CHAR(tran.memo), ' ') AS memo
        , COALESCE(TO_CHAR(tran.nextapprover), ' ') AS nextapprover
        , COALESCE(TO_CHAR(tran.nexus), ' ') AS nexus
        , COALESCE(TO_CHAR(tran.number), ' ') AS number
        , COALESCE(TO_CHAR(tran.onetime), ' ') AS onetime
        , COALESCE(TO_CHAR(tran.ordpicked), ' ') AS ordpicked
        , COALESCE(TO_CHAR(tran.otherrefnum), ' ') AS otherrefnum
        , COALESCE(TO_CHAR(tran.paymenthold), ' ') AS paymenthold
        , COALESCE(TO_CHAR(tran.posting), ' ') AS posting
        , COALESCE(TO_CHAR(tran.printedpickingticket), ' ') AS printedpickingticket
        , COALESCE(TO_CHAR(tran.recordtype), ' ') AS recordtype
        , COALESCE(TO_CHAR(tran.recurannually), ' ') AS recurannually
        , COALESCE(TO_CHAR(tran.recurmonthly), ' ') AS recurmonthly
        , COALESCE(TO_CHAR(tran.recurquarterly), ' ') AS recurquarterly
        , COALESCE(TO_CHAR(tran.recurringbill), ' ') AS recurringbill
        , COALESCE(TO_CHAR(tran.recurweekly), ' ') AS recurweekly
        , COALESCE(TO_CHAR(tran.shipcomplete), ' ') AS shipcomplete
        , COALESCE(TO_CHAR(tran.shipdate), ' ') AS shipdate
        , COALESCE(TO_CHAR(tran.shippingaddress), ' ') AS shippingaddress
        , COALESCE(TO_CHAR(tran.source), ' ') AS source
        , COALESCE(TO_CHAR(tran.startdate), ' ') AS startdate
        , COALESCE(TO_CHAR(tran.status), ' ') AS status
        , COALESCE(TO_CHAR(tran.subsidiarytaxregnum), ' ') AS subsidiarytaxregnum
        , COALESCE(TO_CHAR(tran.taxdetailsoverride), ' ') AS taxdetailsoverride
        , COALESCE(TO_CHAR(tran.taxpointdate), ' ') AS taxpointdate
        , COALESCE(TO_CHAR(tran.taxpointdateoverride), ' ') AS taxpointdateoverride
        , COALESCE(TO_CHAR(tran.taxregoverride), ' ') AS taxregoverride
        , COALESCE(TO_CHAR(tran.taxtotal), ' ') AS taxtotal
        , COALESCE(TO_CHAR(tran.terms), ' ') AS terms
        , COALESCE(TO_CHAR(tran.tobeprinted), ' ') AS tobeprinted
        , COALESCE(TO_CHAR(tran.totalaftertaxes), ' ') AS totalaftertaxes
        , COALESCE(TO_CHAR(tran.totalcostestimate), ' ') AS totalcostestimate
        , COALESCE(TO_CHAR(tran.trandate), ' ') AS trandate
        , COALESCE(TO_CHAR(tran.trandisplayname), ' ') AS trandisplayname
        , COALESCE(TO_CHAR(tran.tranid), ' ') AS tranid
        , COALESCE(TO_CHAR(tran.transactionnumber), ' ') AS transactionnumber
        , COALESCE(TO_CHAR(tran.type), ' ') AS type
        , COALESCE(TO_CHAR(tran.typebaseddocumentnumber), ' ') AS typebaseddocumentnumber
        , COALESCE(TO_CHAR(tran.userevenuearrangement), ' ') AS userevenuearrangement
        , COALESCE(TO_CHAR(tran.visibletocustomer), ' ') AS visibletocustomer
        , COALESCE(TO_CHAR(tran.void), ' ') AS void
        , COALESCE(TO_CHAR(tran.voided), ' ') AS voided
        , COALESCE(TO_CHAR(tran.accountbasednumber), ' ') AS accountbasednumber
        , COALESCE(TO_CHAR(tran.custbody_11724_pay_bank_fees), ' ') AS custbody_11724_pay_bank_fees
        , COALESCE(TO_CHAR(tran.custbody_9997_autocash_assertion_field), ' ') AS custbody_9997_autocash_assertion_field
        , COALESCE(TO_CHAR(tran.custbody_9997_is_for_ep_dd), ' ') AS custbody_9997_is_for_ep_dd
        , COALESCE(TO_CHAR(tran.custbody_9997_is_for_ep_eft), ' ') AS custbody_9997_is_for_ep_eft
        , COALESCE(TO_CHAR(tran.custbody_bank_statement), ' ') AS custbody_bank_statement
        , COALESCE(TO_CHAR(tran.custbody_bs_bankstatementtransaction), ' ') AS custbody_bs_bankstatementtransaction
        , COALESCE(TO_CHAR(tran.custbody_eff_nsp2p_xml2nstrans), ' ') AS custbody_eff_nsp2p_xml2nstrans
        , COALESCE(TO_CHAR(tran.custbody_fam_jrn_is_reversal), ' ') AS custbody_fam_jrn_is_reversal
        , COALESCE(TO_CHAR(tran.custbody_fam_lp_financelease), ' ') AS custbody_fam_lp_financelease
        --, COALESCE(TO_CHAR(tran.custbody_ff_sc_b2pmodel), ' ') AS custbody_ff_sc_b2pmodel
        , COALESCE(TO_CHAR(tran.custbody_nov_appro_status), ' ') AS custbody_nov_appro_status
        , COALESCE(TO_CHAR(tran.custbody_nov_commentaires_statut), ' ') AS custbody_nov_commentaires_statut
        , COALESCE(TO_CHAR(tran.custbody_nov_company_number), ' ') AS custbody_nov_company_number
        , COALESCE(TO_CHAR(tran.custbody_nov_createdby_purchase), ' ') AS custbody_nov_createdby_purchase
        , COALESCE(TO_CHAR(tran.custbody_nov_current_approval_level), ' ') AS custbody_nov_current_approval_level
        , COALESCE(TO_CHAR(tran.custbody_nov_infos_internes), ' ') AS custbody_nov_infos_internes
        , COALESCE(TO_CHAR(tran.custbody_nov_montant_po), ' ') AS custbody_nov_montant_po
        , COALESCE(TO_CHAR(tran.custbody_nov_nom_service), ' ') AS custbody_nov_nom_service
        , COALESCE(TO_CHAR(tran.custbody_nov_not_cash_basis), ' ') AS custbody_nov_not_cash_basis
        , COALESCE(TO_CHAR(tran.custbody_nov_po_fact), ' ') AS custbody_nov_po_fact
        , COALESCE(TO_CHAR(tran.custbody_amount_billed_merging), ' ') AS custbody_amount_billed_merging
        , COALESCE(TO_CHAR(tran.custbody_invoice_before_merging), ' ') AS custbody_invoice_before_merging
        , COALESCE(TO_CHAR(tran.custbody_nov_merging), ' ') AS custbody_nov_merging
        , COALESCE(TO_CHAR(tran.custbody_po_amount_before_merging), ' ') AS custbody_po_amount_before_merging
        , COALESCE(TO_CHAR(tran.custbody_po_before_merging), ' ') AS custbody_po_before_merging
        , COALESCE(TO_CHAR(tran.custbody_nov_regle_decompte), ' ') AS custbody_nov_regle_decompte
        , COALESCE(TO_CHAR(tran.custbody_11187_pref_entity_bank), ' ') AS custbody_11187_pref_entity_bank
        , COALESCE(TO_CHAR(tran.custbody_9997_pfa_record), ' ') AS custbody_9997_pfa_record
        , COALESCE(TO_CHAR(tran.custbody_nov_id_affaire), ' ') AS custbody_nov_id_affaire
        , COALESCE(TO_CHAR(tran.custbody_nov_prepayment_amount), ' ') AS custbody_nov_prepayment_amount
        , COALESCE(TO_CHAR(tran.custbody_nov_prepayment_amount_vp), ' ') AS custbody_nov_prepayment_amount_vp
        , COALESCE(TO_CHAR(tran.foreignpaymentamountunused), ' ') AS foreignpaymentamountunused
        , COALESCE(TO_CHAR(tran.foreignpaymentamountused), ' ') AS foreignpaymentamountused
        , COALESCE(TO_CHAR(tran.journaltype), ' ') AS journaltype
        , COALESCE(TO_CHAR(tran.nextbilldate), ' ') AS nextbilldate
        , COALESCE(TO_CHAR(tran.custbody_ff_sc_po_validat_rule_applied), ' ') AS custbody_ff_sc_po_validat_rule_applied
        , COALESCE(TO_CHAR(tran.custbody_nov_po_creator), ' ') AS custbody_nov_po_creator
        , COALESCE(TO_CHAR(tran.custbody_po_montant_ht), ' ') AS custbody_po_montant_ht
        , COALESCE(TO_CHAR(tran.custbody_zc_ai_field_application_data), ' ') AS custbody_zc_ai_field_application_data
        , COALESCE(TO_CHAR(tran.custbody_ff_sc_clickdata), ' ') AS custbody_ff_sc_clickdata
        , COALESCE(TO_CHAR(tran.custbody_ff_sc_field_block_mapping), ' ') AS custbody_ff_sc_field_block_mapping
        , COALESCE(TO_CHAR(tran.custbody_nov_cash_basis_payment), ' ') AS custbody_nov_cash_basis_payment
        , COALESCE(TO_CHAR(tran.custbody_revenue_before_fusion), ' ') AS custbody_revenue_before_fusion
        , COALESCE(TO_CHAR(tran.custbody_so_after_merging), ' ') AS custbody_so_after_merging
        , COALESCE(TO_CHAR(tran.custbody_echeance_traite), ' ') AS custbody_echeance_traite
        , COALESCE(TO_CHAR(tran.custbody_nov_comments_observations), ' ') AS custbody_nov_comments_observations
        , COALESCE(TO_CHAR(tran.intercostatus), ' ') AS intercostatus
        , COALESCE(TO_CHAR(tran.shipcarrier), ' ') AS shipcarrier
        , COALESCE(TO_CHAR(tran.custbody_15889_cust_refund_entity_bank), ' ') AS custbody_15889_cust_refund_entity_bank
        , COALESCE(TO_CHAR(tran.paymentmethod), ' ') AS paymentmethod
        , COALESCE(TO_CHAR(tran.postingperiod), ' ') AS postingperiod
        , COALESCE(TO_CHAR(tran.reversal), ' ') AS reversal
        , COALESCE(TO_CHAR(tran.reversaldate), ' ') AS reversaldate
        , COALESCE(TO_CHAR(tran.reversaldefer), ' ') AS reversaldefer
        , COALESCE(TO_CHAR(tran.memdoc), ' ') AS memdoc
        , COALESCE(TO_CHAR(ap.periodname), ' ') AS period_name
    FROM transaction tran
    LEFT JOIN accountingperiod ap ON tran.postingperiod = ap.id"""

NETSUITE_TRANSACTIONACCOUNTINGLINE_SQL_QUERY = """SELECT
        COALESCE(TO_CHAR(tal.account),' ') AS account
        ,COALESCE(TO_CHAR(tal.accountingbook),' ') AS accountingbook
        ,COALESCE(TO_CHAR(tal.accounttype),' ') AS accounttype
        ,COALESCE(TO_CHAR(tal.amount),' ') AS amount
        ,COALESCE(TO_CHAR(tal.amountlinked),' ') AS amountlinked
        ,COALESCE(TO_CHAR(tal.amountpaid),' ') AS amountpaid
        ,COALESCE(TO_CHAR(tal.amountunpaid),' ') AS amountunpaid
        ,COALESCE(TO_CHAR(tal.credit),' ') AS credit
        ,COALESCE(TO_CHAR(tal.debit),' ') AS debit
        ,COALESCE(TO_CHAR(tal.exchangerate),' ') AS exchangerate
        ,COALESCE(TO_CHAR(tal.lastmodifieddate),' ') AS lastmodifieddate
        ,COALESCE(TO_CHAR(tal.netamount),' ') AS netamount
        ,COALESCE(TO_CHAR(tal.posting),' ') AS posting
        ,COALESCE(TO_CHAR(tal.processedbyrevcommit),' ') AS processedbyrevcommit
        ,COALESCE(TO_CHAR(tal.transaction),' ') AS transaction
        ,COALESCE(TO_CHAR(tal.transactionline),' ') AS transactionline
    FROM transactionaccountingline AS tal
"""

NETSUITE_TRANSACTIONLINE_SQL_QUERY = """SELECT
        COALESCE(TO_CHAR(tranl.accountinglinetype), ' ') AS accountinglinetype
        , COALESCE(TO_CHAR(tranl.actualshipdate), ' ') AS actualshipdate
        , COALESCE(TO_CHAR(tranl.amortizationenddate), ' ') AS amortizationenddate
        , COALESCE(TO_CHAR(tranl.amortizationsched), ' ') AS amortizationsched
        , COALESCE(TO_CHAR(tranl.amortizstartdate), ' ') AS amortizstartdate
        , COALESCE(TO_CHAR(tranl.basegrossamt), ' ') AS basegrossamt
        , COALESCE(TO_CHAR(tranl.basetaxamount), ' ') AS basetaxamount
        , COALESCE(TO_CHAR(tranl.chargetype), ' ') AS chargetype
        , COALESCE(TO_CHAR(tranl.cleared), ' ') AS cleared
        , COALESCE(TO_CHAR(tranl.closedate), ' ') AS closedate
        , COALESCE(TO_CHAR(tranl.commitmentfirm), ' ') AS commitmentfirm
        , COALESCE(TO_CHAR(tranl.costestimate), ' ') AS costestimate
        , COALESCE(TO_CHAR(tranl.costestimaterate), ' ') AS costestimaterate
        , COALESCE(TO_CHAR(tranl.costestimatetype), ' ') AS costestimatetype
        , COALESCE(TO_CHAR(tranl.createdfrom), ' ') AS createdfrom
        , COALESCE(TO_CHAR(tranl.creditforeignamount), ' ') AS creditforeignamount
        , COALESCE(TO_CHAR(tranl.cseg_nov_bus_unit), ' ') AS cseg_nov_bus_unit
        , COALESCE(TO_CHAR(tranl.cseg_nov_cust_cat), ' ') AS cseg_nov_cust_cat
        , COALESCE(TO_CHAR(tranl.cseg_nov_prod_fam), ' ') AS cseg_nov_prod_fam
        , COALESCE(TO_CHAR(tranl.cseg_nov_prod_group), ' ') AS cseg_nov_prod_group
        , COALESCE(TO_CHAR(tranl.cseg_nov_prod_key), ' ') AS cseg_nov_prod_key
        , COALESCE(TO_CHAR(tranl.cseg_nov_prod_type), ' ') AS cseg_nov_prod_type
        , COALESCE(TO_CHAR(tranl.cseg_nov_puk_cat), ' ') AS cseg_nov_puk_cat
        , COALESCE(TO_CHAR(tranl.cseg_nov_rev_categ), ' ') AS cseg_nov_rev_categ
        , COALESCE(TO_CHAR(tranl.cseg_nov_rev_type), ' ') AS cseg_nov_rev_type
        , COALESCE(TO_CHAR(tranl.cseg_nov_subcat), ' ') AS cseg_nov_subcat
        , COALESCE(TO_CHAR(tranl.cseg_nov_type_aff), ' ') AS cseg_nov_type_aff
        , COALESCE(TO_CHAR(tranl.custcol_alf_itemline_id), ' ') AS custcol_alf_itemline_id
        , COALESCE(TO_CHAR(tranl.custcol_atlas_promise_date), ' ') AS custcol_atlas_promise_date
        , COALESCE(TO_CHAR(tranl.custcol_ff_sc_po_id), ' ') AS custcol_ff_sc_po_id
        , COALESCE(TO_CHAR(tranl.custcol_ff_sc_po_line), ' ') AS custcol_ff_sc_po_line
        , COALESCE(TO_CHAR(tranl.custcol_nov_amt_wo_discount_sub_line), ' ') AS custcol_nov_amt_wo_discount_sub_line
        , COALESCE(TO_CHAR(tranl.custcol_nov_date_heure_formation), ' ') AS custcol_nov_date_heure_formation
        , COALESCE(TO_CHAR(tranl.custcol_nov_discount_sub_line), ' ') AS custcol_nov_discount_sub_line
        , COALESCE(TO_CHAR(tranl.custcol_nov_end_date), ' ') AS custcol_nov_end_date
        , COALESCE(TO_CHAR(tranl.custcol_nov_sales_order), ' ') AS custcol_nov_sales_order
        , COALESCE(TO_CHAR(tranl.custcol_nov_so_line_num), ' ') AS custcol_nov_so_line_num
        , COALESCE(TO_CHAR(tranl.custcol_nov_start_date), ' ') AS custcol_nov_start_date
        , COALESCE(TO_CHAR(tranl.custcol_sgk_otc), ' ') AS custcol_sgk_otc
        , COALESCE(TO_CHAR(tranl.debitforeignamount), ' ') AS debitforeignamount
        , COALESCE(TO_CHAR(tranl.documentnumber), ' ') AS documentnumber
        , COALESCE(TO_CHAR(tranl.donotdisplayline), ' ') AS donotdisplayline
        , COALESCE(TO_CHAR(tranl.dropship), ' ') AS dropship
        , COALESCE(TO_CHAR(tranl.eliminate), ' ') AS eliminate
        , COALESCE(TO_CHAR(tranl.entity), ' ') AS entity
        , COALESCE(TO_CHAR(tranl.estgrossprofit), ' ') AS estgrossprofit
        , COALESCE(TO_CHAR(tranl.estgrossprofitpercent), ' ') AS estgrossprofitpercent
        , COALESCE(TO_CHAR(tranl.expectedreceiptdate), ' ') AS expectedreceiptdate
        , COALESCE(TO_CHAR(tranl.expenseaccount), ' ') AS expenseaccount
        , COALESCE(TO_CHAR(tranl.foreignamount), ' ') AS foreignamount
        , COALESCE(TO_CHAR(tranl.foreignamountpaid), ' ') AS foreignamountpaid
        , COALESCE(TO_CHAR(tranl.foreignamountunpaid), ' ') AS foreignamountunpaid
        , COALESCE(TO_CHAR(tranl.fulfillable), ' ') AS fulfillable
        , COALESCE(TO_CHAR(tranl.fxamountlinked), ' ') AS fxamountlinked
        , COALESCE(TO_CHAR(tranl.grossamt), ' ') AS grossamt
        , COALESCE(TO_CHAR(tranl.hasfulfillableitems), ' ') AS hasfulfillableitems
        , COALESCE(TO_CHAR(tranl.id), ' ') AS id
        , COALESCE(TO_CHAR(tranl.isbillable), ' ') AS isbillable
        , COALESCE(TO_CHAR(tranl.isclosed), ' ') AS isclosed
        , COALESCE(TO_CHAR(tranl.iscogs), ' ') AS iscogs
        , COALESCE(TO_CHAR(tranl.iscustomglline), ' ') AS iscustomglline
        , COALESCE(TO_CHAR(tranl.isfullyshipped), ' ') AS isfullyshipped
        , COALESCE(TO_CHAR(tranl.isfxvariance), ' ') AS isfxvariance
        , COALESCE(TO_CHAR(tranl.isinventoryaffecting), ' ') AS isinventoryaffecting
        , COALESCE(TO_CHAR(tranl.isrevrectransaction), ' ') AS isrevrectransaction
        , COALESCE(TO_CHAR(tranl.item), ' ') AS item
        , COALESCE(TO_CHAR(tranl.itemtype), ' ') AS itemtype
        , COALESCE(TO_CHAR(tranl.kitcomponent), ' ') AS kitcomponent
        , COALESCE(TO_CHAR(tranl.linelastmodifieddate), ' ') AS linelastmodifieddate
        , COALESCE(TO_CHAR(tranl.linesequencenumber), ' ') AS linesequencenumber
        , COALESCE(TO_CHAR(tranl.mainline), ' ') AS mainline
        , COALESCE(TO_CHAR(tranl.matchbilltoreceipt), ' ') AS matchbilltoreceipt
        , COALESCE(TO_CHAR(tranl.memo), ' ') AS memo
        , COALESCE(TO_CHAR(tranl.needsrevenueelement), ' ') AS needsrevenueelement
        , COALESCE(TO_CHAR(tranl.netamount), ' ') AS netamount
        , COALESCE(TO_CHAR(tranl.oldcommitmentfirm), ' ') AS oldcommitmentfirm
        , COALESCE(TO_CHAR(tranl.price), ' ') AS price
        , COALESCE(TO_CHAR(tranl.processedbyrevcommit), ' ') AS processedbyrevcommit
        , COALESCE(TO_CHAR(tranl.quantity), ' ') AS quantity
        , COALESCE(TO_CHAR(tranl.quantitybilled), ' ') AS quantitybilled
        , COALESCE(TO_CHAR(tranl.quantityrejected), ' ') AS quantityrejected
        , COALESCE(TO_CHAR(tranl.quantityshiprecv), ' ') AS quantityshiprecv
        , COALESCE(TO_CHAR(tranl.rate), ' ') AS rate
        , COALESCE(TO_CHAR(tranl.rateamount), ' ') AS rateamount
        , COALESCE(TO_CHAR(tranl.ratepercent), ' ') AS ratepercent
        , COALESCE(TO_CHAR(tranl.revenueelement), ' ') AS revenueelement
        , COALESCE(TO_CHAR(tranl.specialorder), ' ') AS specialorder
        , COALESCE(TO_CHAR(tranl.subscription), ' ') AS subscription
        , COALESCE(TO_CHAR(tranl.subscriptionline), ' ') AS subscriptionline
        , COALESCE(TO_CHAR(tranl.subsidiary), ' ') AS subsidiary
        , COALESCE(TO_CHAR(tranl.taxamount), ' ') AS taxamount
        , COALESCE(TO_CHAR(tranl.taxline), ' ') AS taxline
        , COALESCE(TO_CHAR(tranl.transaction), ' ') AS transaction
        , COALESCE(TO_CHAR(tranl.transactiondiscount), ' ') AS transactiondiscount
        , COALESCE(TO_CHAR(tranl.uniquekey), ' ') AS uniquekey
        , COALESCE(TO_CHAR(tranl.custcol_nov_fusion_rev_arr), ' ') AS custcol_nov_fusion_rev_arr
        , COALESCE(TO_CHAR(tranl.billvariancestatus), ' ') AS billvariancestatus
        , COALESCE(TO_CHAR(tranl.commitinventory), ' ') AS commitinventory
        , COALESCE(TO_CHAR(tranl.custcol_zc_3wm_matching_id), ' ') AS custcol_zc_3wm_matching_id
        , COALESCE(TO_CHAR(tranl.foreignpaymentamountunused), ' ') AS foreignpaymentamountunused
        , COALESCE(TO_CHAR(tranl.foreignpaymentamountused), ' ') AS foreignpaymentamountused
        , COALESCE(TO_CHAR(tranl.inventoryreportinglocation), ' ') AS inventoryreportinglocation
        , COALESCE(TO_CHAR(tranl.location), ' ') AS location
        , COALESCE(TO_CHAR(tranl.quantitybackordered), ' ') AS quantitybackordered
        , COALESCE(TO_CHAR(tranl.quantitycommitted), ' ') AS quantitycommitted
        , COALESCE(TO_CHAR(tranl.units), ' ') AS units
        , COALESCE(TO_CHAR(tranl.custcol_alf_item_service_periods), ' ') AS custcol_alf_item_service_periods
        , COALESCE(TO_CHAR(tranl.custcol_nov_merging), ' ') AS custcol_nov_merging
        , COALESCE(TO_CHAR(tranl.billingschedule), ' ') AS billingschedule
        , COALESCE(TO_CHAR(tranl.custcol_nov_revenue_recognition_event), ' ') AS custcol_nov_revenue_recognition_event
        , COALESCE(TO_CHAR(tranl.custcol_nov_scream_id_externe_conso), ' ') AS custcol_nov_scream_id_externe_conso
        , COALESCE(TO_CHAR(tranl.custcol_ff_sc_matching_status), ' ') AS custcol_ff_sc_matching_status
        , COALESCE(TO_CHAR(tranl.custcol_zc_3wm_matching_summary), ' ') AS custcol_zc_3wm_matching_summary
        , COALESCE(TO_CHAR(tranl.custcol_nov_end_customer), ' ') AS custcol_nov_end_customer
        , COALESCE(TO_CHAR(tranl.createdpo), ' ') AS createdpo
        , COALESCE(TO_CHAR(tranl.custcol_po_linked_bill_variance), ' ') AS custcol_po_linked_bill_variance
        , COALESCE(TO_CHAR(tranl.custcol_zc_vendor_txn_idx), ' ') AS custcol_zc_vendor_txn_idx
        , COALESCE(TO_CHAR(tranl.custcol_far_trn_relatedasset), ' ') AS custcol_far_trn_relatedasset
        , COALESCE(TO_CHAR(tranl.custcol_nov_cash_basis), ' ') AS custcol_nov_cash_basis
        , COALESCE(TO_CHAR(tranl.transactionlinetype), ' ') AS transactionlinetype
        , COALESCE(TO_CHAR(tranl.paymentmethod), ' ') AS paymentmethod
        , COALESCE(TO_CHAR(tranl.kitmemberof), ' ') AS kitmemberof
        , COALESCE(TO_CHAR(cnta.name), ' ') AS type_aff
        , COALESCE(TO_CHAR(cnrc.name), ' ') AS rev_categ
    FROM transactionline tranl
    LEFT JOIN customrecord_cseg_nov_type_aff AS cnta ON tranl.cseg_nov_type_aff = cnta.id
    LEFT JOIN customrecord_cseg_nov_rev_categ AS cnrc ON tranl.cseg_nov_rev_categ = cnrc.id"""

NETSUITE_PUK = """SELECT
    COALESCE(TO_CHAR(puk.created), '') AS created
    , COALESCE(TO_CHAR(puk.cseg_nov_bus_unit), '') AS cseg_nov_bus_unit
    , COALESCE(TO_CHAR(puk.cseg_nov_prod_fam), '') AS cseg_nov_prod_fam
    , COALESCE(TO_CHAR(puk.cseg_nov_prod_group), '') AS cseg_nov_prod_group
    , COALESCE(TO_CHAR(puk.cseg_nov_prod_type), '') AS cseg_nov_prod_type
    , COALESCE(TO_CHAR(puk.cseg_nov_puk_cat), '') AS cseg_nov_puk_cat
    , COALESCE(TO_CHAR(puk.cseg_nov_subcat), '') AS cseg_nov_subcat
    , COALESCE(TO_CHAR(puk.externalid), '') AS externalid
    , COALESCE(TO_CHAR(puk.id), '') AS id
    , COALESCE(TO_CHAR(puk.isinactive), '') AS isinactive
    , COALESCE(TO_CHAR(puk.lastmodified), '') AS lastmodified
    , COALESCE(TO_CHAR(puk.lastmodifiedby), '') AS lastmodifiedby
    , COALESCE(TO_CHAR(puk.name), '') AS name
    , COALESCE(TO_CHAR(puk.owner), '') AS owner
    , COALESCE(TO_CHAR(puk.recordid), '') AS recordid
    , COALESCE(TO_CHAR(puk.scriptid), '') AS scriptid
    , COALESCE(TO_CHAR(puk.cseg_nov_division), ' ') AS cseg_nov_division
    , COALESCE(TO_CHAR(puk.cseg_nov_cost_cent), ' ') AS cseg_nov_cost_cent
    , COALESCE(TO_CHAR(cnbu.name), ' ') AS bus_unit
    , COALESCE(TO_CHAR(cnpf.name), ' ') AS prod_fam
    , COALESCE(TO_CHAR(cnpg.name), ' ') AS prod_group
    , COALESCE(TO_CHAR(cnpt.name), ' ') AS prod_type
    , COALESCE(TO_CHAR(cnpc.name), ' ') AS puk_cat
    , COALESCE(TO_CHAR(cns.name), ' ') AS subcat
FROM CUSTOMRECORD_CSEG_NOV_PROD_KEY puk
    -- Custom records CSEG
    LEFT JOIN customrecord_cseg_nov_bus_unit AS cnbu ON puk.cseg_nov_bus_unit = cnbu.id
    LEFT JOIN customrecord_cseg_nov_prod_fam AS cnpf ON puk.cseg_nov_prod_fam = cnpf.id
    LEFT JOIN customrecord_cseg_nov_prod_group AS cnpg ON puk.cseg_nov_prod_group = cnpg.id
    LEFT JOIN customrecord_cseg_nov_prod_type AS cnpt ON puk.cseg_nov_prod_type = cnpt.id
    LEFT JOIN customrecord_cseg_nov_puk_cat AS cnpc ON puk.cseg_nov_puk_cat = cnpc.id
    LEFT JOIN customrecord_cseg_nov_subcat AS cns ON puk.cseg_nov_subcat = cns.id"""

NETSUITE_PUK_SCHEMA = StructType([
    StructField("bus_unit", StringType(), True),
    StructField("created", StringType(), True),
    StructField("cseg_nov_bus_unit", StringType(), True),
    StructField("cseg_nov_prod_fam", StringType(), True),
    StructField("cseg_nov_prod_group", StringType(), True),
    StructField("cseg_nov_prod_type", StringType(), True),
    StructField("cseg_nov_puk_cat", StringType(), True),
    StructField("cseg_nov_subcat", StringType(), True),
    StructField("externalid", StringType(), True),
    StructField("id", StringType(), True),
    StructField("isinactive", StringType(), True),
    StructField("lastmodified", StringType(), True),
    StructField("lastmodifiedby", StringType(), True),
    StructField("links", ArrayType(StringType(), True), True),
    StructField("name", StringType(), True),
    StructField("owner", StringType(), True),
    StructField("prod_fam", StringType(), True),
    StructField("prod_group", StringType(), True),
    StructField("prod_type", StringType(), True),
    StructField("puk_cat", StringType(), True),
    StructField("recordid", StringType(), True),
    StructField("scriptid", StringType(), True),
    StructField("cseg_nov_division", StringType(), True),
    StructField("cseg_nov_cost_cent", StringType(), True),
    StructField("subcat", StringType(), True),
])

TRIBE_CUSTOMER_SCHEMA = StructType([
    StructField("AccountManager", StructType([
        StructField("ID", StringType(), True)
    ]), True),
    StructField("CloseDate", StringType(), True),
    StructField("CreationDate", StringType(), True),
    StructField("Deadline", StringType(), True),
    StructField("DowngradeDate", StringType(), True),
    StructField("End", StringType(), True),
    StructField("FormerDate", StringType(), True),
    StructField("ID", StringType(), True),
    StructField("IsActive", StringType(), True),
    StructField("IsClosed", StringType(), True),
    StructField("IsExclusion", StringType(), True),
    StructField("IsFormer", BooleanType(), True),
    StructField("IsMasterAccount", BooleanType(), True),
    StructField("IsRoot", StringType(), True),
    StructField("LastMutationDate", StringType(), True),
    StructField("MailingOptOut", BooleanType(), True),
    StructField("Name", StringType(), True),
    StructField("NumberOfAttachments", LongType(), True),
    StructField("NumberOfNotes", LongType(), True),
    StructField("Organization", StructType([
        StructField("BIC", StringType(), True),
        StructField("BankName", StringType(), True),
        StructField("Blog", StringType(), True),
        StructField("ChamberOfCommerceName", StringType(), True),
        StructField("ChamberOfCommerceNumber", StringType(), True),
        StructField("CloseDate", StringType(), True),
        StructField("CoCDescription", StringType(), True),  # Extra field from input batch
        StructField("CompanyActivity", StringType(), True),
        StructField("CreationDate", StringType(), True),
        StructField("CreditorNumber", StringType(), True),
        StructField("Deadline", StringType(), True),
        StructField("DebtorNumber", StringType(), True),
        StructField("DiscontinuationDate", StringType(), True),
        StructField("EmailAddress", StringType(), True),
        StructField("End", StringType(), True),
        StructField("EstablishmentDate", StringType(), True),
        StructField("EstablishmentNumber", LongType(), True),
        StructField("ExpirationDate", StringType(), True),
        StructField("Facebook", StringType(), True),
        StructField("FinancialEmailAddress", StringType(), True),
        StructField("FoundingDate", StringType(), True),
        StructField("IBAN", StringType(), True),
        StructField("ID", StringType(), True),
        StructField("Instagram", StringType(), True),
        StructField("IsActive", StringType(), True),
        StructField("IsClosed", StringType(), True),
        StructField("IsExclusion", StringType(), True),
        StructField("IsRoot", StringType(), True),
        StructField("IssueDate", StringType(), True),
        StructField("LastMutationDate", StringType(), True),
        StructField("LinkedIn", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("Number", LongType(), True),
        StructField("NumberOfAttachments", LongType(), True),
        StructField("NumberOfEmployees", LongType(), True),
        StructField("NumberOfNotes", LongType(), True),
        StructField("OIN", StringType(), True),
        StructField("PhoneNumber", StringType(), True),
        StructField("RSIN", LongType(), True),
        StructField("SortIndex", StringType(), True),
        StructField("Start", StringType(), True),
        StructField("Twitter", StringType(), True),
        StructField("VATNumber", StringType(), True),
        StructField("VisitingAddress", StructType([
            StructField("@odata.id", StringType(), True),
            StructField("Country", StructType([
                StructField("@odata.id", StringType(), True),
                StructField("Code", StringType(), True),
                StructField("ID", StringType(), True),
                StructField("Name", StringType(), True),
                StructField("_Name", StringType(), True),
            ]), True),
            StructField("ID", StringType(), True),
            StructField("Postalcode", StringType(), True),
        ]), True),
        StructField("Website", StringType(), True),
        StructField("YouTube", StringType(), True),
        StructField("_Name", StringType(), True),
        StructField("_Type", StringType(), True),
    ]), True),
    StructField("SortIndex", StringType(), True),
    StructField("Start", StringType(), True),
    StructField("UpgradeDate", StringType(), True),
    StructField("_9907b002__20a2__4c92__9499__c183f5f48204", StringType(), True),
    StructField("_Name", StringType(), True),
    StructField("_Type", StringType(), True),
])
