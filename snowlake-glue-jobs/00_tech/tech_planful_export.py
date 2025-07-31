# -*- coding: utf-8 -*-
# %%
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark import SparkConf
import etl_tools
import requests
from pathlib import Path

# import basic auth
from requests.auth import HTTPBasicAuth

# JOB CONTEXT SETUP
args = etl_tools.get_args(env="prod")
env = args["environment"]
conf = SparkConf()

conf = etl_tools.configure_iceberg(
    env=args["environment"],
    db="bronze",
)

sc = SparkContext.getOrCreate(conf=conf)
gc = GlueContext(sc)
job = Job(gc)
spark = gc.spark_session
job.init(args["JOB_NAME"], args)

catalog = spark.conf.get("spark.environment.catalog_name")
database = spark.conf.get("spark.environment.database_name")
# %%

auth = HTTPBasicAuth(
    username="8c74b7b1-0595-408d-a104-e89f6ee6ff4c",
    password="07FBD6E91061B1DE705A4E1609912E75B3597572"
)

# %%

rule_name = "WS_GL Data Load"

rules_gl = requests.get(
    url="https://eustg.planful.com/financemodel/data/rules/gl",
    auth=auth,
)

rule_id = [rule for rule in rules_gl.json() if rule["Name"] == rule_name][0]["DataLoadRuleId"]


print(f"Rule ID for {rule_name}: {rule_id}")

# %%
query = f"""
WITH br AS (
    SELECT
        gl.linenumber,
        SUBSTRING(gl.trandate, 1, 10) AS trandate,
        gl.period,
        YEAR(TO_DATE(gl.period, 'yyyyMM')) AS fiscal_year,
        MONTH(TO_DATE(gl.period, 'yyyyMM')) AS fiscal_month,
        gl.currency,
        gl.debitamount,
        gl.creditamount,
        gl.debitamount - gl.creditamount AS balance,
        gl.currcreditamount,
        gl.currdebitamount,
        REGEXP_REPLACE(REPLACE(gl.description, ',', ''), '[\r\n]', '') AS description,
        gl.module,
        gl.refnumber,
        gl.branch.number AS branch_number,
        gl.account.description AS account_description,
        gl.account.type AS account_type,
        gl.account.number AS account_number,
        gl.subaccount,
        CASE WHEN dep.value = 'nan' THEN NULL ELSE dep.value END AS departement,
        CASE WHEN ven.value = 'nan' THEN NULL ELSE ven.value END AS vendor,
        CASE WHEN pro.value = 'nan' THEN NULL ELSE pro.value END AS product,
        CASE WHEN var.value = 'nan' THEN NULL ELSE var.value END AS variant,
        SUBSTRING(gl.subaccount, 13, 2) AS id_project,
        CASE WHEN rpk.puk = 'nan' THEN NULL ELSE rpk.puk END AS puk,
        CASE WHEN rpk.category = 'nan' THEN NULL ELSE rpk.category END AS ref_puk_category,
        CASE WHEN rpk.subcategory = 'nan' THEN NULL ELSE rpk.subcategory END AS ref_puk_subcategory,
        CASE WHEN rpk.bu = 'nan' THEN NULL ELSE rpk.bu END AS ref_puk_bu,
        CASE WHEN rpk.offer = 'nan' THEN NULL ELSE rpk.offer END AS ref_puk_offer,
        CASE WHEN rpk.product = 'nan' THEN NULL ELSE rpk.product END AS ref_puk_product,
        CASE WHEN rpk.type = 'nan' THEN NULL ELSE rpk.type END AS ref_puk_type,
        CASE WHEN rpk.subtype = 'nan' THEN NULL ELSE rpk.subtype END AS ref_puk_subtype,
        CASE WHEN rpk.division = 'nan' THEN NULL ELSE rpk.division END AS ref_puk_division,
        CASE WHEN rpk.cost_center = 'nan' THEN NULL ELSE rpk.cost_center END AS ref_puk_cost_center
    FROM {catalog}.{env}_snowlake_bronze.visma_generalledgertransactions AS gl
    LEFT JOIN {env}_snowlake_external.mapping_puk_no AS dep
        ON CAST(dep.id AS INT) = CAST(SUBSTRING(gl.subaccount, 1, 3) AS INT)
        AND dep.content = 'departement'
    LEFT JOIN {env}_snowlake_external.mapping_puk_no AS ven
        ON CAST(ven.id AS INT) = CAST(SUBSTRING(gl.subaccount, 4, 3) AS INT)
        AND ven.content = 'vendor'
    LEFT JOIN {env}_snowlake_external.mapping_puk_no AS pro
        ON CAST(pro.id AS INT) = CAST(SUBSTRING(gl.subaccount, 7, 3) AS INT)
        AND pro.content = 'product'
    LEFT JOIN {env}_snowlake_external.mapping_puk_no AS var
        ON CAST(var.id AS INT) = CAST(SUBSTRING(gl.subaccount, 10, 3) AS INT)
        AND var.content = 'variant'
    LEFT JOIN {env}_snowlake_external.ref_puk AS rpk
        ON (
            rpk.puk = pro.puk AND gl.account.number LIKE '3%'
        )
        OR rpk.puk =
            CASE WHEN gl.account.number LIKE '4310' THEN 'DC-40100-00'
                WHEN gl.account.number LIKE '4%' THEN 'DC-40200-00'
                WHEN gl.account.number LIKE '6705' THEN 'HE-99009-03'
                WHEN gl.account.number LIKE '83%' THEN 'HE-99009-05'
                WHEN substr(gl.account.number, 1, 2) IN ('81', '82') THEN 'HE-99009-01'
                WHEN substr(gl.account.number, 1, 1) IN ('6', '7') AND gl.account.number != '6010' THEN pro.puk
                END
    WHERE 1=1
        AND gl.account.number NOT LIKE '89%'
)
SELECT
    account_number AS account
    , departement AS entity
    , product AS product
    , id_project AS costcenter
    , ref_puk_type AS revenue_type
    , fiscal_year AS fiscal_year
    , fiscal_month AS fiscal_month
    , balance AS amount
FROM br
WHERE 1=1
    AND fiscal_year = 2024
    AND fiscal_month < 4
"""

df = spark.sql(query)

df.show()

# %%
df.count()

url = "https://eustg.planful.com/financemodel/data/transferfile"
params = {
    "DataLoadRuleName": "FL GL Data",
    "ColumnDelimiter": ","        # %2C in the curl is just a comma
}

# Same header the curl used: "Authorization: Basic Og=="
headers = {"Authorization": "Basic Og=="}

file1 = Path("planful_export.csv")

with file1.open("rb") as f1:
    files = {
        "file":  (file1.name, f1, "text/csv"),
    }

    resp = requests.post(
        url,
        params=params,          # puts the query string on the URL
        headers=headers,        # basic auth header from curl
        files=files,            # multipart/form-data
        timeout=60
        # allow_redirects=True   # default in requests; matches curl --location
    )

print(resp.status_code)
print(resp.text)   # or resp.json() if the API returns JSON
