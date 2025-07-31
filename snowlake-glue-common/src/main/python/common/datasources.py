# -*- coding: utf-8 -*-
import logging
from dataclasses import dataclass, fields, field
from typing import List
from typing import TypeVar, Generic
from table_tools import WriteMode
import constants
from datetime import datetime
import pyspark.sql.functions as F
import snowlake_api_tools
import etl_tools
from functools import reduce
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql.types import StructType


logger = logging.getLogger(__name__)


@dataclass
class Datasource:
    source_app: str
    primary_keys: List[str] = field(default_factory=list)
    source_table_name: str = ""
    source_table_alias: str = ""
    partition_columns: List[str] = field(default_factory=list)
    order_columns: List[str] = field(default_factory=list)
    datetime_column: str = None
    created_datetime_column: str = None
    write_mode: str | dict[str:str] = None
    schema: StructType = None

    @property
    def target_table_name(self):
        name = self.source_table_alias if self.source_table_alias else self.source_table_name
        return f"{self.source_app}_{name}".lower()

    def __post_init__(self):
        if self.source_table_alias == "":
            self.source_table_alias = self.source_table_name


T = TypeVar('T')


@dataclass
class DatasourcesList(Generic[T]):

    @classmethod
    def get(cls, *names) -> List[T]:

        if not names:
            return [f.default for f in fields(cls)]

        else:
            return [f.default for f in fields(cls) if f.default.source_table_alias in names or f.default.source_table_name in names]

    @classmethod
    def get_all(cls) -> List[T]:
        return [f.default for f in fields(cls)]

    @classmethod
    def get_one(cls, name: str) -> T:
        try:
            datasource_names = [f.default for f in fields(cls) if f.default.source_table_alias == name]
            if datasource_names:
                return datasource_names[0]
            else:
                raise ValueError(f"{name} does not exists in {T.__name__} class, available sources are {[f.default.source_table_alias for f in fields(cls)]}")
        except Exception as e:
            logger.error(e)
            return []

    @classmethod
    def get_multiple(cls, names: List[str]) -> List[T]:
        try:
            return [f.default for f in fields(cls) if f.default.source_table_alias in names]
        except Exception as e:
            logger.error(e)
            return []


@dataclass
class APISource(Datasource):
    """
    A source is a way to declare an object in the code representing infos of a specific source table in a specific format (API, SQL, JDBC, JSON, ...)
    We consider an API source type as in the form : https://<api_endpoint>/<url_prefix>/<source_table_name>/<url_suffix>
    Example :
    With the endoint https://api.focus.no/webkuareadapi/api/export/companies in the source app webkua
    APISource(
        source_table_name = "companies",
        url_prefix = "export",
        primary_keys = ["companyNumber"],  # Refer to your admin to know id columns
        source_app = "webkua"
    )

    Example with webkua companies source table:
        For the source table 'companies' is in the source app 'webkua' the url_path it will be in the form : export/companies
        because the webkua API provides infos of table companies at the endpoint : https://api.focus.no/webkuareadapi/api/export/companies

    You can create your own Source by naming it MySource.
    For example an SQLSource would be:
    @dataclass SQLSource:
        url_
    """

    api_url: str = ""
    url_suffix: str = ""
    explode_arrays: bool = False

    @property
    def url_path(self):
        url_path = f"{self.api_url}/{self.source_table_name}"

        if self.url_suffix != "":
            url_path = f"{url_path}{self.url_suffix}"

        return url_path


@dataclass
class EMSSource(Datasource):
    source_app: str = "ems"


@dataclass
class SalesforceSource(Datasource):
    source_app: str = "salesforce"


@dataclass
class WebkuaSource(APISource):
    source_app: str = "webkua"
    api_url: str = "https://api.focus.no/reportdata/v1/api/export"


@dataclass
class VismaSource(APISource):
    source_app: str = "visma"
    api_url: str = "https://api.focus.no/reportdata/v1"
    page_size: int = 1000

    def prepare_filters(self, date):
        source_filters = {
            "period": date.strftime("%Y%m")
        }

        filters = [F.col(filter) >= F.lit(value) for filter, value in source_filters.items()]
        target_filters = reduce(lambda a, b: a & b, filters)

        return source_filters, target_filters

    def collect_pks(self, source_filters):
        now = datetime.now()
        params = {
            "pageNumber": "1",
            "ledger": "1",
            "fromPeriod": source_filters["period"],
            "toPeriod": now.strftime("%Y%m")
        }
        connector = snowlake_api_tools.FocusApiConnector()
        data = connector.get(self.url_path, params=params)

        logger.info(f"Get total number of pages for {self.source_table_alias}..")

        total_lines = data[0]["metadata"]["totalCount"]
        page_size = data[0]["metadata"]["maxPageSize"]
        number_of_pages = (total_lines // page_size) + 1

        page_range = range(2, number_of_pages + 1)
        rows = []

        def fetch(page):

            logger.info(f"Fetching page {page} for {self.source_table_alias}")

            params["pageNumber"] = page

            data = connector.get(self.url_path, params=params)
            rows.extend(data)

        with ThreadPoolExecutor(max_workers=60) as executor:
            list(executor.map(fetch, page_range))

        return rows


@dataclass
class SuperofficeSource(APISource):
    source_app: str = "superoffice"
    api_url: str = "https://api.focus.no/reportdata/v1/api/v1"
    updated_date: str = ""
    write_mode: str = WriteMode.SCD2
    page_size: int = 100
    columns: List[str] = field(default_factory=list)


@dataclass
class TribeSource(APISource):  # Inherit from APISource
    source_app: str = "tribe"
    api_url: str = "https://api.tribecrm.nl/v1/odata"
    explode_columns: List[str] = None
    flatten_columns: List[str] = None
    updated_date: str = "LastMutationDate"
    created_date: str = "CreationDate"
    params: dict = field(default_factory=dict)
    _dt_max: str = "2000-01-01T00:00:00"
    page_size: int = 100
    write_mode: str = WriteMode.SCD2

    @property
    def dt_max(self):
        return self._dt_max

    @dt_max.setter
    def dt_max(self, value):
        self._dt_max = value
        self.params["$filter"] = self.params["$filter"].replace("{dt_max}", self.dt_max)


@dataclass
class AfasSource(APISource):
    source_app: str = "afas"
    api_url: str = "https://80746.rest.afas.online/ProfitRestServices/connectors"
    params: dict = field(default_factory=dict)
    page_size: int = 100

    def prepare_filters(self, date):
        source_filters = {
            "Jaar": str(date.year),
            "Periode": str(date.month)
        }

        filters = [F.col(filter) >= F.lit(value) for filter, value in source_filters.items()]
        target_filters = reduce(lambda a, b: a & b, filters)

        return source_filters, target_filters

    def collect_pks(self, source_filters):
        connector = snowlake_api_tools.AfasApiConnector()
        rows = []
        print(source_filters)
        print(source_filters.items())
        date = ",".join(list(source_filters.keys()))
        values = ",".join(list(source_filters.values()))
        q = {
                "filterfieldids": date,
                "filtervalues": values,
                "orderbyfieldids": date,
                "operatortypes": "2,2"
        }
        pagination_strategy = snowlake_api_tools.OffsetPagination(
            page_size=self.page_size,
            limit_param="take",
            offset_param="skip"
        )

        results_generator = connector.fetch_pages_lazy(
            url=self.url_path,
            pagination_strategy=pagination_strategy,
            data_key="rows",
            params=q,
            source_name=self.source_table_alias,
        )

        for data in results_generator:
            rows.extend([[item[k] for k in self.primary_keys] for item in data])

        return rows


@dataclass
class NetsuiteSource(APISource):
    source_app: str = "netsuite"
    api_url: str = "https://8009688.suitetalk.api.netsuite.com/services/rest"
    params: dict = field(default_factory=dict)
    page_size: int = 1000
    write_mode: str = WriteMode.APPEND
    sql_query: str = ""
    query_table_alias: str = ""
    has_pagination: bool = True
    missing_ids: List[str] = field(default_factory=list)
    remote_ids: List[str] = field(default_factory=list)
    current_ids: List[str] = field(default_factory=list)
    deleted_ids: List[str] = field(default_factory=list)
    remote_table_name: str = ""

    def __post_init__(self):
        if not self.remote_table_name:
            self.remote_table_name = self.source_table_alias or self.source_table_name

    def get_sql_query(self, where_clause="", order_by_clause="", join_clause=""):
        return f"""
        {self.sql_query}
        {join_clause}
        {where_clause}
        {order_by_clause}
    """

    def get_record_count(self):
        record_count_data = snowlake_api_tools.NetsuiteApiConnector().get(
            url=self.url_path,
            json={"q": f"SELECT COUNT(*) FROM {self.remote_table_name}"}
        )

        logger.info(record_count_data)

        if not record_count_data["items"]:
            raise ValueError(f"Error while fetching record count for {self.source_table_alias} - No data found in response - {record_count_data}")
        if "expr1" not in record_count_data["items"][0]:
            raise ValueError(f"Error while fetching record count for {self.source_table_alias} - No 'expr1' key found in response - {record_count_data}")

        logger.info(f"Record count for {self.source_table_alias} - {record_count_data}")
        return int(record_count_data["items"][0]['expr1'])

    def build_condition(self, id_cols, last_ids, id_operator):
        conditions = []
        for j in range(len(id_cols)):
            parts = [
                f"TO_NUMBER({id_cols[i]}) = {last_ids[i]}" for i in range(j)
            ]

            parts.append(f"TO_NUMBER({id_cols[j]}) {id_operator} {last_ids[j]}")

            conditions.append(f"({' AND '.join(parts)})")

        or_conditions = """
            OR """.join(conditions)

        return f"""
        WHERE (
          {or_conditions}
        )
        """

    def get_where_clause(
        self,
        last_ids: tuple,
        last_date: datetime = None,
        id_operator: str = ">",
        date_operator: str = ">="
    ):

        if self.query_table_alias:
            id_cols = [f"{self.query_table_alias}.{primary_key}" for primary_key in self.primary_keys]
            date_str = f"{self.query_table_alias}.{self.datetime_column}"
        else:
            id_cols = self.primary_keys
            date_str = self.datetime_column

        condition = self.build_condition(id_cols, last_ids, id_operator)

        if self.datetime_column and last_date:

            if type(last_date) is datetime:
                last_date = last_date.strftime('%d/%m/%Y')

            condition += f"""
        OR TO_DATE({date_str}) {date_operator} '{last_date}'"""

        print(condition)
        return condition

    def get_order_by_clause(self):
        if self.query_table_alias:
            id_str = [f"{self.query_table_alias}.{primary_key}" for primary_key in self.primary_keys]
        else:
            id_str = self.primary_keys

        condition = f"""
        ORDER BY {", ".join([f"TO_NUMBER({id})" for id in id_str])} ASC
        """
        print(condition)
        return condition

    def get_last_date(self, df):

        if self.datetime_column:

            if self.write_mode == WriteMode.SCD2:
                df = df.where("_is_active")

            result_df = df.select(*self.primary_keys, self.datetime_column)

            result_df = result_df.select(
                            F.max(F.to_date(self.datetime_column, "dd/MM/yyyy")).alias("max_date")
                        )

            last_date: datetime = result_df.collect()[0][0]

            if last_date:
                print(last_date.strftime('%d/%m/%Y'))
                return last_date.strftime('%d/%m/%Y')
            else:
                return None
        else:
            return None

    def get_last_ids(self, df):

        if self.write_mode == WriteMode.SCD2:
            df = df.where("_is_active")

        ordered_df = df.orderBy(*[F.col(c).cast("int").asc() for c in self.primary_keys])

        last_row = ordered_df.tail(1)[0]
        last_cursor = tuple(int(last_row[c]) for c in self.primary_keys)
        return last_cursor

    def get_remote_last_id(self):
        q = f"""SELECT
                  TOP 1
                  {self.primary_keys[0]}
                  FROM {self.source_table_alias}
                  ORDER BY TO_NUMBER({self.primary_keys[0]}) DESC"""

        response = snowlake_api_tools.NetsuiteApiConnector().get(
            url=self.url_path,
            json={"q": q},
            data_key="items",
        )

        return response["items"][0][self.primary_keys[0]]

    def get_remote_last_date(self):
        q = f"""SELECT
                  TOP 1
                  {self.datetime_column}
                  FROM {self.source_table_alias}
                  ORDER BY TO_DATE({self.datetime_column}) DESC"""

        response = snowlake_api_tools.NetsuiteApiConnector().get(
            url=self.url_path,
            json={"q": q},
            data_key="items",
        )
        print(response)

        return response["items"][0][self.datetime_column]

    def initialize_ids_status(self, target_df):
        """
        Initialize the ids status for the current table
        set the ids status to the class attributes:
        - remote_ids: ids in remote (api table)
        - current_ids: ids in current table (target table)
        - missing_ids: ids in remote but not in current table
        - deleted_ids: ids in current table but not in remote

        :param target_df: The target dataframe
        :return: None
        """

        rows = (
            target_df.select(F.col(self.primary_keys[0]))
            .where(f"{self.primary_keys[0]} IS NOT NULL")
            .distinct()
            .orderBy(self.primary_keys[0])
            .collect()
        )

        current_ids = [row[self.primary_keys[0]] for row in rows]

        remote_ids = []

        pagination_strategy = snowlake_api_tools.OffsetPagination(
            page_size=self.page_size,
            limit_param="limit",
            offset_param="offset",
        )

        # (Re)Connect
        connector = snowlake_api_tools.NetsuiteApiConnector()

        ids = ", ".join(pk for pk in self.primary_keys)
        ordering = ", ".join(f"TO_NUMBER({pk})" for pk in self.primary_keys)

        q = f"SELECT {ids} FROM {self.source_table_alias} ORDER BY {ordering} ASC"

        id_count = self.get_record_count()

        has_more_ids = True
        batch_id = 1

        logger.info(f"{self.source_table_alias} - Source API has {id_count} records")

        while has_more_ids and len(remote_ids) < id_count:

            # (Re)Connect
            connector = snowlake_api_tools.NetsuiteApiConnector()

            results_generator = connector.fetch_pages_lazy(
                url=self.url_path,
                pagination_strategy=pagination_strategy,
                json={"q": q},
                data_key="items",
                max_workers=25,
                source_name=self.source_table_alias,
            )

            for data in results_generator:
                remote_ids.extend([row[self.primary_keys[0]] for row in data])

                logger.info(f"{len(remote_ids) / id_count * 100: .2f}% - Batch id {batch_id} - {self.source_table_alias} - "
                            f"Fetched {len(remote_ids)} remote ids from remote API")
                batch_id += 1

            if len(remote_ids) >= id_count:
                has_more_ids = False
                logger.info(f"Batch id {batch_id} - {self.source_table_alias} - "
                            "Latest batch completed all records.. Stopping ingestion ! ")

        # ids in remote but not in spark
        missing_ids = sorted(list(set(remote_ids) - set(current_ids)))

        # ids in spark but not in remote
        deleted_ids = sorted(list(set(current_ids) - set(remote_ids)))

        self.remote_ids = remote_ids
        self.current_ids = current_ids
        self.missing_ids = missing_ids
        self.deleted_ids = deleted_ids

        logger.info(f"Found {len(missing_ids)} missing ids ids in target table - current id count = {len(current_ids)} / remote id count = {len(remote_ids)} / deleted id count = {len(deleted_ids)} - missing ids example {missing_ids[:10]}")

    def prepare_filters(self, date):
        source_filters = {
           self.datetime_column: date.strftime("%d/%m/%Y"),
        }

        filters = [F.to_date(F.col(filter), "%d/%m/%Y") >= F.to_date(F.lit(value)) for filter, value in source_filters.items()]
        target_filters = reduce(lambda a, b: a & b, filters)

        return source_filters, target_filters

    def collect_pks(self, source_filters):
        has_more = True
        print(source_filters)
        where_clause = " AND ".join([f"TO_DATE({key}) >= '{value}'" for key, value in source_filters.items()])
        rows = []
        q = f"""
            SELECT {",".join(self.primary_keys)}
            FROM {self.target_table_name.removeprefix("netsuite_")}
            WHERE {where_clause}
        """
        print(q)
        connector = snowlake_api_tools.NetsuiteApiConnector()
        pagination_strategy = snowlake_api_tools.OffsetPagination(
            page_size=self.page_size,
            limit_param="limit",
            offset_param="offset",
        )
        while has_more:
            results_generator = connector.fetch_pages_lazy(
                url=self.url_path,
                pagination_strategy=pagination_strategy,
                json={"q": q},
                max_workers=100,
                source_name=self.source_table_alias,
            )
            for data in results_generator:
                rows.extend([[[row[k]] for k in self.primary_keys] for row in data[0]["items"]])

                if any(d["hasMore"] is False for d in data):
                    has_more = False
        # rows = [(item,) for sublist in rows for inner in sublist for item in inner] if len(rows[0]) > 0 else []
        return rows

    def get_missing_ids_where_clause(self):

        if self.query_table_alias:
            id_str = f"{self.query_table_alias}.{self.primary_keys[0]}"
        else:
            id_str = self.primary_keys[0]

        condition = [f"{id_str}='{id}'" for id in self.missing_ids]
        condition = " OR ".join(condition)

        where_clause = f"WHERE {condition}"

        logger.info(f"Table {self.source_table_alias} - Missing ids where clause = {where_clause[:15]}...")

        return where_clause

    def check_missing_ids(self, target_df):

        record_count = self.get_record_count()

        active_count = self.get_active_count(target_df)

        last_id = int(self.get_last_id(target_df))
        remote_last_id = int(self.get_remote_last_id())

        logger.info(f"Table {self.source_table_alias} - Target last id = {last_id} - Remote last id = {remote_last_id} - "
                    f"Target Active count = {active_count} - Remote Record count = {record_count}")

        self.initialize_ids_status(target_df)

        if self.missing_ids:
            logger.info(f"Table {self.source_table_alias} - Found {len(self.missing_ids)} missing ids to fetch {self.missing_ids}")

            self.missing_ids = self.missing_ids

            return True
        else:
            logger.info(f"Table {self.source_table_alias} - No missing ids to fetch")

            return False

    def get_active_count(self, df):

        result_df = df.where(~F.col(self.primary_keys[0]).isin(self.deleted_ids))

        if self.write_mode == WriteMode.SCD2:
            return result_df.where("_is_active").count()
        else:
            return result_df.count()


@dataclass
class WebkuaDataSources(DatasourcesList[WebkuaSource]):
    """
    You can create your own data source by naming it 'MyDataSources' where each field is a source table from this data source
    For example in netsuite we would do something like:
    @dataclass NetsuiteDataSources:
        source_app = "netsuite"

        DECLARANT: VismaSource = WebkuaSource(...)

        EXPLOITANT: WebkuaSource = WebkuaSource(...)
        ...
    """
    COMPANIES: WebkuaSource = WebkuaSource(source_table_name="companies", primary_keys=["companyNumber"])
    CONTRACTITEMINFOS: WebkuaSource = WebkuaSource(source_table_name="contractiteminfos", primary_keys=["id"])
    CONTRACTITEMS: WebkuaSource = WebkuaSource(source_table_name="contractitems", primary_keys=["id", "contractItemInfoId", "licenseNumber", "createdDate"])
    CONTRACTS: WebkuaSource = WebkuaSource(source_table_name="contracts", primary_keys=["contractNumber", "startDate", "nextRenewalDate"])
    COURSECATALOGS: WebkuaSource = WebkuaSource(source_table_name="courseCatalogs", primary_keys=["id"])
    COURSEREGISTRATION: WebkuaSource = WebkuaSource(source_table_name="courseRegistrations", primary_keys=["courseId", "email", "createdAt"])
    COURSES: WebkuaSource = WebkuaSource(source_table_name="courses", primary_keys=["id"])
    LICENSES: WebkuaSource = WebkuaSource(source_table_name="licenses", primary_keys=["id"])
    PRODUCTGROUPS: WebkuaSource = WebkuaSource(source_table_name="productgroups", primary_keys=["id"])
    PRODUCTS: WebkuaSource = WebkuaSource(source_table_name="products", primary_keys=["id"])
    USERACCOUNTS: WebkuaSource = WebkuaSource(source_table_name="useraccounts", primary_keys=["email", "companyId", "phone", "companyNumber"])
    SALESORDERS: WebkuaSource = WebkuaSource(source_table_name="orders/sales", primary_keys=["id", "companyNumber", "contractNumber", "licenseNumber", "contractItemInfoId"], source_table_alias="salesorders")
    RENEWALS: WebkuaSource = WebkuaSource(source_table_name="orders/renewals", source_table_alias="salesorders_renewals")


@dataclass
class VismaDataSources(DatasourcesList[VismaSource]):

    CUSTOMERINVOICE: VismaSource = VismaSource(
        source_table_name="customerinvoice",
        primary_keys=[
            "referenceNumber",
            "paymentReference",
            "origInvoiceDate",
            "customer_number",
            "account_number",
            "invoiceLines_inventoryNumber",
            "invoiceLines_lineNumber",
            "invoiceLines_subaccount_subaccountNumber",
            "invoiceLines_subaccount_segments_segmentId",
        ],
        explode_arrays=True
    )

    GENERALLEDGERTRANSACTION: VismaSource = VismaSource(
        source_table_name="GeneralLedgerTransactions",
        primary_keys=[
            "lineNumber",
            "refNumber",
            "batchNumber"
        ]
    )

    SALESORDER: VismaSource = VismaSource(
        source_table_name="salesorder",
        primary_keys=[
            "orderNo",
            "lines_lineNbr"
        ],
        explode_arrays=True
    )

    INVENTORY: VismaSource = VismaSource(source_table_name="inventory", primary_keys=["inventoryId"])

    SALESORDER: VismaSource = VismaSource(
        source_table_name="salesorder",
        primary_keys=[
            "orderNo",
            "lines_lineNbr"
        ],
        explode_arrays=True
    )


@dataclass
class SuperofficeDataSources(DatasourcesList[SuperofficeSource]):
    CONTACT: SuperofficeSource = SuperofficeSource(
        source_table_name="Contact",
        primary_keys=["PrimaryKey"],
        updated_date="updatedDate",
        source_table_alias="contact",
    )
    PROJECT: SuperofficeSource = SuperofficeSource(
        source_table_name="project",
        primary_keys=["PrimaryKey"],
        updated_date="updatedDate",
        page_size=1000
    )
    SALE: SuperofficeSource = SuperofficeSource(
        source_table_name="sale",
        primary_keys=["PrimaryKey"],
        updated_date="updatedDate",
        write_mode=WriteMode.UPSERT,
        page_size=500,
        columns=constants.TRIBE_SALE_COLUMNS
    )
    PERSON: SuperofficeSource = SuperofficeSource(
        source_table_name="person",
        primary_keys=["PrimaryKey"],
        updated_date="personUpdatedDate",
        page_size=100,
        columns=constants.TRIBE_PERSON_COLUMNS
    )
    # STATUSES: SuperofficeSource = SuperofficeSource(source_table_name="MDOList", primary_keys=["PrimaryKey"], max_date_column="updatedDate", source_table_alias="statuses")


@dataclass
class TribeDataSources(DatasourcesList[TribeSource]):

    CONTACTS: TribeSource = TribeSource(
        source_table_name="Relationship_Person_Contact_Standard",
        primary_keys=["ID"],
        params={
            "$expand": "Person($select=ID),Relation($select=ID)",
            "$filter": "(CreationDate ge {dt_max}+01:00) or (LastMutationDate ge {dt_max}+01:00)",
        },
        source_table_alias="contact"
    )

    CUSTOMER: TribeSource = TribeSource(
        source_table_name="Relationship_Organization_CommercialRelationship",
        primary_keys=["ID"],
        params={
            "$expand": "AccountManager($select=ID),Organization($expand=VisitingAddress($select=Postalcode;$expand=Country($select=_Name,Name,Code)))",
            "$filter": "(CreationDate ge {dt_max}+01:00) or (LastMutationDate ge {dt_max}+01:00)"
        },
        source_table_alias="customer",
        schema=constants.TRIBE_CUSTOMER_SCHEMA
    )

    EMPLOYEE: TribeSource = TribeSource(
        source_table_name="Relationship_Person_Contact_Employee",
        primary_keys=["ID"],
        params={
            "$expand": "Person($select=ID),Relation($select=ID)",
            "$filter": "(CreationDate ge {dt_max}+01:00) or (LastMutationDate ge {dt_max}+01:00)",
        },
        source_table_alias="employee"
    )

    OFFER: TribeSource = TribeSource(
        source_table_name="Activity_Offer",
        primary_keys=["ID", "ProductLines_ID"],
        params={
            "$expand": "Phase($select=ID),Relationship($select=ID),ProductLines($select=ID)",
            "$filter": "(CreationDate ge {dt_max}+01:00) or (LastMutationDate ge {dt_max}+01:00)"
        },
        explode_columns=["ProductLines"],
        flatten_columns=["ProductLines"],
        source_table_alias="offer"
    )

    OPPORTUNITY: TribeSource = TribeSource(
        source_table_name="Activity_SalesOpportunity",
        primary_keys=["ID", "ProductLines_ID"],
        params={
            "$expand": "Phase($select=ID),Relationship($select=ID),ProductLines($select=ID),_88f5ae58__5464__4f8c__b13c__75fed7a73a65($select=_Name),a4d81060__844e__4399__9142__2d960441a391($select=_Name),SalesRepresentative($select=ID,Name,Description)",
            "$filter": "(CreationDate ge {dt_max}+01:00) or (LastMutationDate ge {dt_max}+01:00)"
        },
        explode_columns=["ProductLines"],
        flatten_columns=["ProductLines"],
        source_table_alias="opportunity"
    )

    PHASES: TribeSource = TribeSource(
        source_table_name="Datastore_Phase_ActivitySalesOpportunity",
        primary_keys=["ID"],
        params={
            "$filter": "(CreationDate ge {dt_max}+01:00) or (LastMutationDate ge {dt_max}+01:00)"
        },
        source_table_alias="phase"
    )

    PRODUCT_LINE: TribeSource = TribeSource(
        source_table_name="ProductLine",
        primary_keys=["ID"],
        params={
            "$expand": "Product($select=ID)",
            "$filter": "(CreationDate ge {dt_max}+01:00) or (LastMutationDate ge {dt_max}+01:00)"
        },
        source_table_alias="product_line"
    )

    PRODUCT: TribeSource = TribeSource(
        source_table_name="Product",
        primary_keys=["ID"],
        params={
            "$expand": "Unit,ProductGroup,RepeatInterval",
            "$filter": "(CreationDate ge {dt_max}+01:00) or (LastMutationDate ge {dt_max}+01:00)"
        },
        source_table_alias="product"
    )


@dataclass
class EMSDataSources(DatasourcesList[EMSSource]):
    EMS_ENTITLEMENTUSAGE: EMSSource = EMSSource(
        source_table_name="ems_entitlementusage",
        primary_keys=[
            "customer_id",
            "entitlement_id",
            "product_key",
            "product_name",
            "source_ip_address",
            "start_date_time",
            "end_date_time",
            "identityname",
            "nomsociete"
        ],
        partition_columns=["_year_usage"],
        order_columns=["_year_usage"],
        write_mode={
            "bronze": WriteMode.OVERWRITE_PARTITIONS,
            "silver": WriteMode.UPSERT,
            "gold": WriteMode.APPEND
        }
    )


@dataclass
class AfasDataSources(DatasourcesList[AfasSource]):

    CONTRACT: AfasSource = AfasSource(
        source_table_name="JMAN_Contract_Data",
        primary_keys=[
            "factuurnummer",
            "itemcode",
            "nummer_abonnement",
            "abonnementsregel",
            "project",
            "projectgroep",
        ],
        source_table_alias="contract",
        page_size=10000,
    )

    CUSTOMER: AfasSource = AfasSource(
        source_table_name="JMAN_Customer_Data",
        primary_keys=["Nummer_debiteur"],
        source_table_alias="customer"
    )

    GENERALLEDGERDATA: AfasSource = AfasSource(
        source_table_name="JMAN_GeneralLedger_Data",
        source_table_alias="generalledgerdata",
        primary_keys=[
            "Nr",
            "Zoek",
            "Admin_rek_courant",  # 1
            "Nr_journaalpost",  # 2
            "Jaar",  # 3
            "Periode",  # 4
            "Administratie",  # 5
            "Administratie2",  # 6
            "Boekstukdatum",  # 7
            "Datum_boeking",  # 8
            "Rekeningnr",  # 9
            "Grootboekrekening",  # 10
            "Debiteur_crediteur",  # 11
            "Boekstuknummer",  # 19
            "Verbijzonderingsas_1",  # 12
            "Verbijzonderingsas_2",  # 13
            "Factuurnummer",  # 20
            "Dagboek",  # 22
            "Itemcode",  # 24
            "Volgnummer",  # 26
            "Abonnementsregel",  # 35
        ],
        order_columns=[
            "Admin_rek_courant",  # 1
            "Nr_journaalpost",  # 2
            "Jaar",  # 3
            "Periode",  # 4
            "Administratie",  # 5
            "Administratie2",  # 6
            "Boekstukdatum",  # 7
            "Datum_boeking",  # 8
            "Rekeningnr",  # 9
            "Grootboekrekening",  # 10
            "Debiteur_crediteur",  # 11
            "Boekstuknummer",  # 19
            "Factuurnummer",  # 20
            "Itemcode",  # 24
            "Volgnummer",  # 26
            "Abonnementsregel",  # 35
        ],
        datetime_column=["Jaar", "Periode"],
        page_size=4054
    )

    HOURREPORTDATA: AfasSource = AfasSource(
        source_table_name="JMAN_HourReport_Data",
        primary_keys=[
            # "Medewerker",
            # "Datum",
            # "Datum_2",
            # "Aantal",
            # "Verkooprelatie",
            # "Factuurnummer",
            # "Itemcode"
            "Regelnummer"
        ],
        page_size=4166
    )

    MRR: AfasSource = AfasSource(
        source_table_name="JMAN_MRR_Data",
        page_size=5000,
        datetime_column="VoucherDate",
        primary_keys=[
            "Nummer_abonnement",
            "Abonnementsregel",
            "Volgnummer_journaalpost",
            "Nummer_journaalpost",
            "Code_verbijzonderingsas_2",
            "Volgnummer_verkoopfactuurregel",
            "AccountNo",
            "InvoiceId",
            "VoucherNo",
            "DimAx1",
            "Code_dagboek",
            "Administratie",
            "EntryDate",
            "VoucherDate",
            "Btw-code"
        ],
        source_table_alias="mrr",
        order_columns=["Administratie", "Year", "Period", "EntryDate", "VoucherDate", "VoucherNo", "AccountNo", "InvoiceId"]
    )

    PROFIT_JOURNALS: AfasSource = AfasSource(
        source_table_name="Profit_Journals",
        datetime_column="Modified",
        primary_keys=["UnitId", "JournalId", "Description", "JournalType"],
        source_table_alias="profit_journals",
        order_columns=["UnitId", "JournalId"]
    )

    PROFIT_FINANCIELE_MUTATIES: AfasSource = AfasSource(
        source_table_name="Profit_Financiele_Mutaties",
        page_size=5000,
        datetime_column="Modified",
        primary_keys=["UnitId", "EntryNo", "SeqNo", "JournalId", "AccountNo", "InvoiceId", "VoucherNo", "VatAmt", "Modified", "DimAx1", "DimAx2", "AmtDebit", "Collect_On", "Type", "Description", "Klic_Debiteurnr", "Klic_Ordernr", "Klic_Artikelnr"],
        source_table_alias="profit_financiele_mutaties",
        order_columns=["UnitId", "EntryNo", "SeqNo", "JournalId", "AccountNo", "InvoiceId", "VoucherNo", "Modified"]
    )


@dataclass
class NetsuiteDataSources(DatasourcesList[NetsuiteSource]):

    ACCOUNT: NetsuiteSource = NetsuiteSource(
        source_table_name="query/v1/suiteql",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        source_table_alias="account",
        order_columns=["id"],
        query_table_alias="acc",
        write_mode=WriteMode.SCD2,
        sql_query=constants.NETSUITE_ACCOUNT_QUERY,
    )

    ACCOUNTINGPERIOD: NetsuiteSource = NetsuiteSource(
        source_table_name="query/v1/suiteql",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        order_columns=["id"],
        source_table_alias="accountingperiod",
        query_table_alias="accp",
        write_mode=WriteMode.SCD2,
        sql_query=constants.NETSUITE_ACCOUNTINGPERIOD_SQL_QUERY,
    )

    CHARGE: NetsuiteSource = NetsuiteSource(
        source_table_name="query/v1/suiteql",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        order_columns=["id"],
        source_table_alias="charge",
        query_table_alias="cha",
        write_mode=WriteMode.UPSERT,
        sql_query=constants.NETSUITE_CHARGE_SQL_QUERY,
    )

    CONSOLIDATEDEXCHANGERATE: NetsuiteSource = NetsuiteSource(
        source_table_name="query/v1/suiteql",
        primary_keys=["id"],
        order_columns=["id"],
        source_table_alias="consolidatedexchangerate",
        query_table_alias="cer",
        write_mode=WriteMode.SCD2,
        sql_query=constants.NETSUITE_CONSOLIDATEDEXCHANGERATE_SQL_QUERY,
    )

    CUSTOMER: NetsuiteSource = NetsuiteSource(
        source_table_name="query/v1/suiteql",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        order_columns=["id"],
        source_table_alias="customer",
        query_table_alias="cus",
        write_mode=WriteMode.SCD2,
        sql_query=constants.NETSUITE_CUSTOMER_SQL_QUERY,
    )

    EMPLOYEE: NetsuiteSource = NetsuiteSource(
        source_table_name="query/v1/suiteql",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        order_columns=["id"],
        source_table_alias="employee",
        query_table_alias="emp",
        write_mode=WriteMode.SCD2,
        sql_query=constants.NETSUITE_EMPLOYEE_SQL_QUERY,
    )

    ENTITY: NetsuiteSource = NetsuiteSource(
        source_table_name="query/v1/suiteql",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        order_columns=["id"],
        source_table_alias="entity",
        query_table_alias="ent",
        write_mode=WriteMode.SCD2,
        sql_query=constants.NETSUITE_ENTITY_SQL_QUERY,
    )

    ITEM: NetsuiteSource = NetsuiteSource(
        source_table_name="query/v1/suiteql",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        order_columns=["id"],
        source_table_alias="item",
        query_table_alias="itm",
        write_mode=WriteMode.SCD2,
        sql_query=constants.NETSUITE_ITEM_SQL_QUERY,
    )

    PUK: NetsuiteSource = NetsuiteSource(
        source_table_name="query/v1/suiteql",
        primary_keys=["id"],
        datetime_column="lastmodified",
        order_columns=["id"],
        source_table_alias="puk",
        query_table_alias="puk",
        remote_table_name="customrecord_cseg_nov_prod_key",
        write_mode=WriteMode.SCD2,
        sql_query=constants.NETSUITE_PUK,
        schema=constants.NETSUITE_PUK_SCHEMA,
    )

    REVENUEELEMENT: NetsuiteSource = NetsuiteSource(
        source_table_name="query/v1/suiteql",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        order_columns=["id"],
        source_table_alias="revenueelement",
        query_table_alias="reve",
        write_mode=WriteMode.UPSERT,
        sql_query=constants.NETSUITE_REVENUEELEMENT_SQL_QUERY,
    )

    REVENUEPLAN: NetsuiteSource = NetsuiteSource(
        source_table_name="query/v1/suiteql",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        order_columns=["id"],
        source_table_alias="revenueplan",
        query_table_alias="revp",
        write_mode=WriteMode.UPSERT,
        sql_query=constants.NETSUITE_REVENUEPLAN_SQL_QUERY,
    )

    REVENUEPLANPLANNEDREVENUE: NetsuiteSource = NetsuiteSource(
        source_table_name="query/v1/suiteql",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        order_columns=["id"],
        source_table_alias="revenueplanplannedrevenue",
        query_table_alias="rppr",
        write_mode=WriteMode.UPSERT,
        sql_query=constants.NETSUITE_REVENUEPLANPLANNEDREVENUE_SQL_QUERY,
    )

    SUBSCRIPTION: NetsuiteSource = NetsuiteSource(
        source_table_name="query/v1/suiteql",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        order_columns=["id"],
        source_table_alias="subscription",
        query_table_alias="sub",
        write_mode=WriteMode.UPSERT,
        sql_query=constants.NETSUITE_SUBSCRIPTION_SQL_QUERY,
    )

    SUBSCRIPTIONLINE: NetsuiteSource = NetsuiteSource(
        source_table_name="query/v1/suiteql",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        order_columns=["id"],
        source_table_alias="subscriptionline",
        query_table_alias="subl",
        write_mode=WriteMode.UPSERT,
        sql_query=constants.NETSUITE_SUBSCRIPTIONLINE_SQL_QUERY,
    )

    SUBSIDIARY: NetsuiteSource = NetsuiteSource(
        source_table_name="query/v1/suiteql",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        order_columns=["id"],
        source_table_alias="subsidiary",
        query_table_alias="sub",
        write_mode=WriteMode.SCD2,
        sql_query=constants.NETSUITE_SUBSIDIARY_SQL_QUERY,
    )

    TRANSACTION: NetsuiteSource = NetsuiteSource(
        source_table_name="query/v1/suiteql",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        created_datetime_column="createddate",
        order_columns=["id"],
        source_table_alias="transaction",
        query_table_alias="tran",
        write_mode=WriteMode.UPSERT,
        sql_query=constants.NETSUITE_TRANSACTION_SQL_QUERY,
    )

    TRANSACTIONACCOUNTINGLINE: NetsuiteSource = NetsuiteSource(
        source_table_name="query/v1/suiteql",
        primary_keys=["transaction", "transactionline"],
        datetime_column="lastmodifieddate",
        order_columns=["transaction", "transactionline"],
        source_table_alias="transactionaccountingline",
        query_table_alias="tal",
        write_mode=WriteMode.UPSERT,
        sql_query=constants.NETSUITE_TRANSACTIONACCOUNTINGLINE_SQL_QUERY,
    )

    TRANSACTIONLINE: NetsuiteSource = NetsuiteSource(
        source_table_name="query/v1/suiteql",
        primary_keys=["uniquekey"],
        datetime_column="linelastmodifieddate",
        order_columns=["uniquekey"],
        source_table_alias="transactionline",
        query_table_alias="tranl",
        write_mode=WriteMode.UPSERT,
        sql_query=constants.NETSUITE_TRANSACTIONLINE_SQL_QUERY,
    )


@dataclass
class SalesforceDataSources(DatasourcesList[SalesforceSource]):
    ACCOUNT: SalesforceSource = SalesforceSource(
        source_table_name="account",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        write_mode=WriteMode.SCD2,
    )

    CAMPAIGN: SalesforceSource = SalesforceSource(
        source_table_name="campaign",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        write_mode=WriteMode.SCD2,
    )

    OPPORTUNITY: SalesforceSource = SalesforceSource(
        source_table_name="opportunity",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        write_mode=WriteMode.SCD2,
    )

    OPPORTUNITYLINEITEM: SalesforceSource = SalesforceSource(
        source_table_name="opportunitylineitem",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        write_mode=WriteMode.SCD2,
    )

    OPPORTUNITYSPLIT: SalesforceSource = SalesforceSource(
        source_table_name="opportunitysplit",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        write_mode=WriteMode.SCD2,
    )

    OPPORTUNITYTEAMMEMBER: SalesforceSource = SalesforceSource(
        source_table_name="opportunityteammember",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        write_mode=WriteMode.SCD2,
    )

    PRODUCT: SalesforceSource = SalesforceSource(
        source_table_name="product2",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        write_mode=WriteMode.SCD2,
    )

    QUOTE: SalesforceSource = SalesforceSource(
        source_table_name="quote",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        write_mode=WriteMode.SCD2,
    )

    QUOTELINEITEM: SalesforceSource = SalesforceSource(
        source_table_name="quotelineitem",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        write_mode=WriteMode.SCD2,
    )

    REFERENTIALINFORMATION: SalesforceSource = SalesforceSource(
        source_table_name="referentialinformation__c",
        primary_keys=["id"],
        datetime_column="lastmodifieddate",
        write_mode=WriteMode.SCD2,
    )
