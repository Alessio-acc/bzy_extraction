from typing import Any, Dict, Tuple, Optional, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StructType, StructField, StringType
from google.cloud import storage as gcs_storage
import re


feeding_stg_conf = {
    "gcp_project":        "prj-dpu-prd-dpcc-001",
    "tgt_gcs_bucket":     "edl-fdm-aml-feeding-bzy_ew8_production",
    "tgt_gcs_base_path":  "0/edl-fdm-aml-feeding-bzy-gcs-storage-area",
    "tgt_bq_dataset":     "bqd_dpu_prd_ew8_edl_fdm_aml_feeding_bzy_0_bqd_edl_fdm_aml_feeding_bzy_bigquery_dataset",
}


STG_CATEGORY: Dict[str, str] = {
    # ACCOUNT
    "stg_account_alt_ccy_values":       "ACCOUNT",
    "stg_cards":                        "ACCOUNT",
    "stg_casa":                         "ACCOUNT",
    "stg_correspondent_prod_served":    "ACCOUNT",
    "stg_employee_master":              "ACCOUNT",
    "stg_employee_service_account":     "ACCOUNT",
    "stg_loan_contracts":               "ACCOUNT",
    # CUSTOM
    "stg_unc_csmt_table":               "CUSTOM",
    # CUSTOMERS (NAME SCREENING)
    "stg_address_master":               "CUSTOMERS (NAME SCREENING)",
    "stg_country_master":               "CUSTOMERS (NAME SCREENING)",
    "stg_customer_acct_role_master":    "CUSTOMERS (NAME SCREENING)",
    "stg_customer_country_relation":    "CUSTOMERS (NAME SCREENING)",
    "stg_customer_identifctn_doc":      "CUSTOMERS (NAME SCREENING)",
    "stg_party_account_role_map":       "CUSTOMERS (NAME SCREENING)",
    "stg_party_address_map":            "CUSTOMERS (NAME SCREENING)",
    "stg_party_details":                "CUSTOMERS (NAME SCREENING)",
    "stg_party_email_map":              "CUSTOMERS (NAME SCREENING)",
    "stg_party_master":                 "CUSTOMERS (NAME SCREENING)",
    "stg_party_party_relationship":     "CUSTOMERS (NAME SCREENING)",
    "stg_party_phone_map":              "CUSTOMERS (NAME SCREENING)",
    "stg_party_role_map":               "CUSTOMERS (NAME SCREENING)",
    "stg_party_type_master":            "CUSTOMERS (NAME SCREENING)",
    "stg_party_other_names":            "CUSTOMERS (NAME SCREENING)",
    # ENTERPRISE
    "stg_watchlist_master":             "ENTERPRISE",
    "stg_watchlist_member_entry":       "ENTERPRISE",
    # TRANSACTION
    "stg_cards_payment_txns":           "TRANSACTION",
    "stg_casa_txns":                    "TRANSACTION",
    "stg_front_office_txn_party":       "TRANSACTION",
    "stg_trading_account_txns":         "TRANSACTION",
    "stg_txn_alt_ccy_values":           "TRANSACTION",
}

# Valid GCS environment subfolders
ENV_FOLDER_ALLOWED: set = {"SIT", "UAT", "PROD"}

#Lista colonne party names
STG_OTHER_PARTY_NAME = ["D_VALIDATION_END_DATE", "D_VALIDATION_START_DATE", "FIC_MIS_DATE", "N_PARTY_OTHER_NAME_ID", "V_DATA_ORIGIN", "V_LANGUAGE", "V_LAST_NAME", "V_MIDDLE_NAME", "V_NAME_QUALITY", "V_OTHER_NAME_TYPE", "V_PARTY_FULL_NAME", "V_PARTY_ID", "V_PARTY_NAME", "V_SALUTATION", "V_SUFFIX"]


def get_general_variables(config: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """Returns (gcp_project, tgt_gcs_bucket, tgt_gcs_base_path, tgt_bq_dataset)."""
    return (
        config["gcp_project"],
        config["tgt_gcs_bucket"],
        config["tgt_gcs_base_path"],
        config["tgt_bq_dataset"],
    )


def read_bq_table(
        spark: SparkSession,
        project: str,
        dataset: str,
        stg: str,
) -> DataFrame:
    """
    Reads int_{stg} from BigQuery.
    Args:
        spark: active SparkSession.
        project: GCP project, used as billing/parent project for BigQuery.
        dataset: BigQuery dataset containing the int_stg_* tables.
        stg: STG name, e.g. 'stg_casa'.
    Returns:
        A PySpark DataFrame.
    """
    path = f"{project}.{dataset}.int_{stg}"
    print(f"    [BQ read] {path}")
    return (
        spark.read.format("bigquery")
        .option("table", path)
        .option("parentProject", project)
        .load()
    )

from google.cloud import storage as gcs_storage
import re

def write_csv(
        df: DataFrame,
        gcs_path: str,
        mode: str = "overwrite",
        options: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Writes a single CSV file to an exact GCS path.
    Spark writes to a temp folder, then the part-* file is renamed
    to the exact target path via GCS API, and the temp folder is deleted.
    Default options match the Oracle ingestion format (same as Foundry).
    Any key in `options` overrides the corresponding default.
    """
    supported_modes = {"append", "overwrite", "error", "ignore"}
    if mode not in supported_modes:
        raise Exception(f"Mode {mode} is not supported. Supported modes: {supported_modes}")

    default_options: Dict[str, Any] = {
        "sep":        "|",
        "header":     "true",
        "quoteMode":  "NON_NUMERIC",
        "quote":      '"',
        "multiLine":  "true",
        "emptyValue": "",
    }
    merged_options = {**default_options, **(options or {})}

    tmp_path = gcs_path.replace(".csv", "_tmp/")

    df_writer = (
        df.coalesce(1)
        .write
        .format("csv")
      	.option("escape", "\"")
        .mode(mode)
    )
    for k, v in merged_options.items():
        df_writer = df_writer.option(k, v)

    print(f"Write mode: {mode}, Output path: {gcs_path}")
    df_writer.save(tmp_path)

    # Rename part-* file to exact target path via GCS API
    match = re.match(r"gs://([^/]+)/(.+)", gcs_path)
    bucket_name, blob_path = match.group(1), match.group(2)
    tmp_prefix = re.match(r"gs://[^/]+/(.+)", tmp_path).group(1)

    client = gcs_storage.Client()
    bucket = client.bucket(bucket_name)

    part_blob = next(
        b for b in bucket.list_blobs(prefix=tmp_prefix) if b.name.endswith(".csv")
    )
    bucket.copy_blob(part_blob, bucket, new_name=blob_path)

    for b in bucket.list_blobs(prefix=tmp_prefix):
        b.delete()



def build_gcs_path(
        bucket: str,
        base_path: str,
        stg: str,
        environment: str,
        legal_entity: str,
        country_id: str,
        env_folder: str,
) -> str:
    """
    Builds the full GCS output path for a given STG.

    Pattern:
        gs://{bucket}/{base_path}/Data Extraction/{CATEGORY}/{env_folder}/{tgt_table_name}/ or
        gs://{bucket}/{base_path}/Data Extraction/{CATEGORY}/{env_folder}/bkp/{reference_date}/{tgt_table_name}/

    Example:
        gs://edl-fdm-aml-feeding-baz_ew8_production/0/edl-fdm-aml-feeding-baz-gcs-storage-area/Data Extraction/ACCOUNT/SIT/QA_BAZ_PLB0ML1_CZ_STG_CASA/

    --environment  → controls the table name prefix (QA or BA)
    --env_folder   → controls the GCS subfolder    (SIT, UAT, PROD)
    """
    category = STG_CATEGORY.get(stg)
    if category is None:
        raise ValueError(f"STG '{stg}' has no category mapping in STG_CATEGORY.")

    if env_folder.upper() not in ENV_FOLDER_ALLOWED:
        raise ValueError(
            f"env_folder '{env_folder}' not valid. Allowed: {', '.join(ENV_FOLDER_ALLOWED)}"
        )

    tgt_table_name = (
        f"{environment.upper()}_{legal_entity.upper()}"
        f"_PLB0ML1_{country_id.upper()}_{stg.upper()}"
    )

    base_gcs_path = f"gs://{bucket}/{base_path}/Data Extraction/{category}/{env_folder.upper()}/"

    return f"{base_gcs_path}{tgt_table_name}.csv"


def resolve_fic_mis_date(
        df: DataFrame,
        is_force_active: bool,
        forced_date: str,
) -> DataFrame:

    if is_force_active:
        print(f"    [!] Force mode – FIC_MIS_DATE overridden to {forced_date}")
        df = df.withColumn("FIC_MIS_DATE_aux", F.lit(forced_date).cast(DateType()))
        df = df.withColumn("FIC_MIS_DATE", F.col("FIC_MIS_DATE_aux"))
    else:
        #max_date = df.agg(F.max("FIC_MIS_DATE")).first()[0]
        #print(f"    [i] Max FIC_MIS_DATE = {max_date}")
        #df = df.withColumn("FIC_MIS_DATE_aux", F.lit(max_date).cast(DateType()))
        print(f"    [i] FIC_MIS_DATE_aux = FIC_MIS_DATE")
        df = df.withColumn("FIC_MIS_DATE_aux", F.col("FIC_MIS_DATE"))

    return df


def clean_string_columns(df: DataFrame) -> DataFrame:
    """
    Replaces \\r, \\n and combinations with a single space
    in every StringType column. Numeric and date columns are left untouched.
    """
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(
                col_name,
                F.regexp_replace(F.col(col_name), r"[\r\n]+", " "),
            )
    return df

def clean_stg_specific_columns(df: DataFrame, stg: str) -> DataFrame:
    """
    Applies STG-specific column cleaning rules:
    - stg_casa_txns: removes | from V_ORIGNTR_TO_BENFCRY_INSTRCTNS
    - stg_party_master: removes | from V_NATIONALITY_CITY
    - stg_loan_contracts: removes " from V_ACCT_DISPLAY_NAME
    """
    if stg == "stg_casa_txns":
        df = df.select([
            F.regexp_replace(F.col(c), "\\|", "").alias(c) if c == "V_ORIGNTR_TO_BENFCRY_INSTRCTNS"
            else F.col(c)
            for c in df.columns
        ])
    elif stg == "stg_party_master":
        df = df.select([
            F.regexp_replace(F.col(c), "\\|", "").alias(c) if c == "V_NATIONALITY_CITY"
            else F.col(c)
            for c in df.columns
        ])
    # elif stg == "stg_loan_contracts":
    #     df = df.select([
    #         F.regexp_replace(F.col(c), '"', "").alias(c) if c == "V_ACCT_DISPLAY_NAME"
    #         else F.col(c)
    #         for c in df.columns
    #     ])
    return df

def run_extraction(
        spark: SparkSession,
        project: str,
        dataset: str,
        tgt_bucket: str,
        tgt_base_path: str,
        stg: str,
        environment: str,
        legal_entity: str,
        country_id: str,
        env_folder: str,
        is_force_active: bool,
        forced_date: str,
        ref_dt_amla,
        write_mode: str = "overwrite",
) -> None:
    """
    Full pipeline for a single STG:
        1. Read from BigQuery  (int_{stg})
        2. Resolve FIC_MIS_DATE  (force applies to ALL STGs)
        3. Clean string columns
        4. Write CSV to GCS under {base_path}/Data Extraction/{CATEGORY}/{env_folder}/
    """
    print(f"\n=== {stg.upper()} ===")
    df = None

    if stg != "stg_party_other_names":
        df = read_bq_table(spark, project, dataset, stg)

    if stg in ["stg_party_party_relationship", "stg_customer_country_relation", "stg_party_other_names"]:
        df = add_amla_records(spark, df, stg, ref_dt_amla)

    df = resolve_fic_mis_date(df, is_force_active, forced_date)
    df = clean_string_columns(df)
    df = clean_stg_specific_columns(df, stg)

    gcs_path = build_gcs_path(tgt_bucket, tgt_base_path, stg, environment, legal_entity, country_id, env_folder)
    write_csv(df, gcs_path, mode=write_mode)

    tgt_name = (
        f"{environment.upper()}_{legal_entity.upper()}"
        f"_PLB0ML1_{country_id.upper()}_{stg.upper()}"
    )
    print(f"  [✓] {tgt_name}  →  {gcs_path}")

def add_amla_records(spark: SparkSession, stg_table_name: DataFrame = None, stg: str = None, ref_dt_amla: str = None) -> DataFrame:
    # 1. Crea schema temporaneo tutto StringType
    if stg != "stg_party_other_names":
        schema_str = StructType([StructField(f.name, StringType(), True) for f in stg_table_name.schema.fields])

    print("add_amla_records - START")
    print(f"{stg=}")
    #print(f"{stg_table_name.schema=}")

    # Dati da aggiungere
    #
    amla_customer_country_rel = [
        (ref_dt_amla, "0000000000000044", "CO3", "SK", "F"),
        (ref_dt_amla, "0000000000000076", "CO3", "SK", "F"),
    ]

    amla_party_party_rel = [
        ("2017-10-07", None, ref_dt_amla, None, "N", None, "CO3", None, "0000000000100880", "0000000001001537", "Persons on whose behalf ", "PEP"),
        ("2017-10-07", None, ref_dt_amla, None, "N", None, "CO3", None, "0000000006625278", "0000000001014115", "Persons on whose behalf ", "PEP"),
        ("2017-10-07", None, ref_dt_amla, None, "N", None, "CO3", None, "0000000001431034", "0000000001018402", "Senior Managing Officials", "SMO"),
        ("2017-10-07", None, ref_dt_amla, None, "N", None, "CO3", None, "0000000001482112", "0000000006726911", "Senior Managing Officials", "SMO"),
        ("2017-10-07", None, ref_dt_amla, None, "N", None, "CO3", None, "0000000001028179", "0000000001030593", "Senior Managing Officials", "SMO"),
    ]

    amla_party_other_name = [("9999-12-31", "9999-12-31", ref_dt_amla, "1", "CO3", None, None, None, None, None, None, "0000000000000018", "Špalková", None, None),
                             ("9999-12-31", "9999-12-31", ref_dt_amla, "1", "CO3", None, None, None, None, None, None, "0000000000000041", "Beňa", None, None),
                            ]

    if stg == "stg_customer_country_relation":
        new_rows = amla_customer_country_rel
    elif stg == "stg_party_other_names":
        new_rows = amla_party_other_name
    else:
        new_rows = amla_party_party_rel

        # Crea DataFrame con le nuove righe usando lo stesso schema del df esistente
    if stg == "stg_party_other_names":
        df_new_rows = create_dummy_table_amla(spark, "stg_party_other_names", STG_OTHER_PARTY_NAME, new_rows)
    else:
        df_new_rows = spark.createDataFrame(new_rows, schema=schema_str)


    if stg == "stg_party_party_relationship":
        df_new_rows = df_new_rows \
            .withColumn("D_RELATIONSHIP_EFF_DATE", F.to_date("D_RELATIONSHIP_EFF_DATE", "yyyy-MM-dd")) \
            .withColumn("FIC_MIS_DATE", F.to_date("FIC_MIS_DATE", "yyyy-MM-dd"))
    elif stg == "stg_party_other_names":
        df_new_rows = df_new_rows.withColumn("FIC_MIS_DATE", F.to_date("FIC_MIS_DATE", "yyyy-MM-dd"))
        df_new_rows = df_new_rows.withColumn("D_VALIDATION_END_DATE", F.to_date("D_VALIDATION_END_DATE", "yyyy-MM-dd"))
        df_new_rows = df_new_rows.withColumn("D_VALIDATION_START_DATE", F.to_date("D_VALIDATION_START_DATE", "yyyy-MM-dd"))
    else:
        df_new_rows = df_new_rows.withColumn("FIC_MIS_DATE", F.to_date("FIC_MIS_DATE", "yyyy-MM-dd"))

    # Unisci al DataFrame esistente
    if stg == "stg_party_other_names":
        df = df_new_rows
    else:
        df = stg_table_name.union(df_new_rows)

    return df

def create_dummy_table_amla(spark, tabel_name: str, column_lst: List, new_rows: List) -> DataFrame:
    schema_str = StructType([StructField(f, StringType(), True) for f in column_lst])
    df = spark.createDataFrame(new_rows, schema=schema_str)
    return df
#