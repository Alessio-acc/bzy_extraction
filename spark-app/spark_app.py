from pyspark.sql import SparkSession
import argparse
import utils.utils as utils

parser = argparse.ArgumentParser()
parser.add_argument('--list_stg',        default="")             # e.g. "stg_casa,stg_party_master"
parser.add_argument('--is_force_active', default="false")        # "true" to force FIC_MIS_DATE on all STGs
parser.add_argument('--forced_date',     default="2026-02-28")   # date to force, format YYYY-MM-DD
parser.add_argument('--environment',     default="QA")           # table name prefix: "QA" for SIT and UAT, "BA" for PROD
parser.add_argument('--env_folder',      default="SIT")          # GCS subfolder: "SIT", "UAT", "PROD"
parser.add_argument('--ref_dt_amla',     default="")
parser.add_argument('--legal_entity',    default="BZY")          # e.g. "BZY"
parser.add_argument('--country_id',      default="SK")           # e.g. "SK"
parser.add_argument('--write_mode',      default="overwrite")    # CSV write mode: "overwrite", "append", "error", "ignore"
args = parser.parse_args()

list_stg        = [p.strip() for p in args.list_stg.split(",") if p.strip()]
is_force_active = args.is_force_active.strip().lower() == "true"
forced_date     = args.forced_date.strip()
environment     = args.environment.strip().upper()
env_folder      = args.env_folder.strip().upper()
ref_dt_amla     = args.ref_dt_amla.strip()
legal_entity    = args.legal_entity.strip().upper()
country_id      = args.country_id.strip().upper()
write_mode      = args.write_mode.strip().lower()

config = utils.feeding_stg_conf
project, tgt_bucket, tgt_base_path, tgt_bq_dataset = utils.get_general_variables(config)

print("gcp_project    :", project)
print("tgt_bucket     :", tgt_bucket)
print("tgt_base_path  :", tgt_base_path)
print("tgt_bq_dataset :", tgt_bq_dataset)
print("environment    :", environment)
print("env_folder     :", env_folder)
print("ref_dt_amla    :", ref_dt_amla)
print("legal_entity   :", legal_entity)
print("country_id     :", country_id)
print("is_force_active:", is_force_active)
print("forced_date    :", forced_date)
print("write_mode     :", write_mode)
print("list_stg       :", list_stg)

spark = (
    SparkSession.builder
    .appName("extraction-to-oracle")
    .config("spark.sql.catalog.hive_internal", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hive_internal.type", "hive")
    .config("spark.sql.defaultCatalog", "hive_internal")
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.scheduler.pool", "default")
    .getOrCreate()
)

for stg in list_stg:
    if stg in utils.STG_CATEGORY:
        utils.run_extraction(
            spark=spark,
            project=project,
            dataset=tgt_bq_dataset,
            tgt_bucket=tgt_bucket,
            tgt_base_path=tgt_base_path,
            stg=stg,
            environment=environment,
            legal_entity=legal_entity,
            country_id=country_id,
            env_folder=env_folder,
            ref_dt_amla=ref_dt_amla,
            is_force_active=is_force_active,
            forced_date=forced_date,
            write_mode=write_mode,
        )
    else:
        raise Exception(f"✗ Stg {stg} not supported. Use one of the following: {', '.join(utils.STG_CATEGORY.keys())}.")

spark.stop()
#