import os
import yaml
from pyspark.sql import SparkSession

def load_spark_config():
    config_path = os.path.join(os.path.dirname(__file__), "config", "spark_config.yaml")
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f).get("spark", {})
    return {}

def get_lakehouse_spark_session(app_name: str = None):
    """Production Spark session với Delta Lake"""
    spark_cfg = load_spark_config()
    final_app_name = app_name or spark_cfg.get("app_name", "StockFinancialLakehouse")
    
    builder = SparkSession.builder.appName(final_app_name)
    
    # Load basic configs
    configs = spark_cfg.get("configs", {})
    for k, v in configs.items():
        builder = builder.config(k, str(v))
        
    # Load S3 credentials from environment or fallback to defaults
    s3_creds = spark_cfg.get("s3_credentials", {})
    if s3_creds:
        from dotenv import load_dotenv
        load_dotenv()
        endpoint = os.getenv(s3_creds.get("endpoint_env", ""), s3_creds.get("default_endpoint", "http://localhost:9000"))
        access_key = os.getenv(s3_creds.get("access_key_env", ""), s3_creds.get("default_access_key", "minioadmin"))
        secret_key = os.getenv(s3_creds.get("secret_key_env", ""), s3_creds.get("default_secret_key", "minioadmin"))
        
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
        builder = builder.config("spark.hadoop.fs.s3a.access.key", access_key)
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", secret_key)

    # Use delta-spark builder configuration if Delta is used
    try:
        from delta import configure_spark_with_delta_pip
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError:
        spark = builder.getOrCreate()
    
    # Extra configs after session creation
    extras = spark_cfg.get("extra", {})
    for k, v in extras.items():
        spark.conf.set(k, str(v))
    
    return spark

# Global session
