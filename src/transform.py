import hydra
from omegaconf import DictConfig
from pyspark.sql import SparkSession

from src.utils import *

@hydra.main(config_path=get_cwd(), config_name="config")
def main(cfg: DictConfig) -> None:

    spark = SparkSession.builder.getOrCreate()

    all_data_paths = get_datasets_paths(cfg.data_repositories.release_output_root, cfg.datasets)

    for data_path in all_data_paths:
        data = spark.read.parquet(data_path)
        # Convert snake case to camel case
        data = camel_case_col_names(data)
        # Add more transformations here...
        # Write to parquet
        write_subset_to_parquet(data, cfg.data_repositories.transformed_data_root, data_path, cfg.subset_size)

    spark.stop()

if __name__ == '__main__':
    main()