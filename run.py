"""Running the ETL application"""

# Logging packages
import logging
import logging.config

import yaml

import argparse

from source_code.common.s3 import S3BucketConnector
from source_code.transformers.transformer import ETL, SourceConfig, TargetConfig


def main():
    """
        entry point to run the ETL job.
    """
    # Parsing YAML file

    ########### NEW, 3 lines
    # parser = argparse.ArgumentParser(description='Run the Xetra ETL Job.')
    # parser.add_argument('config', help='A configuration file in YAML format.')
    # args = parser.parse_args()

    ########### OLD, 2 lines
    config = r'C:\Users\gedim\Desktop\Programming\Data Engineering\ETL pipelines\Project\ETL-pipeline-on-AWS\configs\xetra_report1_config.yml'
    config = yaml.safe_load(open(config))

    ########### NEW, 1 line
    # config = yaml.safe_load(open(args.config))

    # configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    # reading s3 configuration
    s3_config = config['s3']
    # creating the S3BucketConnector classes for source and target
    s3_bucket_src = S3BucketConnector(access_key=s3_config['access_key'],
                                      secret_key=s3_config['secret_key'],
                                      endpoint_url=s3_config['src_endpoint_url'],
                                      bucket=s3_config['src_bucket'])
    s3_bucket_trg = S3BucketConnector(access_key=s3_config['access_key'],
                                      secret_key=s3_config['secret_key'],
                                      endpoint_url=s3_config['trg_endpoint_url'],
                                      bucket=s3_config['trg_bucket'])
    # reading source configuration
    # ** allows to submit dictionaries as keyword arguments
    source_config = SourceConfig(**config['source'])
    # reading target configuration
    target_config = TargetConfig(**config['target'])
    # reading meta file configuration
    meta_config = config['meta']
    # creating ETL class
    logger = logging.getLogger(__name__)
    logger.info('ETL job started.')
    etl = ETL(s3_bucket_src, s3_bucket_trg,
                         meta_config['meta_key'], source_config, target_config)
    # running etl job for xetra report 1
    etl.etl_report1()
    logger.info('ETL job finished.')


if __name__ == '__main__':
    main()