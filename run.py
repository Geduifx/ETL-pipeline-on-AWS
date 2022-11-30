"""Running the ETL application"""

# Logging packages
import logging
import logging.config
import yaml

def main():
    """
        entry point to run the ETL job.
    """
    # Parsing YAML file
    config = r'C:\Users\gedim\Desktop\Programming\Data Engineering\ETL pipelines\Project\ETL-pipeline-on-AWS\configs\xetra_report1_config.yml'
    config = yaml.safe_load(open(config))
    # configure logging
    log_config = config['logging']
    # Load config as a dictionary.
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("This is a test.")


if __name__ == '__main__':
    main()