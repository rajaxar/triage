import dotenv
import argparse
import logging
import yaml
from yaml import FullLoader
import json
import os
import datetime
from dotenv import load_dotenv, find_dotenv

import logging
import logging.config

from pathlib import Path

cwd = Path.cwd()
log_file_path = os.path.join(cwd, 'logging.ini')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('sLogger')

from triage.component.architect.feature_generators import FeatureGenerator
from triage.experiments import MultiCoreExperiment
from triage.util.db import create_engine

from sqlalchemy.event import listens_for
from sqlalchemy.pool import Pool


def db_connect(db_profile):
    # with open('triage_configs/db_default_profile.json') as f:
    with open(db_profile, 'r') as f:
        DB_CONFIG = json.load(f)
    return DB_CONFIG


def load_features(config_filename):
    # with open('triage_configs/nc_recid_sep_tables.yaml', 'r') as f:
    with open(config_filename, 'r') as f:
        feature_configs = yaml.load(f, Loader=yaml.SafeLoader)
    return feature_configs


def run(config_filename, db_profile, replace=True, predictions=True):
    # load main experiment config

    with open(config_filename, 'r') as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)
    print('Triage configs loaded!')

    logger.info('**********************NEW MODELING RUN**********************')
    logger.debug(config)

    DB_CONFIG = db_connect(db_profile)
    # Lookup the latest match timestamp and insert into the user metadata
    db_engine = create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['pass']}@{DB_CONFIG['host']}/{DB_CONFIG['db']}"
    )
    # db_engine = create_engine(db_url_write)
    print('DB engine created!')

    experiment = MultiCoreExperiment(
        config=config,
        db_engine=db_engine,
        project_path = os.path.join(cwd, 'nc_results/'),
        replace=replace,
        n_db_processes=16,
        n_processes=60,
        save_predictions=predictions,
    )

    experiment.validate()
    experiment.run()

# nc_experiment = run(config_filename = 'triage_configs/nc_recid_sep_tables.yaml')

if __name__ == '__main__':
    class Args():
        pass

    a = Args()
    parser = argparse.ArgumentParser(description="Run triage pipeline")

    parser.add_argument(
            "-c",
            "--config_filename",
            type=str,
            help="Pass the config filename"
        )

    parser.add_argument(
            "-d",
            "--db_profile",
            type=str,
            help="Pass the database configuration filename"
        )

    parser.add_argument(
            "-r",
            "--replace",
            help="If this flag is set, triage will overwrite existing models, matrices, and results",
            action="store_true"
        )

    parser.add_argument(
            "-p",
            "--predictions",
            help="If this flag is set, triage will write predictions to the database",
            action="store_true"
        )


    try:
        args = parser.parse_args(namespace=a)
    except argparse.ArgumentError or argparse.ArgumentTypeError as exc:
        exit("run-triage error: Please review arguments passed: {}".format(
            args, exc.message))
    except Exception as e:
        exit("run-triage error: Please review arguments passed: {}".format(e))

    run(config_filename=a.config_filename, db_profile=a.db_profile,
        replace=a.replace, predictions=a.predictions)
