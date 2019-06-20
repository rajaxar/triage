# North Carolina Public Offender Recidivism Prediction

## Information Processing

North Carolina's Department of Public Safety posts "[all public information on all NC Department of Public Safety offenders convicted since 1972](http://webapps6.doc.state.nc.us/opi/downloads.do?method=view)." Inspired by a previous scrape by Fred Whitehurst, Isabella Langan, and Tom Workman, Joe Walsh and Jack Barbey wrote scripts to download the data, save in CSV format, and process into an appropriate format for machine learning.

To get the data, run `./ncdoc_parallel.sh`. It will download and transform the data and store the outputs in the `preprocessed/` directory.

Requirements:
- [bash](https://www.gnu.org/software/bash/)
- [csvkit](https://github.com/wireservice/csvkit)
- [GNU parallel](https://www.gnu.org/software/parallel/)
- [Python](https://www.python.org/downloads/)
- [Numpy](https://docs.scipy.org/doc/numpy-1.15.0/user/install.html)
- [Pandas](https://pandas.pydata.org/pandas-docs/stable/install.html)
- [Jupyter](https://jupyter.org/install)


## Applying Triage

Triage feature generation and predictive modeling for recidivism is performed on data after light manipulation in Python to produce three datasets containing information on specific on inmate sentences, offenses, and disciplinary infractions.

### Files:
- `generate_tables`:
    - `create_recidivism_set_unprocessed.ipynb` - Jupyter notebook which processes the raw data into three datasets for predicting recidivism with triage
    - `create_separated_ables.sql` - SQL queries to create Postgres database tables corresponding to output from `create_recidivism_set_unprocessed.ipynb`.
    - `load_nc_db.py` - CLI utility for uploading `.pkl` outputs from  `create_separated_ables.sql` to Postgres database tables.
- `data`:
    - Preparation
        - `ncdoc_des2csv.sh` - downloads and processes one zip file into a CSV
        - `ncdoc_parallel.sh` - runs `ncdoc_des2csv.sh` in parallel for all the necessary files
        - `fixed_width_definitions_format.csv` - gives necessary data for unzipping files into CSVs
    - Outputs
        - `sentences_table.csv.zip` - High-level on inmate sentences from 1950 to April 2019. Output of `create_recidivism_set_unprocessed.ipynb`.    
        - `offense_counts_table.csv.zip` - Detail on specific offenses related to inmate sentences. Output of `create_recidivism_set_unprocessed.ipynb`.
        - `discipline_table.csv.zip` - Detail on disciplinary infractions occurring during inmate sentences. Output of `create_recidivism_set_unprocessed.ipynb`.
- `triage_configs`:
    - `nc_recid_sep_tables.yaml` - Triage configuration file to run experiments on the three data tables



