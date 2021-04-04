#!/bin/bash
set -e

# download the raw file, unzip and process
#
# raw files are fixed width and come with definition files.
# convert those definition files into in2csv format then
# run in2csv to save in csv format.
#
# input: 8-character zipfile
# outputs: file fixed-width definition
#          data in csv format


# create directories if they don't exist
mkdir -p "/mnt/data/projects/nc_prison/data/raw" "/mnt/data/projects/nc_prison/data/preprocessed"

ZIPFILE=$1
FILE_NO_EXTENSION="${ZIPFILE%.zip}"
URL="http://www.doc.state.nc.us/offenders"

# download the file
wget -N \
     -P "/mnt/data/projects/nc_prison/data/raw/" \
     "$URL"/"$ZIPFILE"

# unzip
unzip -o \
      -d "/mnt/data/projects/nc_prison/data/preprocessed/" \
      "/mnt/data/projects/nc_prison/data/raw/$ZIPFILE"

# create schema file
echo 'column,start,length' > "/mnt/data/projects/nc_prison/data/preprocessed/$FILE_NO_EXTENSION"_schema.csv
in2csv -f fixed \
       -s fixed_width_definitions_format.csv \
       "/mnt/data/projects/nc_prison/data/preprocessed/$FILE_NO_EXTENSION".des |
awk '(NR>1)' |
sed -E 's/[ ]{2,}/ /g' |
tr ' ' '_' |
grep -vE "^Name," |
cut -d',' -f2,4-5 >> "/mnt/data/projects/nc_prison/data/preprocessed/$FILE_NO_EXTENSION"_schema.csv

# do the conversion 
in2csv -s "/mnt/data/projects/nc_prison/data/preprocessed/$FILE_NO_EXTENSION"_schema.csv \
       "/mnt/data/projects/nc_prison/data/preprocessed/$FILE_NO_EXTENSION".dat | \
tr -d '?' > "/mnt/data/projects/nc_prison/data/preprocessed/$FILE_NO_EXTENSION".csv

