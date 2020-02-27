#!/bin/bash

CURATION_DIR="${1}"

if [[ -z "${CURATION_DIR}" ]]; then
  echo "missing argument [CURATION_DIR]"
  exit 1
fi

TEST_DATA_DIR="test_data"
SCHEMA_DIR="${CURATION_DIR}/data_steward/resources/fields"

CDR_PROJECT="aou-res-curation-test"
CDR_DATASET="${CDR_PROJECT}:calbach_prototype"


for site in "nyc" "pitt"; do
  for tbl in "person" "measurement" "condition_occurrence"; do
    bq --project_id "${CDR_PROJECT}" load \
      --skip_leading_rows 1 \
      "${CDR_DATASET}.${site}_${tbl}" \
      "${TEST_DATA_DIR}/${site}/${tbl}.csv" \
      "${SCHEMA_DIR}/${tbl}.json"
  done
done

