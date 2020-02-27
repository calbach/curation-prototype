#!/bin/bash

TEST_DATA_DIR="test_data"

CDR_PROJECT="aou-res-curation-test"
CDR_DATASET="${CDR_PROJECT}.calbach_prototype"

for site in "nyc" "pitt"; do
  for tbl in "person" "measurement" "condition_occurrence"; do
    bq \
      --format=json \
      --project_id "${CDR_PROJECT}" \
      query \
      --nouse_legacy_sql \
      "SELECT * FROM \`${CDR_DATASET}.${site}_${tbl}\`" \
      | jq -c ".[]" \
      > "test_data/${site}/${tbl}.json"
  done
done

