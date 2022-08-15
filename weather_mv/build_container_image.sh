#!/usr/bin/env bash
set -eu



PROJECT_ID=$(gcloud config list --format 'value(core.project)')

gcloud builds submit . --config=cloudbuildGeobeam.yaml \
--project="${PROJECT_ID}"



