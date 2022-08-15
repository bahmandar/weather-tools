#!/usr/bin/env bash
set -eu


#PACKAGE_VERSION=$(python setup.py --version)
#PACKAGE_NAME=$(python setup.py --name)

PROJECT_ID=$(gcloud config list --format 'value(core.project)')

gcloud builds submit . --config=cloudbuildFlex.yaml \
--project="${PROJECT_ID}"

#--substitutions=_PACKAGE_NAME="${PACKAGE_NAME}",_PACKAGE_VERSION="${PACKAGE_VERSION}"

gcloud dataflow flex-template build gs://"${PROJECT_ID}"/templates/flex/weather-mv.json \
--image gcr.io/"${PROJECT_ID}"/dataflow/geobeam:weather-mv-flex \
--sdk-language PYTHON \
--metadata-file metadata.json



