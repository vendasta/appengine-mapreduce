#!/bin/bash

REPO_NAME="appengine-mapreduce"
TRIGGER_NAME="appengine-mapreduce-master"
TRIGGER_DESCRIPTION="Appengine Mapreduce - Master"

gcloud builds triggers delete $TRIGGER_NAME --project=repcore-prod

gcloud builds triggers create github \
    --repo-owner="vendasta" \
    --repo-name=$REPO_NAME \
    --branch-pattern=^master$ \
    --included-files="python/**" \
    --build-config="python/cloudbuild-master.yaml" \
    --project=repcore-prod \
    --name=$TRIGGER_NAME \
    --description="$TRIGGER_DESCRIPTION" \


TRIGGER_NAME="appengine-mapreduce-branches"
TRIGGER_DESCRIPTION="Appengine Mapreduce - Branches"

gcloud builds triggers delete $TRIGGER_NAME --project=repcore-prod

gcloud builds triggers create github \
    --repo-owner="vendasta" \
    --repo-name=$REPO_NAME \
    --branch-pattern=^master$ \
    --included-files="python/**" \
    --build-config="python/cloudbuild-branches.yaml" \
    --project=repcore-prod \
    --name=$TRIGGER_NAME \
    --description="$TRIGGER_DESCRIPTION" \
