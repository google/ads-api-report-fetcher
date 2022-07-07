#!/bin/bash
#
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#------------------------------------------------------------------------------ 

WORKFLOW_NAME=gaarf-wf

while :; do
    case $1 in
  -n|--name)
      shift
      WORKFLOW_NAME=$1
      ;;
  *)
      break
    esac
  shift
done

gcloud workflows deploy $WORKFLOW_NAME --source workflows.yaml
