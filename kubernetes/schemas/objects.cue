// Copyright 2026 The flink-gcp authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schemas

import (
	core "cue.dev/x/k8s.io/api/core/v1"
	apps "cue.dev/x/k8s.io/api/apps/v1"
	batch "cue.dev/x/k8s.io/api/batch/v1"
	flink "github.com/flink-gcp/flink-connector-gcp/kubernetes/gen/flink/v1beta1"
)

#Object: {
	metadata!: name!: string
} & (
	{apiVersion: "v1", kind: "ConfigMap", core.#ConfigMap} |
	{apiVersion: "v1", kind: "Service", core.#Service} |
	{apiVersion: "apps/v1", kind: "Deployment", apps.#Deployment} |
	{apiVersion: "batch/v1", kind: "Job", batch.#Job} |
	{apiVersion: "flink.apache.org/v1beta1", kind: "FlinkDeployment", flink.#FlinkDeployment})
