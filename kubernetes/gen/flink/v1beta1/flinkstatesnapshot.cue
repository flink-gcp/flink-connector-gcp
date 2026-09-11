// Generated from the Apache Flink Kubernetes Operator 1.15.0 chart.
// Regenerate with just tier3-schemas refresh; do not edit by hand.
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1beta1

#FlinkStateSnapshot: {
	_embeddedResource
	spec?: {
		backoffLimit?: int
		checkpoint?: {}
		jobReference?: {
			kind?: "FlinkDeployment" | "FlinkSessionJob"
			name?: string
		}
		savepoint?: {
			alreadyExists?:   bool
			disposeOnDelete?: bool
			formatType?:      "CANONICAL" | "NATIVE" | "UNKNOWN"
			path?:            string
		}
	}
	status?: {
		error?:            string
		failures?:         int
		path?:             string
		resultTimestamp?:  string
		state?:            "ABANDONED" | "COMPLETED" | "FAILED" | "IN_PROGRESS" | "TRIGGER_PENDING"
		triggerId?:        string
		triggerTimestamp?: string
	}

	_embeddedResource: {
		apiVersion!: string
		kind!:       string
		metadata?: {
			...
		}
	}
	apiVersion: "flink.apache.org/v1beta1"
	kind:       "FlinkStateSnapshot"
	metadata!: {
		name!:      string
		namespace!: string
		labels?: [string]:      string
		annotations?: [string]: string
		...
	}
}
