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

package tier3

import pubsub "github.com/flink-gcp/flink-connector-gcp/kubernetes/pkg/pubsub"

run: namespace: "tier3-pubsub"
let inputs = run
let application = pubsub.#Application & {
	run: {
		id:    inputs.id
		image: inputs.image
		phase: inputs.phase
	}
	// Forwarded only when given, so that a leaf without one meets the
	// package's own default, or its lack of one.
	if inputs.recordsPerSubscription != _|_ {run: recordsPerSubscription: inputs.recordsPerSubscription}
	if inputs.parallelism != _|_ {run: parallelism: inputs.parallelism}
}

delivery: resources: app: application.resource
