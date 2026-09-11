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

import flink "github.com/flink-gcp/flink-connector-gcp/kubernetes/pkg/flink"

delivery: resources: app: flink.#Application & {
	metadata: name: run.id
	spec: {
		image:          run.image
		serviceAccount: "smoke"
		job: jarURI: "local:///opt/flink/usrlib/example.jar"
	}
}
