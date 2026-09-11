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

module: "github.com/flink-gcp/flink-connector-gcp/kubernetes@v0"
language: {
	version: "v0.17.1"
}
source: {
	kind: "git"
}
deps: {
	"cue.dev/x/k8s.io@v0": {
		v: "v0.6.0"
	}
}
