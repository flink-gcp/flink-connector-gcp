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

// The fault kinds every recovery trial names, as `flink_tier3/exercise.py` declares them in its
// `FAULTS` table, which a test holds this to: the direction a rescale moves the writers, or null,
// and the writers' parallelism of each phase, one per single-slot TaskManager. A rescale moves it
// between one and two, inside the three-Pod quota of the BigQuery and Cloud Tasks namespaces, and
// every other kind keeps two.
// A scenario indexes its sizes by kind rather than choosing them by comparing it: compared, a
// kind outside the four rendered some fixed size instead of failing.
faultKinds: {
	"jm-replacement": {rescale: null, initial: 2, upgrade: 2}
	"tm-replacement": {rescale: null, initial: 2, upgrade: 2}
	"rescale-out": {rescale: "out", initial: 1, upgrade: 2}
	"rescale-in": {rescale: "in", initial: 2, upgrade: 1}
}
// The kind of a trial that names none; a BigQuery or Cloud Tasks trial of it runs the upgrade and
// then the JobManager failover.
defaultFault: "jm-replacement"
