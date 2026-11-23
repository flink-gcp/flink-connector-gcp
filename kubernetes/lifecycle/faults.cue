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

// The writers' parallelism, and the TaskManager count, of both applications a recovery trial
// renders, the initial one and the savepoint upgrade, as `flink_tier3/exercise.py` declares it in
// `RENDERED_PARALLELISM`, which a test holds this to. The fault sequence of ADR-0204 patches only
// its rescales away from it.
renderedParallelism: 2
