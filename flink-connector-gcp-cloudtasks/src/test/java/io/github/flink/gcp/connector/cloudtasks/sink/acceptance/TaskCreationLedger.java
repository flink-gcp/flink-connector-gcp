/*
 * Copyright 2026 The flink-gcp authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.flink.gcp.connector.cloudtasks.sink.acceptance;

import com.google.cloud.tasks.v2.Task;

import java.util.HashMap;
import java.util.Map;

/** Creation evidence is separate from request receipt, successful responses and dispatch counts. */
final class TaskCreationLedger {
    private final Map<String, Generation> generations = new HashMap<>();

    synchronized void created(Task task) {
        require(task.hasCreateTime(), "CreateTask response has no create_time");
        Generation previous = generations.get(task.getName());
        if (previous == null) {
            generations.put(task.getName(), new Generation(task));
        } else if (!previous.task.getCreateTime().equals(task.getCreateTime())) {
            require(previous.recreationExpected, "Unexpected second creation: " + task.getName());
            require(previous.removed, "No observed removal before re-creation");
            require(
                    task.getCreateTime().getSeconds() > previous.task.getCreateTime().getSeconds(),
                    "Re-creation must have a later second-truncated create_time");
            Generation next = new Generation(task);
            next.number = previous.number + 1;
            generations.put(task.getName(), next);
        } else {
            require(!previous.removed, "Successful response reused a removed creation generation");
        }
    }

    synchronized void observed(Task task, boolean fromList) {
        Generation generation = get(task.getName());
        require(
                generation.task.getCreateTime().equals(task.getCreateTime()),
                "Readback differs from the captured creation generation");
        require(!generation.removed, "Removed generation became live again");
        if (fromList) {
            generation.listObserved = true;
        } else {
            generation.getObserved = true;
        }
    }

    synchronized void removed(String name) {
        Generation generation = get(name);
        require(
                generation.getObserved && generation.listObserved,
                "Creation was not observed live");
        generation.removed = true;
    }

    synchronized void expectRecreation(String name) {
        Generation generation = get(name);
        require(generation.removed, "Negative control requires observed removal");
        generation.recreationExpected = true;
    }

    synchronized void assertRecreatedAndObserved(String name) {
        Generation generation = get(name);
        require(generation.number == 2, "Negative control did not detect a second creation");
        require(
                generation.getObserved && generation.listObserved,
                "New generation not observed live");
    }

    synchronized int generation(String name) {
        return get(name).number;
    }

    private Generation get(String name) {
        Generation generation = generations.get(name);
        require(generation != null, "No captured CreateTask response for " + name);
        return generation;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static final class Generation {
        private final Task task;
        private int number = 1;
        private boolean getObserved;
        private boolean listObserved;
        private boolean removed;
        private boolean recreationExpected;

        private Generation(Task task) {
            this.task = task;
        }
    }
}
