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

package io.github.flink.gcp.connector.firestore;

import org.apache.flink.runtime.metrics.MetricNames;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class FirestoreMetricNamesTest {

    @Test
    void noNameTakesFlinksNumPrefixOrCollidesWithAFlinkName() throws Exception {
        Set<String> flinkNames = new HashSet<>();
        for (Field field : MetricNames.class.getFields()) {
            if (field.getType() == String.class) {
                flinkNames.add((String) field.get(null));
            }
        }
        List<String> names = new ArrayList<>();
        for (Field field : FirestoreMetricNames.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class) {
                names.add((String) field.get(null));
            }
        }

        assertThat(names).isNotEmpty().doesNotHaveDuplicates();
        assertThat(names).noneMatch(name -> name.startsWith("num"));
        assertThat(names).noneMatch(flinkNames::contains);
    }
}
