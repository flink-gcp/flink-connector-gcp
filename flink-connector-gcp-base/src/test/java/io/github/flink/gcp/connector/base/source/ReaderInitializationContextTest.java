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

package io.github.flink.gcp.connector.base.source;

import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;

import io.github.flink.gcp.connector.testutils.FakeSourceReaderContext;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReaderInitializationContextTest {

    private final MetricListener listener = new MetricListener();
    private final SourceReaderMetricGroup metricGroup =
            InternalSourceReaderMetricGroup.mock(listener.getMetricGroup());
    private final FakeSourceReaderContext context = new FakeSourceReaderContext(metricGroup);

    @Test
    void handsTheReaderContextsOwnMetricGroupAndClassLoaderToTheSchema() {
        // Asserted as identity rather than as "returns something": the adapter exists only to name
        // the two members the deserialization schema's context expects, so returning a different
        // metric group or an unrelated class loader is exactly the defect it could carry.
        ReaderInitializationContext initialization = new ReaderInitializationContext(context);

        assertThat(initialization.getMetricGroup()).isSameAs(context.metricGroup());
        // Not asserted through asClassLoader(): every class in this build shares one loader under
        // surefire, so an adapter that invented its own loader would compare equal. What does
        // discriminate is where the returned wrapper came from — the reader context builds its own,
        // as an anonymous class of its own.
        assertThat(initialization.getUserCodeClassLoader().getClass().getEnclosingClass())
                .isEqualTo(FakeSourceReaderContext.class);
    }

    @Test
    void refusesAMissingReaderContext() {
        assertThatThrownBy(() -> new ReaderInitializationContext(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("context must not be null");
    }
}
