/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.spanner.source.changestream.reader;

import com.google.cloud.spanner.AsyncResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultSpannerChangeStreamQueryClientFactoryTest {

    @Test
    void reentrantPublicationRunsOnlyAfterTheCallbackReturns() {
        Deque<Runnable> delegated = new ArrayDeque<>();
        Executor delegate = delegated::addLast;
        DefaultSpannerChangeStreamQueryClientFactory.SerializingExecutor executor =
                new DefaultSpannerChangeStreamQueryClientFactory.SerializingExecutor(delegate);
        List<String> order = new ArrayList<>();

        executor.execute(
                () -> {
                    order.add("callback");
                    assertThat(
                                    DefaultSpannerChangeStreamQueryClientFactory.DefaultHandle
                                            .pauseBeforePublishing(
                                                    new SpannerChangeStreamRecord.Heartbeat(
                                                            java.time.Instant.EPOCH),
                                                    executor,
                                                    ignored -> order.add("mailbox-visible")))
                            .isEqualTo(AsyncResultSet.CallbackResponse.PAUSE);
                    order.add("callback-returned");
                });

        delegated.removeFirst().run();
        assertThat(order).containsExactly("callback", "callback-returned");
        delegated.removeFirst().run();
        assertThat(order)
                .containsExactlyElementsOf(
                        Arrays.asList("callback", "callback-returned", "mailbox-visible"));
    }
}
