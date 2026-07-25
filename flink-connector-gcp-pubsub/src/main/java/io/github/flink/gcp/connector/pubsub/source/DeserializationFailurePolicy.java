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

package io.github.flink.gcp.connector.pubsub.source;

import org.apache.flink.annotation.PublicEvolving;

/**
 * What the source does with a message its deserialization schema cannot convert.
 *
 * <p>Set via {@link
 * PubSubSourceBuilder#deserializationFailurePolicy(DeserializationFailurePolicy)}. Either way the
 * failure is counted in Flink's standard {@code numRecordsInErrors} metric.
 */
@PublicEvolving
public enum DeserializationFailurePolicy {

    /**
     * Fails the job. The message stays unacknowledged, so Pub/Sub redelivers it — which means a
     * message that can never be deserialized fails the job again after every restart until it is
     * removed or the schema is fixed. That is the default because silently discarding data should
     * be a decision, not an accident.
     */
    FAIL,

    /**
     * Discards the message and carries on, acknowledging it immediately so it is not redelivered.
     * Failures are counted and logged at a decreasing rate, so a burst of bad messages does not
     * flood the log.
     *
     * <p><b>This drops data.</b> A schema that collected records before failing keeps those — the
     * emitted prefix has already reached the output and cannot be recalled — so a partial message
     * is discarded partially.
     */
    DROP
}
