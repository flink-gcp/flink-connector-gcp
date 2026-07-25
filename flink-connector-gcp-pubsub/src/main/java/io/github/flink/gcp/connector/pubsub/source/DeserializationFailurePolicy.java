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
 * PubSubSourceBuilder#deserializationFailurePolicy(DeserializationFailurePolicy)}. Whichever is
 * chosen, the failure is counted in Flink's standard {@code numRecordsInErrors} metric.
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
    DROP,

    /**
     * Returns the message to Pub/Sub for redelivery and carries on, leaving what to do with it to
     * the subscription's dead-letter policy: each redelivery raises the message's delivery attempt
     * count until Pub/Sub forwards it to the dead-letter topic.
     *
     * <p><b>Requires a dead-letter policy on every subscription</b>, which the source checks at
     * startup and refuses to run without. Nacking does not fail the job, so without one a message
     * the schema can never convert is redelivered forever, invisibly.
     *
     * <p>Dead-lettering counts deliveries rather than causes, so an unrelated job restart raises
     * the same counter: set the subscription's delivery-attempt limit high enough that ordinary
     * failovers do not dead-letter healthy messages.
     *
     * <p>Like {@link #DROP}, a schema that emitted records before failing keeps those, so the
     * message is both partially emitted and redelivered in full.
     */
    NACK(true);

    private final boolean requiresDeadLetterPolicy;

    DeserializationFailurePolicy() {
        this(false);
    }

    DeserializationFailurePolicy(boolean requiresDeadLetterPolicy) {
        this.requiresDeadLetterPolicy = requiresDeadLetterPolicy;
    }

    /**
     * Returns whether this policy needs a dead-letter policy on the subscription, which the source
     * checks at startup.
     *
     * <p>A property of the policy rather than a comparison at the call site, because the constraint
     * belongs next to the constant that creates it: a future policy that nacks has to answer this,
     * and the check two packages away would otherwise silently let it through. What makes a nack
     * need one is not the nack but the job surviving it — the message comes back and fails again
     * forever. The reader also nacks when emitting a message downstream fails, and that one needs
     * nothing behind it because it rethrows and the job fails visibly.
     */
    public boolean requiresDeadLetterPolicy() {
        return requiresDeadLetterPolicy;
    }
}
