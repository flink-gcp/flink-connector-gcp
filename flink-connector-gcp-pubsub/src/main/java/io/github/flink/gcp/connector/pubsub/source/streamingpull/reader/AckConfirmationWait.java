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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.AckResponse;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Waits for the server to confirm the acknowledgements of one completed checkpoint, and reports a
 * rejection, a timeout, a failed round trip and an interrupt as an {@link IOException} saying what
 * the operator has to decide.
 *
 * <p>Immutable, and holds no acknowledgement state: {@link PubSubAckTracker} calls it outside its
 * monitor, because with confirmation requested the wait is a server round trip and holding the lock
 * across it would stall the callback threads delivering new messages.
 *
 * <p>On a subscription without exactly-once delivery a failed acknowledgement never completes its
 * future. The timeout is therefore the only signal a failure produces, which is why it is reported
 * as an ambiguity — the acknowledgements may have failed, or merely be slow — rather than as a
 * server error.
 */
@Internal
final class AckConfirmationWait {

    private final Duration timeout;

    /**
     * Creates the wait.
     *
     * @param timeout how long the acknowledgements of one checkpoint may take to be confirmed
     */
    AckConfirmationWait(Duration timeout) {
        this.timeout = Preconditions.checkNotNull(timeout);
    }

    /**
     * Waits for every confirmation of one checkpoint, and fails if any is rejected, late, or
     * interrupted.
     *
     * @param confirmations the futures the checkpoint's acknowledgements returned
     * @param checkpointId the completed checkpoint, named in every failure
     * @throws IOException if an acknowledgement was rejected, the wait timed out, the round trip
     *     failed, or the thread was interrupted
     */
    void await(List<ApiFuture<AckResponse>> confirmations, long checkpointId) throws IOException {
        try {
            List<AckResponse> responses =
                    ApiFutures.allAsList(confirmations)
                            .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            for (AckResponse response : responses) {
                if (response != AckResponse.SUCCESSFUL) {
                    throw new IOException(
                            "Pub/Sub rejected an acknowledgement of checkpoint "
                                    + checkpointId
                                    + " with "
                                    + response
                                    + ".");
                }
            }
        } catch (TimeoutException e) {
            throw new IOException(
                    "Pub/Sub did not confirm the acknowledgements of checkpoint "
                            + checkpointId
                            + " within "
                            + timeout
                            + " ("
                            + confirmations.size()
                            + " messages). On a subscription without exactly-once delivery a"
                            + " failed acknowledgement never completes its future, so this timeout"
                            + " is the only signal there is — the acknowledgements may have"
                            + " failed, or merely be slow. Raise"
                            + " PubSubSubscriberOptions.awaitAckConfirmation(...) if the"
                            + " subscription is simply slow to respond.",
                    e);
        } catch (ExecutionException e) {
            throw new IOException(
                    "Failed to acknowledge the messages of checkpoint " + checkpointId + ".",
                    e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(
                    "Interrupted while confirming the acknowledgements of checkpoint "
                            + checkpointId
                            + ".",
                    e);
        }
    }
}
