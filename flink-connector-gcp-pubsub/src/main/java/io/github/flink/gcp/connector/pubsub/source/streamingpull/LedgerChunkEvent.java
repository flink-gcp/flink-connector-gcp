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

package io.github.flink.gcp.connector.pubsub.source.streamingpull;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;

/**
 * Carries one restore-deduplication ledger chunk between a reader and the enumerator (#2049).
 *
 * <p>A reader that is handed a restored chunk uploads it with this event; the enumerator forwards
 * the same event to every reader the current plan gives a split of the chunk's subscription. One
 * event holds one chunk, whose size the reader bounded when it took the checkpoint, so no event
 * approaches the RPC frame size however the state is redistributed.
 */
@Internal
public final class LedgerChunkEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final SubscriptionSplit chunk;

    /**
     * Creates the event.
     *
     * @param chunk the ledger chunk split
     */
    public LedgerChunkEvent(SubscriptionSplit chunk) {
        if (!chunk.isLedgerChunk()) {
            throw new IllegalArgumentException(chunk + " is not a ledger chunk.");
        }
        this.chunk = chunk;
    }

    /** Returns the ledger chunk split. */
    public SubscriptionSplit chunk() {
        return chunk;
    }
}
