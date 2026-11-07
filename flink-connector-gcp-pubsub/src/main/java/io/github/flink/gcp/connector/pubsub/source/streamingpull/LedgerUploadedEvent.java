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
 * Tells the enumerator that a reader has uploaded every restored ledger chunk it was handed
 * (#2049).
 *
 * <p>Sent by every reader with restore deduplication on, including one that was handed no chunk,
 * because the enumerator cannot tell "no chunks" from "chunks still in flight" otherwise. Events
 * from one reader arrive in order, so this one follows that reader's chunks.
 */
@Internal
public final class LedgerUploadedEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    /** Creates the event. */
    public LedgerUploadedEvent() {}
}
