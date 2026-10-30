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

package io.github.flink.gcp.connector.base.sink;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

/**
 * Admits one write per key at a time and holds the rest of that key's writes in arrival order.
 *
 * <p>A key is <em>active</em> from the admission of a write until the caller {@linkplain
 * #release(Object) releases} it with nothing held. A write for an active key is {@linkplain
 * #hold(Object, Object) held} behind the earlier ones, and a release hands back the oldest held
 * write, which then occupies the key in its turn. Writes for other keys are not affected.
 *
 * <p>Nothing here knows what a write is or when it is answered; the writer decides both and calls
 * this sequencer from its task thread only, so the sequencer is not thread-safe. The Firestore and
 * Bigtable sinks share it (#1618); a key's {@code toString} appears in the invariant failures
 * below, so a sink whose keys are user data supplies a key type that does not render them.
 *
 * @param <K> the key a write is ordered by
 * @param <W> the write
 */
@Internal
public final class PerKeyWriteSequencer<K, W> {

    /**
     * Every active key, mapped to its held writes in arrival order, or to {@code null} until one is
     * held: most keys never hold one, and a deque per admitted write would be garbage. A deque,
     * once made, stays while its key is active, so a key written repeatedly does not make one per
     * write.
     */
    private final Map<K, ArrayDeque<W>> activeKeys = new HashMap<>();

    /**
     * Admits a write if its key is not active, making the key active.
     *
     * @return {@code true} if the write may be sent now; {@code false} if its key is active, when
     *     the caller must {@link #hold(Object, Object)} it instead
     */
    public boolean tryAdmit(K key) {
        if (activeKeys.containsKey(key)) {
            return false;
        }
        activeKeys.put(key, null);
        return true;
    }

    /**
     * Holds a write behind its active key's earlier writes.
     *
     * @throws IllegalStateException if the key is not active
     */
    public void hold(K key, W write) {
        if (!activeKeys.containsKey(key)) {
            throw new IllegalStateException(
                    "Held a write for key " + key + ", which has no admitted write.");
        }
        activeKeys.computeIfAbsent(key, k -> new ArrayDeque<>()).add(write);
    }

    /**
     * Ends the turn of the key's current write.
     *
     * @return the key's oldest held write, which now occupies the key and may be sent, or {@code
     *     null} if none is held, which makes the key inactive
     * @throws IllegalStateException if the key is not active: a write was released twice, or one
     *     never admitted was released
     */
    @Nullable
    public W release(K key) {
        if (!activeKeys.containsKey(key)) {
            throw new IllegalStateException(
                    "Released key " + key + ", which has no admitted write.");
        }
        ArrayDeque<W> waiting = activeKeys.get(key);
        W next = waiting == null ? null : waiting.poll();
        if (next == null) {
            activeKeys.remove(key);
        }
        return next;
    }

    /** Forgets every key and held write. */
    public void clear() {
        activeKeys.clear();
    }
}
