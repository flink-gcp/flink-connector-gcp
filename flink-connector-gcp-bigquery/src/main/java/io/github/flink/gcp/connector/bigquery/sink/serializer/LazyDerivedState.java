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

package io.github.flink.gcp.connector.bigquery.sink.serializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Holds state its owner derives from its own serializable fields, once, and again on a task
 * manager.
 *
 * <p>A serializer ships inside the job graph, so what it can carry is what Java serialization can
 * write: a protobuf {@link com.google.protobuf.Descriptors.Descriptor Descriptor} or a conversion
 * plan built from one cannot travel, and has to be derived again where the records are. This holds
 * that derived value in a {@code transient} field and rebuilds it on first use, so the owner needs
 * neither a null check of its own nor a {@code readObject}.
 *
 * <p>The holder itself is <em>not</em> transient: only its contents are, so an owner read back from
 * a stream comes with an empty holder rather than a null one, and the null check stays out of the
 * owner. That holds for a stream a build carrying this field wrote — deserialization assigns the
 * fields the stream carries and runs no initializer, so an owner restored from an older stream
 * would find null here. Nothing crosses builds: a job graph is written and read by one jar, and no
 * checkpoint carries a serializer.
 *
 * <p>Reading the state costs one volatile read once it exists, and derivation happens once even
 * under concurrent first calls (the owner may be shared by several writers). The value is published
 * through the volatile write, so a reader that sees it sees a fully constructed object.
 *
 * <p>The derivation is taken as an owner and a {@link Function} rather than stored. It cannot
 * become a field, so nothing this holder does can put a connector-minted lambda into the job graph
 * (ADR-0125), and {@link Function} is not {@link Serializable} anyway. Passing the owner separately
 * also lets the call site name an unbound method reference ({@code Serializer::deriveState}), which
 * captures nothing, where the {@code this::deriveState} a {@code Supplier} parameter would take
 * captures the owner on a path that runs per record.
 *
 * <p>Deriving eagerly remains the owner's job: every serializer holding one of these derives in its
 * constructor, so that a schema problem fails where the pipeline is built rather than from {@code
 * serialize()} inside the sink's failure handler (ADR-0023, ADR-0024, ADR-0027). This holder is the
 * mechanism, not the policy.
 *
 * @param <S> the derived state
 */
@Internal
public final class LazyDerivedState<S> implements Serializable {

    private static final long serialVersionUID = 1L;

    private transient volatile S state;

    /**
     * Returns the derived state, deriving it first if this holder is empty.
     *
     * @param owner the object the state is derived from
     * @param deriver the derivation, ideally an unbound method reference of the owning type; it is
     *     used only while the holder is empty, so a later call cannot replace what is held
     * @param <O> the owning type
     * @return the derived state, never {@code null}
     */
    public <O> S get(O owner, Function<O, S> deriver) {
        S local = state;
        if (local == null) {
            local = derive(owner, deriver);
        }
        return local;
    }

    private synchronized <O> S derive(O owner, Function<O, S> deriver) {
        S local = state;
        if (local != null) {
            return local;
        }
        local =
                Preconditions.checkNotNull(
                        deriver.apply(owner), "The derived state must not be null");
        state = local;
        return local;
    }
}
