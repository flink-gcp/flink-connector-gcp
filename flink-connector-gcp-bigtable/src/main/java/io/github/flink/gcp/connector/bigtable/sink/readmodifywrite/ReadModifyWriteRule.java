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

package io.github.flink.gcp.connector.bigtable.sink.readmodifywrite;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.ReadModifyWriteRowRequest;

import java.io.Serializable;

/** One immutable append or signed increment, applied in its request's list order. */
@PublicEvolving
public final class ReadModifyWriteRule implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String family;
    private final ByteString qualifier;
    private final ByteString appendValue;
    private final long incrementAmount;

    private ReadModifyWriteRule(
            String family, ByteString qualifier, ByteString appendValue, long incrementAmount) {
        this.family = Preconditions.checkNotNull(family, "family must not be null");
        Preconditions.checkArgument(!family.isBlank(), "family must not be blank");
        this.qualifier = Preconditions.checkNotNull(qualifier, "qualifier must not be null");
        this.appendValue = appendValue;
        this.incrementAmount = incrementAmount;
    }

    /**
     * Appends bytes to the latest value; an unset cell starts empty.
     *
     * @param family the nonblank family
     * @param qualifier the binary qualifier, possibly empty
     * @param value the nonempty bytes to append
     * @return the rule
     */
    public static ReadModifyWriteRule append(
            String family, ByteString qualifier, ByteString value) {
        Preconditions.checkNotNull(value, "value must not be null");
        Preconditions.checkArgument(!value.isEmpty(), "value must not be empty");
        return new ReadModifyWriteRule(family, qualifier, value, 0);
    }

    /**
     * Adds a signed amount to an eight-byte big-endian integer; an unset cell starts at zero.
     * Arithmetic, including overflow and invalid stored values, is handled by Bigtable.
     *
     * @param family the nonblank family
     * @param qualifier the binary qualifier, possibly empty
     * @param amount the signed amount, including zero
     * @return the rule
     */
    public static ReadModifyWriteRule increment(String family, ByteString qualifier, long amount) {
        return new ReadModifyWriteRule(family, qualifier, null, amount);
    }

    ReadModifyWriteRowRequest.Rule toRule() {
        return appendValue != null
                ? ReadModifyWriteRowRequest.Rule.append(family, qualifier, appendValue)
                : ReadModifyWriteRowRequest.Rule.increment(family, qualifier, incrementAmount);
    }
}
