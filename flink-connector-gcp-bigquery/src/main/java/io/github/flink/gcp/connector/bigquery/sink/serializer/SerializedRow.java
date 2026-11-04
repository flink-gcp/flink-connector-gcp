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

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.WireFormat;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * One serialized row as the serializer produced it, before or instead of its encoding.
 *
 * <p>The row has two parts. The base is the serializer's row, held either as the message it built
 * or as the bytes it returned. The tail holds the fields the sink added after the serializer, as a
 * message of the augmented descriptor that sets nothing else, or is {@code null} when the sink adds
 * none.
 *
 * <p>The encoded row is the base's bytes followed by the tail's. Protobuf reads a concatenation of
 * two well-formed encodings as their merge, and every added field is a singular scalar whose last
 * occurrence wins, so the concatenation decodes as the base with the added fields set, exactly as
 * setting them on a parsed base and serializing it again would. {@link #toByteString()} therefore
 * needs no parse, and {@link #getSerializedSize()} is the sum of the two parts.
 */
@Internal
public final class SerializedRow {

    @Nullable private final Message baseMessage;
    @Nullable private ByteString baseBytes;
    @Nullable private final DynamicMessage tail;

    private SerializedRow(
            @Nullable Message baseMessage,
            @Nullable ByteString baseBytes,
            @Nullable DynamicMessage tail) {
        this.baseMessage = baseMessage;
        this.baseBytes = baseBytes;
        this.tail = tail;
    }

    /**
     * Serializes a record with the given serializer, keeping the message it builds when it is a
     * {@link ProtoRowMessageSerializer} and its bytes otherwise.
     *
     * @return the row, without a tail, or {@code null} when the serializer skips the record
     * @throws IOException as the serializer does
     */
    @Nullable
    @SuppressWarnings("unchecked")
    public static <T> SerializedRow serialize(
            BigQueryProtoSerializationSchema<? super T> serializer, T element) throws IOException {
        if (serializer instanceof ProtoRowMessageSerializer) {
            Message message =
                    ((ProtoRowMessageSerializer<? super T>) serializer).serializeToMessage(element);
            return message == null ? null : ofMessage(message);
        }
        ByteString bytes = serializer.serialize(element);
        return bytes == null ? null : ofBytes(bytes);
    }

    /** A row whose base is the message the serializer built. */
    static SerializedRow ofMessage(Message base) {
        return new SerializedRow(Preconditions.checkNotNull(base), null, null);
    }

    /** A row whose base is the bytes the serializer returned. */
    static SerializedRow ofBytes(ByteString base) {
        return new SerializedRow(null, Preconditions.checkNotNull(base), null);
    }

    /**
     * Fails when a base held as bytes is not a well-formed encoding, which a tail must not follow,
     * and reports whether it carries a top-level field under one of the given numbers.
     *
     * <p>A base whose last field declares more bytes than remain, or that leaves a group open,
     * would read the tail's bytes as part of that field: the concatenation could then decode as
     * some other row instead of failing. The check walks the encoding without building anything. A
     * base the serializer handed over as a message is well-formed by construction and carries only
     * fields of its own descriptor.
     *
     * @param numbers the field numbers the tail will set
     * @return whether the base's bytes already carry a value under one of those numbers
     * @throws InvalidProtocolBufferException if the base's bytes are not a well-formed encoding
     */
    boolean checkWellFormed(int[] numbers) throws InvalidProtocolBufferException {
        if (baseMessage != null) {
            return false;
        }
        CodedInputStream input = baseBytes.newCodedInput();
        boolean carries = false;
        try {
            for (int tag = input.readTag(); tag != 0; tag = input.readTag()) {
                if (!carries) {
                    int number = WireFormat.getTagFieldNumber(tag);
                    for (int candidate : numbers) {
                        carries |= candidate == number;
                    }
                }
                if (!input.skipField(tag)) {
                    throw new InvalidProtocolBufferException(
                            "The serialized row closes a group it never opened");
                }
            }
        } catch (InvalidProtocolBufferException e) {
            throw e;
        } catch (IOException e) {
            // A ByteString input reads from memory; nothing else can fail here.
            throw new IllegalStateException(e);
        }
        return carries;
    }

    /** Returns this row with the given added fields, which no field of the base may follow. */
    SerializedRow withTail(DynamicMessage tail) {
        return new SerializedRow(baseMessage, baseBytes, Preconditions.checkNotNull(tail));
    }

    /**
     * Returns the number of bytes {@link #toByteString()} returns, without encoding the row.
     *
     * <p>Protobuf computes a message's size as an {@code int} without an overflow check, so a
     * message past 2 GiB, which repeated references to one {@code ByteString} can make in modest
     * memory, reports a negative size, for the base or for the tail. That is returned as {@link
     * Long#MAX_VALUE}: its encoding cannot be built, so a caller treats any size above {@link
     * Integer#MAX_VALUE} as a failure whatever limit it configures. A size wrapped past 4 GiB reads
     * positive again and cannot be told apart.
     */
    public long getSerializedSize() {
        long base = baseMessage != null ? baseMessage.getSerializedSize() : baseBytes.size();
        long added = tail == null ? 0 : tail.getSerializedSize();
        // Either part can overflow on its own; the sum of two ints cannot overflow a long.
        return base < 0 || added < 0 ? Long.MAX_VALUE : base + added;
    }

    /** Returns the encoded row: the base's bytes followed by the tail's. */
    public ByteString toByteString() {
        ByteString base = baseBytes();
        return tail == null ? base : base.concat(tail.toByteString());
    }

    /**
     * Returns the base as a message of the given descriptor: the serializer's own message when it
     * has exactly that descriptor, and otherwise the base's bytes parsed against it.
     *
     * @param descriptor the serializer's descriptor for the row's destination
     * @return the base message
     * @throws InvalidProtocolBufferException if the base's bytes do not parse as that descriptor
     */
    public Message getBase(Descriptors.Descriptor descriptor)
            throws InvalidProtocolBufferException {
        if (baseMessage != null && baseMessage.getDescriptorForType() == descriptor) {
            return baseMessage;
        }
        return DynamicMessage.parseFrom(descriptor, baseBytes());
    }

    /**
     * Returns the added fields as a message of the given augmented descriptor, or {@code null} when
     * the sink adds none.
     *
     * <p>The tail is built against the augmented descriptor the sink holds when it serializes the
     * row. That descriptor is cached weakly and can be derived again as another instance, which a
     * reader holding the earlier one cannot read field by field; such a tail is re-read from its
     * own few bytes. The re-read does not require the serializer's {@code REQUIRED} fields, which a
     * tail never sets.
     *
     * @param descriptor the augmented descriptor the reader holds for the row's destination
     * @return the tail message, or {@code null}
     * @throws InvalidProtocolBufferException if the tail's bytes do not parse as that descriptor
     */
    @Nullable
    public DynamicMessage getTail(Descriptors.Descriptor descriptor)
            throws InvalidProtocolBufferException {
        if (tail == null || tail.getDescriptorForType() == descriptor) {
            return tail;
        }
        return DynamicMessage.newBuilder(descriptor).mergeFrom(tail.toByteString()).buildPartial();
    }

    private ByteString baseBytes() {
        ByteString base = baseBytes;
        if (base == null) {
            base = baseMessage.toByteString();
            baseBytes = base;
        }
        return base;
    }
}
