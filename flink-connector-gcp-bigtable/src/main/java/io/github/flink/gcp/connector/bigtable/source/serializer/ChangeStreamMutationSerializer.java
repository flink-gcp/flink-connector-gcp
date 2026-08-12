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

package io.github.flink.gcp.connector.bigtable.source.serializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.util.Objects;

/** Serializes the SDK's immutable mutation model without Kryo reflecting into its collections. */
@Internal
public final class ChangeStreamMutationSerializer extends TypeSerializer<ChangeStreamMutation> {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public TypeSerializer<ChangeStreamMutation> duplicate() {
        return this;
    }

    @Override
    public ChangeStreamMutation createInstance() {
        return null;
    }

    @Override
    public ChangeStreamMutation copy(ChangeStreamMutation from) {
        return from;
    }

    @Override
    public ChangeStreamMutation copy(ChangeStreamMutation from, ChangeStreamMutation reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(ChangeStreamMutation record, DataOutputView target) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream objects = new ObjectOutputStream(bytes)) {
            objects.writeObject(record);
        }
        byte[] serialized = bytes.toByteArray();
        target.writeInt(serialized.length);
        target.write(serialized);
    }

    @Override
    public ChangeStreamMutation deserialize(DataInputView source) throws IOException {
        int length = source.readInt();
        if (length < 0) {
            throw new IOException("Negative ChangeStreamMutation length: " + length);
        }
        byte[] serialized = new byte[length];
        source.readFully(serialized);
        try (ObjectInputStream objects =
                new ConnectorObjectInputStream(new ByteArrayInputStream(serialized))) {
            return (ChangeStreamMutation) objects.readObject();
        } catch (ClassNotFoundException | ClassCastException e) {
            throw new IOException("Could not deserialize a ChangeStreamMutation", e);
        }
    }

    @Override
    public ChangeStreamMutation deserialize(ChangeStreamMutation reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int length = source.readInt();
        if (length < 0) {
            throw new IOException("Negative ChangeStreamMutation length: " + length);
        }
        target.writeInt(length);
        target.write(source, length);
    }

    @Override
    public TypeSerializerSnapshot<ChangeStreamMutation> snapshotConfiguration() {
        return new Snapshot();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ChangeStreamMutationSerializer;
    }

    @Override
    public int hashCode() {
        return ChangeStreamMutationSerializer.class.hashCode();
    }

    /** Snapshot that rejects Java-serialized mutations from another Bigtable SDK version. */
    public static final class Snapshot implements TypeSerializerSnapshot<ChangeStreamMutation> {

        private static final int VERSION = 1;
        private static final String UNKNOWN_SDK_VERSION = "unknown";

        private String sdkVersion;

        public Snapshot() {
            this(currentSdkVersion());
        }

        Snapshot(String sdkVersion) {
            this.sdkVersion = Objects.requireNonNull(sdkVersion, "sdkVersion");
        }

        @Override
        public int getCurrentVersion() {
            return VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            out.writeUTF(sdkVersion);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            if (readVersion != VERSION) {
                throw new IOException(
                        "Unsupported ChangeStreamMutation serializer snapshot version "
                                + readVersion
                                + "; this connector reads version "
                                + VERSION
                                + ".");
            }
            sdkVersion = in.readUTF();
        }

        @Override
        public TypeSerializer<ChangeStreamMutation> restoreSerializer() {
            return new ChangeStreamMutationSerializer();
        }

        @Override
        public TypeSerializerSchemaCompatibility<ChangeStreamMutation> resolveSchemaCompatibility(
                TypeSerializerSnapshot<ChangeStreamMutation> oldSerializerSnapshot) {
            if (!(oldSerializerSnapshot instanceof Snapshot)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }
            Snapshot oldSnapshot = (Snapshot) oldSerializerSnapshot;
            return Objects.equals(sdkVersion, oldSnapshot.sdkVersion)
                    ? TypeSerializerSchemaCompatibility.compatibleAsIs()
                    : TypeSerializerSchemaCompatibility.incompatible();
        }

        private static String currentSdkVersion() {
            String version = ChangeStreamMutation.class.getPackage().getImplementationVersion();
            return version == null ? UNKNOWN_SDK_VERSION : version;
        }
    }

    /** Resolves SDK classes through the connector rather than the calling job's classloader. */
    private static final class ConnectorObjectInputStream extends ObjectInputStream {

        private ConnectorObjectInputStream(InputStream in) throws IOException {
            super(in);
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc)
                throws IOException, ClassNotFoundException {
            try {
                return Class.forName(
                        desc.getName(),
                        false,
                        ChangeStreamMutationSerializer.class.getClassLoader());
            } catch (ClassNotFoundException e) {
                return super.resolveClass(desc);
            }
        }
    }
}
