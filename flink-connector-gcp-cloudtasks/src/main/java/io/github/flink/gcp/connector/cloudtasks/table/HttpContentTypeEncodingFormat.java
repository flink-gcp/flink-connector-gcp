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

package io.github.flink.gcp.connector.cloudtasks.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/** An encoding format that owns the HTTP Content-Type for body-carrying requests. */
@Internal
public interface HttpContentTypeEncodingFormat
        extends EncodingFormat<SerializationSchema<RowData>> {

    /** Returns the media type the format writes. */
    String getContentType();

    /** Validates the physical row type before the sink enters the runtime-provider phase. */
    void validatePhysicalDataType(DataType physicalDataType);
}
