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

package io.github.flink.gcp.connector.pubsub.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.pubsub.table.PubSubConnectorOptions;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * The {@code pubsub} table source with {@code scan.watermark-push-down.enabled}: a {@code
 * WATERMARK} declared in the DDL is generated inside the source, per split, rather than by an
 * operator after it.
 *
 * <p>That is what makes the FLIP-296 {@code scan.watermark.*} table options work, watermark
 * alignment in particular: the planner reads them only for a source implementing {@link
 * SupportsWatermarkPushDown}. It has two costs, which is why it is a separate class the option
 * selects rather than the ability of every table.
 *
 * <ul>
 *   <li>The planner hands a {@code SourceProvider} {@code WatermarkStrategy.noWatermarks()} on
 *       every supported Flink line (FLINK-19722), so a pushed strategy reaches the source only
 *       through a {@link DataStreamScanProvider}. Flink 2.x extracts a table's connector lineage
 *       dataset from a {@code SourceProvider} only (FLINK-39219), so this table's lineage then
 *       loses the {@code pubsub} namespace and the {@code gcp} facet.
 *   <li>The planner adds source idleness to the pushed strategy from {@code
 *       scan.watermark.idle-timeout} or, absent that, {@code table.exec.source.idle-timeout}.
 *       Source idleness can mark a split idle while it still has records (FLINK-40234), and an
 *       event-time operator downstream then drops them as late. Without push-down the global option
 *       is used by the planner's own watermark operator instead, so enabling push-down would move
 *       an existing setting onto that path silently. {@link #applyWatermark(WatermarkStrategy)}
 *       refuses a positive idle timeout instead. The strategy cannot be asked whether it carries
 *       idleness, so the timeout is resolved from the options the way the planner resolves it, and
 *       at the moment the planner does: the session option is read from the live session
 *       configuration when the strategy arrives, because a query planned after a {@code
 *       sqlQuery(...)} sees any change made to the session in between. Restoring a compiled plan
 *       replays the timeout the plan recorded, so a plan compiled without one is still refused in a
 *       session whose {@code table.exec.source.idle-timeout} is positive: refusing a safe strategy
 *       is the side this errs on.
 * </ul>
 *
 * <p>A table that declares no watermark, or a plan compiled before the option was enabled, never
 * has {@link #applyWatermark(WatermarkStrategy)} called, and reads through the {@code
 * SourceProvider} exactly as {@link PubSubDynamicSource} does.
 */
@Internal
public final class PubSubWatermarkPushDownSource extends PubSubDynamicSource
        implements SupportsWatermarkPushDown {

    /** The {@link ProviderContext#generateUid(String)} name the planner gives its own scan. */
    private static final String SOURCE_UID_NAME = "source";

    /** The table's {@code scan.watermark.idle-timeout}, or {@code null} when it sets none. */
    @Nullable private final Duration tableIdleTimeout;

    /**
     * The session configuration the planner reads {@code table.exec.source.idle-timeout} from. It
     * is the live session object, read only in {@link #applyWatermark(WatermarkStrategy)}, and it
     * is not part of the source's identity: every copy of one table shares it.
     */
    private final ReadableConfig sessionConfig;

    /** The strategy the planner pushed, or {@code null} while none has been. */
    @Nullable private WatermarkStrategy<RowData> watermarkStrategy;

    private PubSubWatermarkPushDownSource(
            PubSubDynamicSource source,
            @Nullable Duration tableIdleTimeout,
            ReadableConfig sessionConfig,
            @Nullable WatermarkStrategy<RowData> watermarkStrategy) {
        super(source);
        this.tableIdleTimeout = tableIdleTimeout;
        this.sessionConfig =
                Preconditions.checkNotNull(sessionConfig, "sessionConfig must not be null");
        this.watermarkStrategy = watermarkStrategy;
    }

    /**
     * Wraps a resolved source.
     *
     * @param source the source the table's other options resolved to
     * @param tableOptions the table's options, including any {@code OPTIONS} hint
     * @param sessionConfig the live session configuration the planner reads {@code
     *     table.exec.source.idle-timeout} from
     * @return the push-down source
     */
    public static PubSubWatermarkPushDownSource of(
            PubSubDynamicSource source, ReadableConfig tableOptions, ReadableConfig sessionConfig) {
        Preconditions.checkNotNull(source, "source must not be null");
        return new PubSubWatermarkPushDownSource(
                source,
                tableOptions.getOptional(FactoryUtil.SOURCE_IDLE_TIMEOUT).orElse(null),
                sessionConfig,
                null);
    }

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        // Mirrors WatermarkPushDownSpec.calculateIdleTimeoutMillis on Flink 1.20.5, 2.2.1 and
        // 2.3.0: a table value of zero or more wins, a negative one falls through to the session
        // option, and only a positive result adds idleness.
        String idleTimeoutOption = FactoryUtil.SOURCE_IDLE_TIMEOUT.key();
        Duration idleTimeout = tableIdleTimeout;
        if (idleTimeout == null || idleTimeout.isNegative()) {
            idleTimeoutOption = ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT.key();
            idleTimeout = sessionConfig.get(ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT);
        }
        if (idleTimeout != null && !idleTimeout.isZero() && !idleTimeout.isNegative()) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' generates the watermark inside the source, and '%s' = '%s'"
                                    + " would add source idleness to it. Source idleness can mark a"
                                    + " split idle while it still has records (FLINK-40234), and"
                                    + " an event-time operator downstream then drops them as late."
                                    + " Set '%s' = '0' on this table, set '%s' = '0' for this job,"
                                    + " or remove '%s'.",
                            PubSubConnectorOptions.SCAN_WATERMARK_PUSH_DOWN_ENABLED.key(),
                            idleTimeoutOption,
                            idleTimeout,
                            FactoryUtil.SOURCE_IDLE_TIMEOUT.key(),
                            ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT.key(),
                            PubSubConnectorOptions.SCAN_WATERMARK_PUSH_DOWN_ENABLED.key()));
        }
        this.watermarkStrategy =
                Preconditions.checkNotNull(watermarkStrategy, "watermarkStrategy must not be null");
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        if (watermarkStrategy == null) {
            return super.getScanRuntimeProvider(context);
        }
        return new WatermarkedScanProvider(
                createSource(context),
                watermarkStrategy,
                context.createTypeInformation(producedDataType()),
                "PubSubSource-" + lineageTableName(),
                parallelism());
    }

    @Override
    public DynamicTableSource copy() {
        return new PubSubWatermarkPushDownSource(
                this, tableIdleTimeout, sessionConfig, watermarkStrategy);
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        PubSubWatermarkPushDownSource that = (PubSubWatermarkPushDownSource) o;
        return Objects.equals(tableIdleTimeout, that.tableIdleTimeout)
                && Objects.equals(watermarkStrategy, that.watermarkStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), tableIdleTimeout, watermarkStrategy);
    }

    /**
     * Reads the source with the pushed strategy, which a {@code SourceProvider} cannot carry.
     *
     * <p>It returns the source stream itself, so the planner's parallelism lands on the source
     * operator as it does for a {@code SourceProvider}. Flink 1.20 and 2.2 also overwrite its name
     * and uid with their own; Flink 2.3 does so only for plans compiled before 2.3, which is why
     * the uid is set here from {@code generateUid("source")}, the name the planner gives a {@code
     * SourceProvider}'s scan, and why the operator name is the connector's on 2.3.
     */
    private static final class WatermarkedScanProvider implements DataStreamScanProvider {

        private final Source<RowData, ?, ?> source;
        private final WatermarkStrategy<RowData> watermarkStrategy;
        private final TypeInformation<RowData> producedType;
        private final String name;
        @Nullable private final Integer parallelism;

        private WatermarkedScanProvider(
                Source<RowData, ?, ?> source,
                WatermarkStrategy<RowData> watermarkStrategy,
                TypeInformation<RowData> producedType,
                String name,
                @Nullable Integer parallelism) {
            this.source = source;
            this.watermarkStrategy = watermarkStrategy;
            this.producedType = producedType;
            this.name = name;
            this.parallelism = parallelism;
        }

        @Override
        public DataStream<RowData> produceDataStream(
                ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
            DataStreamSource<RowData> stream =
                    execEnv.fromSource(source, watermarkStrategy, name, producedType);
            providerContext.generateUid(SOURCE_UID_NAME).ifPresent(stream::uid);
            return stream;
        }

        @Override
        public boolean isBounded() {
            return false;
        }

        @Override
        public Optional<Integer> getParallelism() {
            return Optional.ofNullable(parallelism);
        }
    }
}
