package com.dataartisans.training.source;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import com.dataartisans.training.entities.FakeKafkaRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The {@link FakeKafkaSource} reads from {@code NO_OF_PARTIONS} Kafka partitions.
 * <p>
 * The timestamps roughly start at the epoch and are ascending per partition. The partitions themselves can be out of sync.
 * *
 */
@Slf4j
public class FakeKafkaSource extends RichParallelSourceFunction<FakeKafkaRecord> implements CheckpointedFunction {
    private static final long serialVersionUID = 4658785571367840693L;

    private static final int NO_OF_PARTIONS             = 8;
    private static final int MAX_TIME_BETWEEN_EVENTS_MS = 1;

    private final Random rand;

    private transient volatile boolean                          cancelled;
    private transient          long[]                           lastTimestampPerPartition;
    private transient          ListState<Tuple2<Integer, Long>> perPartitionTimestampState;
    private transient          int                              indexOfThisSubtask;
    private transient          int                              numberOfParallelSubtasks;
    private transient          List<Integer>                    assignedPartitions;

    private final List<byte[]>  serializedMeasurements;
    private final double        poisonPillRate;
    private final BitSet        idlePartitions;

    FakeKafkaSource(final int seed, final float poisonPillRate, List<Integer> idlePartitions, List<byte[]> serializedMeasurements) {
        this.poisonPillRate = poisonPillRate;
        this.idlePartitions = new BitSet(NO_OF_PARTIONS);
        for (int i : idlePartitions) {
            this.idlePartitions.set(i);
        }
        this.serializedMeasurements = serializedMeasurements;

        this.rand = new Random(seed);
    }

    @Override
    public void open(final Configuration parameters) {
        indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

        assignedPartitions = IntStream.range(0, NO_OF_PARTIONS)
                                      .filter(i -> i % numberOfParallelSubtasks == indexOfThisSubtask)
                                      .boxed()
                                      .collect(Collectors.toList());

        log.info("Now reading from partitions: {}", assignedPartitions);
    }


    @Override
    public void run(final SourceContext<FakeKafkaRecord> sourceContext) throws Exception {

        int numberOfPartitions = assignedPartitions.size();

        while (!cancelled) {
            int nextPartition = assignedPartitions.get(rand.nextInt(numberOfPartitions));

            if (idlePartitions.get(nextPartition)) {
                Thread.sleep(1000); // avoid spinning wait
                continue;
            }

            long nextTimestamp = updateTimestampForPartition(nextPartition);

            byte[] serializedMeasurement = serializedMeasurements.get(rand.nextInt(serializedMeasurements.size()));

            if (rand.nextFloat() > 1 - poisonPillRate) {
                serializedMeasurement = Arrays.copyOf(serializedMeasurement, 10);
            }

            synchronized (sourceContext.getCheckpointLock()) {
                sourceContext.collect(new FakeKafkaRecord(nextTimestamp, null, serializedMeasurement,
                        nextPartition));

            }
        }
    }

    private long updateTimestampForPartition(int partition) {
        if (partition != 0 && lastTimestampPerPartition[partition] == 0L) {
            lastTimestampPerPartition[partition] = partition * 100L;
        }
        lastTimestampPerPartition[partition] += rand.nextInt(MAX_TIME_BETWEEN_EVENTS_MS + 1);
        return lastTimestampPerPartition[partition];
    }

    @Override
    public void cancel() {
        cancelled = true;
    }

    @Override
    public void snapshotState(final FunctionSnapshotContext context) throws Exception {
        perPartitionTimestampState.clear();
        for (final Integer partition : assignedPartitions) {
            perPartitionTimestampState.add(new Tuple2<>(partition, lastTimestampPerPartition[partition]));
        }
    }

    @Override
    public void initializeState(final FunctionInitializationContext context) throws Exception {
        perPartitionTimestampState = context.getOperatorStateStore()
                                            .getUnionListState(new ListStateDescriptor<>("perPartitionTimestampState", TypeInformation
                                                    .of(new TypeHint<Tuple2<Integer, Long>>() {
                                                    })));

        lastTimestampPerPartition = new long[NO_OF_PARTIONS];
        for (Tuple2<Integer, Long> next : perPartitionTimestampState.get()) {
            lastTimestampPerPartition[next.f0] = next.f1;
        }
    }
}
