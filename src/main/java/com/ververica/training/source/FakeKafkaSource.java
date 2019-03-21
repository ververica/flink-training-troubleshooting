package com.ververica.training.source;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import com.ververica.training.DoNotChangeThis;
import com.ververica.training.entities.FakeKafkaRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.BitSet;
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
@DoNotChangeThis
public class FakeKafkaSource extends RichParallelSourceFunction<FakeKafkaRecord> implements CheckpointedFunction {
    private static final long serialVersionUID = 4658785571367840693L;

    private static final int    NO_OF_PARTIONS = 8;
    public static final  Logger log            = LoggerFactory.getLogger(FakeKafkaSource.class);

    private final Random rand;

    private transient volatile boolean       cancelled;
    private transient          int           indexOfThisSubtask;
    private transient          int           numberOfParallelSubtasks;
    private transient          List<Integer> assignedPartitions;
    private transient          boolean       throttled;

    private final List<byte[]> serializedMeasurements;
    private final double       poisonPillRate;
    private final BitSet       idlePartitions;

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

        ParameterTool jobParameters =
                (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        throttled = jobParameters.has("latencyUseCase");

        log.info("Now reading (throttled: {}) from partitions: {}", throttled, assignedPartitions);
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

            long nextTimestamp = getTimestampForPartition(nextPartition);

            byte[] serializedMeasurement = serializedMeasurements.get(rand.nextInt(serializedMeasurements.size()));

            if (rand.nextFloat() > 1 - poisonPillRate) {
                serializedMeasurement = Arrays.copyOf(serializedMeasurement, 10);
            }

            synchronized (sourceContext.getCheckpointLock()) {
                sourceContext.collect(new FakeKafkaRecord(nextTimestamp, null, serializedMeasurement,
                        nextPartition));
            }

            if (throttled) {
                Thread.sleep(1);
            }
        }
    }

    private long getTimestampForPartition(int partition) {
        return System.currentTimeMillis() + (partition * 50L);
    }

    @Override
    public void cancel() {
        cancelled = true;
    }

    @Override
    public void snapshotState(final FunctionSnapshotContext context) {
    }

    @Override
    public void initializeState(final FunctionInitializationContext context) {
    }
}
