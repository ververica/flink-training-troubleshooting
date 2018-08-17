package com.dataartisans.training.source;

import com.dataartisans.training.entities.FakeKafkaRecord;
import com.dataartisans.training.entities.Measurement;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.HashMap;
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

    private static final List<String> LOCATIONS = Lists.newArrayList("waldstadion", "boellenfalltor", "olympiastadion");

    public static final int NO_OF_PARTIONS        = 20;
    public static final int SLOW_SUBTASK_DELAY_MS = 10;

    private static final int MAX_TIME_BETWEEN_EVENTS_MS = 1;

    private final Random       rand;
    private final ObjectMapper mapper;


    private transient volatile boolean                          cancelled;
    private transient          HashMap<Integer, Long>           lastTimestampPerPartition;
    private transient          ListState<Tuple2<Integer, Long>> perPartitionTimestampState;
    private transient          int                              indexOfThisSubtask;
    private transient          int                              numberOfParallelSubtasks;
    private transient          List<Integer>                    assignedPartitions;

    public FakeKafkaSource(final int seed) {
        this.rand = new Random(seed);
        mapper = new ObjectMapper();
    }

    @Override
    public void open(final Configuration parameters) throws Exception {
        indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

        assignedPartitions = IntStream.range(0, NO_OF_PARTIONS)
                                      .filter(i -> i % numberOfParallelSubtasks == indexOfThisSubtask)
                                      .boxed()
                                      .collect(Collectors.toList());

        log.info("{}({}/{}) reads from partitions: {}", this.getClass(), indexOfThisSubtask, numberOfParallelSubtasks, assignedPartitions);
    }

    @Override
    public void run(final SourceContext<FakeKafkaRecord> sourceContext) throws Exception {

        //TODO: Create Skew in some keys.

        int numberOfPartitions = assignedPartitions.size();

        while (!cancelled) {

            int nextPartition = assignedPartitions.get(rand.nextInt(numberOfPartitions));
            long nextTimestamp =
                    lastTimestampPerPartition.getOrDefault(nextPartition, nextPartition * 100L) +
                    rand.nextInt(MAX_TIME_BETWEEN_EVENTS_MS + 1);

            //TODO: Get rid of costly serialization
            Measurement nextMeasurement = new Measurement(rand.nextInt(100),
                    rand.nextDouble() * 100, LOCATIONS.get(rand.nextInt(LOCATIONS.size())));
            byte[] serializedMeasurement = mapper.writeValueAsBytes(nextMeasurement);

            synchronized (sourceContext.getCheckpointLock()) {
                sourceContext.collect(new FakeKafkaRecord(nextTimestamp, null, serializedMeasurement,
                        indexOfThisSubtask * 2));

            }

            lastTimestampPerPartition.put(nextPartition, nextTimestamp);
        }
    }

    @Override
    public void cancel() {
        cancelled = true;
    }

    @Override
    public void snapshotState(final FunctionSnapshotContext context) throws Exception {
        perPartitionTimestampState.clear();
        for (final Integer partition : assignedPartitions) {
            perPartitionTimestampState.add(new Tuple2<>(partition, lastTimestampPerPartition.get(partition)));
        }
    }

    @Override
    public void initializeState(final FunctionInitializationContext context) throws Exception {
        perPartitionTimestampState = context.getOperatorStateStore()
                                            .getUnionListState(new ListStateDescriptor<Tuple2<Integer, Long>>("perPartitionTimestampState", TypeInformation
                                                    .of(new TypeHint<Tuple2<Integer, Long>>() {
                                                    })));

        Iterable<Tuple2<Integer, Long>> perPartitionTimestamps = perPartitionTimestampState.get();
        Iterator<Tuple2<Integer, Long>> partitionTimestampIterator = perPartitionTimestamps.iterator();

        lastTimestampPerPartition = Maps.newHashMap();
        while (partitionTimestampIterator.hasNext()) {
            Tuple2<Integer, Long> next = partitionTimestampIterator.next();
            lastTimestampPerPartition.put(next.f0, next.f1);
        }
    }
}
