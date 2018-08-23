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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
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


    public static final int           NO_OF_PARTIONS  = 8;
    public static final List<Integer> IDLE_PARTITIONS = Lists.newArrayList(0, 4);
//    public static final List<Integer> IDLE_PARTITIONS = Lists.newArrayList();

    private static final int MAX_TIME_BETWEEN_EVENTS_MS = 1;

    private final Random       rand;
    private final ObjectMapper mapper;


    private transient          List<String>                     locations;
    private transient volatile boolean                          cancelled;
    private transient          HashMap<Integer, Long>           lastTimestampPerPartition;
    private transient          ListState<Tuple2<Integer, Long>> perPartitionTimestampState;
    private transient          int                              indexOfThisSubtask;
    private transient          int                              numberOfParallelSubtasks;
    private transient          List<Integer>                    assignedPartitions;
    private                    double                           poisonPillRate;

    public FakeKafkaSource(final int seed, final float poisonPillRate) {
        this.rand = new Random(seed);
        this.poisonPillRate = poisonPillRate;
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

        this.locations = importLocations();

        log.info("Now reading from partitions: {}", this.getClass(), indexOfThisSubtask, numberOfParallelSubtasks, assignedPartitions);
    }


    @Override
    public void run(final SourceContext<FakeKafkaRecord> sourceContext) throws Exception {

        //TODO: create skew in some keys.

        int numberOfPartitions = assignedPartitions.size();

        while (!cancelled) {
            int nextPartition = assignedPartitions.get(rand.nextInt(numberOfPartitions));

            if (IDLE_PARTITIONS.contains(nextPartition)) {
                Thread.sleep(1000); // avoid spinning wait
                continue;
            }

            long nextTimestamp =
                    lastTimestampPerPartition.getOrDefault(nextPartition, nextPartition * 100L) +
                    rand.nextInt(MAX_TIME_BETWEEN_EVENTS_MS + 1);

            //TODO: Get rid of costly serialization
            Measurement nextMeasurement = new Measurement(rand.nextInt(100),
                    rand.nextDouble() * 100, locations.get(rand.nextInt(locations.size())));
            byte[] serializedMeasurement = mapper.writeValueAsBytes(nextMeasurement);

            if (rand.nextFloat() > 1 - poisonPillRate) {
                serializedMeasurement = Arrays.copyOf(serializedMeasurement, 10);
            }

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
            perPartitionTimestampState.add(new Tuple2<>(partition, lastTimestampPerPartition.getOrDefault(partition, 0L)));
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

    private List<String> importLocations() throws IOException {
        List<String> locations = Lists.newArrayList();
        try (InputStream is = getClass().getResourceAsStream("/cities.csv");
             BufferedReader br = new BufferedReader(new InputStreamReader(is));) {
            String city;
            while ((city = br.readLine()) != null) {
                locations.add(city);
            }
            log.info("{} locations imported.", locations.size());
        }
        return locations;
    }
}
