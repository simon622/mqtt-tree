package org.slj.mqtt.tree.benchmark;

import com.codahale.metrics.Counter;
import com.hivemq.cluster.topictree.SegmentKeyUtil;
import com.hivemq.mqtt.message.QoS;
import com.hivemq.mqtt.message.subscribe.Topic;
import com.hivemq.mqtt.topic.tree.LocalTopicTree;
import com.hivemq.persistence.local.xodus.bucket.BucketUtils;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.slj.mqtt.tree.MqttTree;
import org.slj.mqtt.tree.MqttTreeLimitExceededException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class TopicTreeHotSpotWritingBenchmark {

    public static final int BRANCHES = 50;
    public static final int WRITER_THREADS = 5;

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @State(Scope.Group)
    public static class TopicTreeWritesHotSpotExecutionPlanShared {

        public MqttTree<String> tree;
        public LocalTopicTree hivemqTree;

        public AtomicInteger idCounter;
        public Map<Integer, List<String>> bucketToTopicMap;

        @Setup(Level.Iteration)
        public void setUp() {
            idCounter = new AtomicInteger(0);
            bucketToTopicMap = new HashMap<>();

            tree = new MqttTree<>(MqttTree.DEFAULT_SPLIT, true);
            tree.withMaxMembersAtLevel(1000000);

            hivemqTree = new LocalTopicTree(10, 10, 1, 1, new Counter(), new Counter());

            for (int i = 0; i < WRITER_THREADS; i++) { // even distribution between threads
                bucketToTopicMap.putIfAbsent(i, new ArrayList<>());

                for (int j = 0; j < BRANCHES; j++) {
                    int depth = ThreadLocalRandom.current().nextInt(1, 10);
                    String topic = generateRandomTopic(depth);

                    bucketToTopicMap.get(i).add(topic);
                }
            }


        }
    }

    // only for trie lock-based implementation since HiveMQ's bucket does not support writes from multiple threads
    @State(Scope.Thread)
    public static class TopicTreeWritesHotSpotExecutionPlanThreadLocal {

        public int id;

        @Setup(Level.Iteration)
        public void setUp(TopicTreeMultiBucketWritesBenchmark.TopicTreeWritesExecutionPlanShared executionPlan) {
            id = executionPlan.idCounter.getAndIncrement();
        }
    }

    @Benchmark
    @Fork(value = 1, warmups = 1)
    @Warmup(iterations = 5)
    @BenchmarkMode(Mode.Throughput)
    @Group("lockBasedTrieWriting")
    @GroupThreads(WRITER_THREADS)
    @OperationsPerInvocation(BRANCHES)
    public void newTTTest(
            TopicTreeWritesHotSpotExecutionPlanShared sharedExecutionPlan,
            TopicTreeWritesHotSpotExecutionPlanThreadLocal localExecutionPlan,
            Blackhole bh) throws MqttTreeLimitExceededException {
        for (final String topic : sharedExecutionPlan.bucketToTopicMap.get(localExecutionPlan.id)) {
            bh.consume(sharedExecutionPlan.tree.subscribe(topic, "foo"));
        }
    }

    @Benchmark
    @Fork(value = 1, warmups = 1)
    @Warmup(iterations = 5)
    @BenchmarkMode(Mode.Throughput)
    @Group("brokerTopicTreeWriting")
    @GroupThreads(1) // 4 threads won't do anything due to all segments being hashed into the same bucket.
    @OperationsPerInvocation(WRITER_THREADS * BRANCHES)
    public void brokerTTTest(
            TopicTreeWritesHotSpotExecutionPlanShared sharedExecutionPlan,
            TopicTreeWritesHotSpotExecutionPlanThreadLocal localExecutionPlan,
            Blackhole bh) {

        for (int i = 0; i < WRITER_THREADS; i++) {
            for (final String topic : sharedExecutionPlan.bucketToTopicMap.get(i)) {
                bh.consume(sharedExecutionPlan.hivemqTree.addTopic("foo", new Topic(topic, QoS.AT_LEAST_ONCE), (byte) 0, null, 0));
            }
        }

    }


    public static String generateRandomTopic(int segments) {
        StringBuilder sb = new StringBuilder();
        sb.append(42);//simulating all segments going into the same bucket
        sb.append("/");
        for (int i = 0; i < segments - 1; i++) {
            //if (i == 0) sb.append("/"); // removed since HiveMQ TT makes the first segment empty then
            int r = ThreadLocalRandom.current().nextInt(0, 1000);
            sb.append(r);
            sb.append("/");
        }
        return sb.toString();
    }
}
