package org.slj.mqtt.tree.benchmark;

import com.codahale.metrics.Counter;
import com.hivemq.mqtt.topic.tree.SegmentKeyUtil;
import com.hivemq.mqtt.message.QoS;
import com.hivemq.mqtt.message.subscribe.Topic;
import com.hivemq.mqtt.topic.tree.LocalTopicTree;
import com.hivemq.util.BucketUtils;
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

public class TopicTreeMultiBucketWritesBenchmark {

    public static final int BRANCHES = 500;
    public static final int WRITER_THREADS = 20;

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @State(Scope.Group)
    public static class TopicTreeWritesExecutionPlanShared {

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

            hivemqTree = new LocalTopicTree(10, 10, 1, WRITER_THREADS, new Counter(), new Counter());

            for (int i = 0; i < WRITER_THREADS; i++) { // ensure that each bucket exists
                bucketToTopicMap.putIfAbsent(i, new ArrayList<>());
            }

            for (int i = 0; i < WRITER_THREADS * BRANCHES; i++) { // the distribution between threads is not exactly even due to hashing, but it's the same across threads of both implementations on the given iteration
                int depth = ThreadLocalRandom.current().nextInt(1, 10);
                String topic = generateRandomTopic(depth);

                final String firstSegmentKey = SegmentKeyUtil.firstSegmentKey(topic);
                final int bucketId = BucketUtils.getBucket(firstSegmentKey, WRITER_THREADS);

                bucketToTopicMap.get(bucketId).add(topic);
            }
        }
    }

    @State(Scope.Thread)
    public static class TopicTreeWritesExecutionPlanThreadLocal {

        public List<String> generatedLocalTopics = new ArrayList<>();
        public int id;

        @Setup(Level.Iteration)
        public void setUp(TopicTreeWritesExecutionPlanShared executionPlan) {
            id = executionPlan.idCounter.getAndIncrement();

            generatedLocalTopics = new ArrayList<>(executionPlan.bucketToTopicMap.get(id));
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
            TopicTreeWritesExecutionPlanShared sharedExecutionPlan,
            TopicTreeWritesExecutionPlanThreadLocal localExecutionPlan,
            Blackhole bh) throws MqttTreeLimitExceededException {
        for (final String topic : localExecutionPlan.generatedLocalTopics) {
            bh.consume(sharedExecutionPlan.tree.subscribe(topic, "foo"));
        }
    }

    @Benchmark
    @Fork(value = 1, warmups = 1)
    @Warmup(iterations = 5)
    @BenchmarkMode(Mode.Throughput)
    @Group("brokerTopicTreeWriting")
    @GroupThreads(WRITER_THREADS)
    @OperationsPerInvocation(BRANCHES)
    public void brokerTTTest(
            TopicTreeWritesExecutionPlanShared sharedExecutionPlan,
            TopicTreeWritesExecutionPlanThreadLocal localExecutionPlan,
            Blackhole bh) {
        for (final String topic : localExecutionPlan.generatedLocalTopics) {
            bh.consume(sharedExecutionPlan.hivemqTree.addTopic("foo", new Topic(topic, QoS.AT_LEAST_ONCE), (byte) 0, null, localExecutionPlan.id));
        }
    }


    public static String generateRandomTopic(int segments) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < segments; i++) {
            //if (i == 0) sb.append("/"); // removed since HiveMQ TT makes the first segment empty then
            int r = ThreadLocalRandom.current().nextInt(0, 1000);
            sb.append(r);
            sb.append("/");
        }
        return sb.toString();
    }
}
