package org.slj.mqtt.tree.benchmark;

import com.codahale.metrics.Counter;
import com.hivemq.mqtt.message.QoS;
import com.hivemq.mqtt.message.subscribe.Topic;
import com.hivemq.mqtt.topic.tree.LocalTopicTree;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.slj.mqtt.tree.MqttTree;
import org.slj.mqtt.tree.MqttTreeLimitExceededException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

// http://psy-lob-saw.blogspot.com/2013/12/jaq-spsc-latency-benchmarks1.html
public class TopicTreeReadsBenchmark {

    public static final int BRANCHES = 50;

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @State(Scope.Group)
    public static class TopicTreeReadsExecutionPlanShared {

        public List<String> added = new ArrayList<>();//initialized only once per benchmark trial

        public MqttTree<String> tree;
        public LocalTopicTree hivemqTree;

        @Setup(Level.Trial)
        public void setUp() throws MqttTreeLimitExceededException {
            tree = new MqttTree<>(MqttTree.DEFAULT_SPLIT, true);
            tree.withMaxMembersAtLevel(1000000);

            hivemqTree = new LocalTopicTree(10, 10, 1, 1, new Counter(), new Counter());

            for (int i = 0; i < BRANCHES; i++) {
                int depth = ThreadLocalRandom.current().nextInt(1, 10);
                String topic = generateRandomTopic(depth);
                tree.subscribe(topic, "foo");
                hivemqTree.addTopic("foo", new Topic(topic, QoS.AT_LEAST_ONCE), (byte) 0, null, 0);//using addTopic instead of addSubscriptions to be comparable
                added.add(topic);
            }
        }
    }

    @State(Scope.Thread)
    public static class TopicTreeReadsExecutionPlanThreadLocal {

        public List<String> localAdded = new ArrayList<>();

        @Setup(Level.Iteration)
        public void setUp(TopicTreeReadsExecutionPlanShared executionPlan) {
            localAdded = new ArrayList<>(executionPlan.added);
        }
    }

    @Benchmark
    @Fork(value = 1, warmups = 1)
    @Warmup(iterations = 5)
    @BenchmarkMode(Mode.Throughput)
    @Group("lockBasedTrieReading")
    @GroupThreads(4)
    @OperationsPerInvocation(BRANCHES)
    public void newTTTest(
            TopicTreeReadsExecutionPlanShared executionPlan,
            TopicTreeReadsExecutionPlanThreadLocal topicTreeReadsExecutionPlanThreadLocal,
            Blackhole bh) {
        for (final String topic : topicTreeReadsExecutionPlanThreadLocal.localAdded) {
            bh.consume(executionPlan.tree.search(topic));
        }
    }

    @Benchmark
    @Fork(value = 1, warmups = 1)
    @Warmup(iterations = 5)
    @BenchmarkMode(Mode.Throughput)
    @Group("brokerTopicTreeReading")
    @GroupThreads(4)
    @OperationsPerInvocation(BRANCHES)
    public void brokerTTTest(
            TopicTreeReadsExecutionPlanShared executionPlan,
            TopicTreeReadsExecutionPlanThreadLocal topicTreeReadsExecutionPlanThreadLocal,
            Blackhole bh) {
        for (final String topic : topicTreeReadsExecutionPlanThreadLocal.localAdded) {
            bh.consume(executionPlan.hivemqTree.findTopicSubscribers(topic, 0));
        }
    }


    public static String generateRandomTopic(int segments) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < segments; i++) {
            //if (i == 0) sb.append("/");
            int r = ThreadLocalRandom.current().nextInt(0, 1000);
            sb.append(r);
            sb.append("/");
        }
        return sb.toString();
    }
}