/*
 * Copyright (c) 2005, 2014, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package org.slj;

//import com.codahale.metrics.MetricRegistry;
//import com.google.common.collect.ImmutableSet;
//import com.hivemq.metrics.MetricsHolder;
//import com.hivemq.mqtt.message.QoS;
//import com.hivemq.mqtt.message.subscribe.Topic;
//import com.hivemq.mqtt.topic.tree.LocalTopicTree;
import org.openjdk.jmh.annotations.*;
import org.slj.mqtt.tree.MqttTree;
import org.slj.mqtt.tree.MqttTreeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@Threads(10)
public class MqttTreeBenchmark {

    private ITreeProvider<String> mqttTree;
    private ITreeProvider<String> stripedTree;
    private List<String> topics;
    private List<String> clientIds;
    private int distinctTopicCount = 20000;

    AtomicInteger treeMatches = new AtomicInteger();
    AtomicInteger stripedMatches = new AtomicInteger();

    @Setup
    public void setUp() throws Exception {
        topics = new ArrayList<>();
        clientIds = new ArrayList<>();
        for (int i = 0; i < distinctTopicCount;i++){
            topics.add(MqttTreeUtils.generateRandomTopic(15));
            clientIds.add("ClientId"+ i);
        }

        mqttTree = createMqttTree();
        populateTree(mqttTree);

//        stripedTree = createStripedMqttTree();
//        populateTree(stripedTree);
    }

    @TearDown
    public void outputMatches(){
        if(stripedMatches.get() > 0) System.err.println("Striped Matches " + stripedMatches.get());
        if(treeMatches.get() > 0) System.err.println("MQTT Tree Matches " + treeMatches.get());
    }

    @Benchmark
    public void testAddMqttTree() throws Exception {
        int idx = ThreadLocalRandom.current().nextInt(0, this.distinctTopicCount);
        mqttTree.add(topics.get(idx), clientIds.get(idx));
    }


    @Benchmark
    public void testReadMqttTree() {
        int idx = ThreadLocalRandom.current().nextInt(0, this.distinctTopicCount);
        treeMatches.addAndGet(mqttTree.search(topics.get(idx)));
    }

    @Benchmark
    public void testAddStripedTree() throws Exception {
        int idx = ThreadLocalRandom.current().nextInt(0, this.distinctTopicCount);
        if(stripedTree != null) stripedTree.add(topics.get(idx), clientIds.get(idx));
    }


    @Benchmark
    public void testReadStripedTree() {
        //one known miss
        int idx = ThreadLocalRandom.current().nextInt(0, this.distinctTopicCount);
        if(stripedTree != null) stripedMatches.addAndGet(stripedTree.search(topics.get(idx)));
    }

    protected void populateTree(ITreeProvider<String> tree) throws Exception {
        for (int i = 0; i < distinctTopicCount;i++){
            tree.add(topics.get(i), "ClientId"+ i);
        }
    }

    protected ITreeProvider createMqttTree(){
        return new ITreeProvider<String>() {
            final MqttTree tree = new MqttTree(MqttTree.DEFAULT_SPLIT, true);
            @Override
            public int search(String path) {
                Set<String> s = tree.search(path);
                return s.size();
            }

            @Override
            public void add(String path, String member) throws Exception{
                tree.subscribe(path, member);
            }
        };
    }

//    protected ITreeProvider createStripedMqttTree(){
//        return new ITreeProvider<String>() {
//            final LocalTopicTree tree = new LocalTopicTree(new MetricsHolder(new MetricRegistry()));
//            @Override
//            public int search(String path) {
//                ImmutableSet<?> s = tree.findTopicSubscribers(path).getSubscribers();
//                return s.size();
//            }
//
//            @Override
//            public void add(String path, String member){
//                tree.addTopic(member, new Topic(path, QoS.AT_LEAST_ONCE), (byte) 0x00, null);
//            }
//        };
//    }
}
