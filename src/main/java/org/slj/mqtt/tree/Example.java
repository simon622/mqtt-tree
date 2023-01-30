package org.slj.mqtt.tree;

import java.util.Set;

public class Example {
    public static void main(String[] args) throws MqttSubscriptionTreeLimitExceededException {
        MqttSubscriptionTree<String> tree = new MqttSubscriptionTree<String>(MqttSubscriptionTree.DEFAULT_SPLIT, true);
        tree.withWildcard(MqttSubscriptionTree.DEFAULT_WILDCARD);
        tree.withWildpath(MqttSubscriptionTree.DEFAULT_WILDPATH);

        tree.withMaxPathSegments(1024);
        tree.withMaxMembersAtLevel(1024);

        tree.addPath("/this/is/a/topic", "ClientId1", "ClientId2");

        tree.addPath("/this/+/a/topic", "ClientId3");

        tree.addPath("/this/#", "ClientId4");

        Set<String> m = tree.search("/this/is/a/topic");

        System.out.println(String.format("matching search had [%s] members", m.size()));

        m = tree.search("/is/a/different/topic");

        System.out.println(String.format("non-matching search had [%s] members", m.size()));
    }
}
