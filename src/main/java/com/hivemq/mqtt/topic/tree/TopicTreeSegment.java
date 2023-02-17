package com.hivemq.mqtt.topic.tree;

import com.google.common.collect.ImmutableMap;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.mqtt.topic.SubscriberWithQoS;

import java.util.Set;

public class TopicTreeSegment {

    private final @NotNull String segmentKey;
    //map from topic to subscribers with their QoS levels
    private final @NotNull ImmutableMap<String, Set<SubscriberWithQoS>> subscribers;

    public TopicTreeSegment(
            final @NotNull String segmentKey,
            final @NotNull ImmutableMap<String, Set<SubscriberWithQoS>> subscribers) {
        this.segmentKey = segmentKey;
        this.subscribers = subscribers;
    }

    public @NotNull String getSegmentKey() {
        return segmentKey;
    }

    public @NotNull ImmutableMap<String, Set<SubscriberWithQoS>> getSubscribers() {
        return subscribers;
    }
}
