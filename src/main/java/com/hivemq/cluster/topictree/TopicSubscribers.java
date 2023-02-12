package com.hivemq.cluster.topictree;

import com.google.common.collect.ImmutableSet;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.mqtt.topic.SubscriberWithIdentifiers;

/**
 * @author Till Seeberger
 * @since 4.5.1
 */
public class TopicSubscribers {

    private final @NotNull ImmutableSet<SubscriberWithIdentifiers> subscriber;
    private final @NotNull ImmutableSet<String> sharedSubscriptions;

    public TopicSubscribers(final @NotNull ImmutableSet<SubscriberWithIdentifiers> subscriber,
                            final @NotNull ImmutableSet<String> sharedSubscriptions) {
        this.subscriber = subscriber;
        this.sharedSubscriptions = sharedSubscriptions;
    }

    @NotNull
    public ImmutableSet<SubscriberWithIdentifiers> getSubscribers() { return subscriber; }

    @NotNull
    public ImmutableSet<String> getSharedSubscriptions() { return sharedSubscriptions; }
}