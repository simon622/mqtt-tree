package com.hivemq.mqtt.topic.tree;

import com.codahale.metrics.Counter;
import com.hivemq.extension.sdk.api.annotations.NotNull;

public class SubscriptionCounters {

    private final @NotNull Counter subscriptionCounter;
    private final @NotNull Counter sharedSubscriptionCounter;

    public SubscriptionCounters(
            final @NotNull Counter subscriptionCounter,
            final @NotNull Counter sharedSubscriptionCounter) {
        this.subscriptionCounter = subscriptionCounter;
        this.sharedSubscriptionCounter = sharedSubscriptionCounter;
    }

    public @NotNull Counter getSubscriptionCounter() {
        return subscriptionCounter;
    }

    public @NotNull Counter getSharedSubscriptionCounter() {
        return sharedSubscriptionCounter;
    }
}
