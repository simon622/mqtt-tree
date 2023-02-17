package com.hivemq.mqtt.topic.tree;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.services.subscription.SubscriptionType;

public enum SubscriptionFilter {

    /**
     * Include individual and shared subscriptions
     */
    ALL(0),

    /**
     * Only include individual subscriptions
     */
    INDIVIDUAL(1),

    /**
     * Only include shared subscriptions
     */
    SHARED(2);

    private static final @NotNull SubscriptionFilter @NotNull [] VALUES = values();

    private final int identifier;

    SubscriptionFilter(final int identifier) {
        this.identifier = identifier;
    }

    public int getIdentifier() {
        return identifier;
    }

    public static @NotNull SubscriptionFilter fromType(final @NotNull SubscriptionType subscriptionType) {
        switch (subscriptionType) {
            case ALL:
                return ALL;
            case INDIVIDUAL:
                return INDIVIDUAL;
            case SHARED:
                return SHARED;
        }
        //Fallback
        return ALL;
    }

    public static @NotNull SubscriptionFilter valueOf(final int identifier) {
        return identifier >= 0 && identifier < VALUES.length ? VALUES[identifier] : ALL;
    }
}
