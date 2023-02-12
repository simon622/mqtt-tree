package com.hivemq.mqtt.topic.tree;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.mqtt.topic.SubscriberWithQoS;

public interface IterateCallback {

    /**
     * @param subscriber the current subscriber
     * @return true to continue the iteration, false to stop
     */
    boolean call(@NotNull SubscriberWithQoS subscriber, @NotNull String topic);
}
