package com.hivemq.mqtt.message.subscribe;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.mqtt.message.QoS;
import com.hivemq.mqtt.message.mqtt5.Mqtt5RetainHandling;

/**
 * @author Florian Limpöck
 * @since 4.0.0
 */
public interface Mqtt5Topic {

    /**
     * The default for whether the client must not receive messages published by itself.
     */
    boolean DEFAULT_NO_LOCAL = false;

    /**
     * The default handling of retained message.
     */
    Mqtt5RetainHandling DEFAULT_RETAIN_HANDLING = Mqtt5RetainHandling.SEND;

    /**
     * The default for whether the retain flag for incoming publishes must be set to its original value.
     */
    boolean DEFAULT_RETAIN_AS_PUBLISHED = false;

    /**
     * @return the topic as String representation
     */
    @NotNull
    String getTopic();

    /**
     * @return the QoS of a Topic
     */
    @NotNull
    QoS getQoS();

    /**
     * @return whether the client must not receive messages published by itself. The default is {@link #DEFAULT_NO_LOCAL}.
     */
    boolean isNoLocal();

    /**
     * @return the handling of retained message for this subscription. The default is {@link #DEFAULT_RETAIN_HANDLING}.
     */
    @NotNull
    Mqtt5RetainHandling getRetainHandling();

    /**
     * @return whether the retain flag for incoming publishes must be set to its original value.
     */
    boolean isRetainAsPublished();

}
