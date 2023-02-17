package com.hivemq.mqtt.message.subscribe;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.mqtt.message.QoS;

/**
 * @author Florian Limpöck
 * @since 4.0.0
 */
public interface Mqtt3Topic {

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

}
