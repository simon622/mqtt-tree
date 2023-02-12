package com.hivemq.mqtt.message;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.extension.sdk.api.packets.general.Qos;

/**
 * The Quality of Service level
 *
 * @author Dominik Obermaier
 * @since 1.4
 */
public enum QoS {

    /**
     * At most once delivery. The message will be delivered once or never (best effort delivery)
     */
    AT_MOST_ONCE(0),
    /**
     * At least once delivery. The message will be delivered once or multiple times
     */
    AT_LEAST_ONCE(1),
    /**
     * At exactly once delivery. The message will be delivered once and only once
     */
    EXACTLY_ONCE(2);

    private static final @NotNull QoS @NotNull [] VALUES = values();

    private final int qosNumber;
    private final @NotNull Qos qos;

    QoS(final int qosNumber) {
        this.qosNumber = qosNumber;
        qos = Qos.valueOf(name());
    }

    /**
     * @return the integer value of the QoS. Can be 0, 1 or 2
     */
    public int getQosNumber() {
        return qosNumber;
    }

    public @NotNull Qos toQos() {
        return qos;
    }

    private static final @NotNull QoS @NotNull [] LOOKUP = new QoS[Qos.values().length];

    static {
        for (final QoS qoS : values()) {
            LOOKUP[qoS.qos.ordinal()] = qoS;
        }
    }

    /**
     * Creates a QoS level enum from an integer
     *
     * @param i the QoS level as integer (0,1,2)
     * @return the QoS level or <code>null</code> if an invalid QoS level was passed
     */
    @Nullable
    public static QoS valueOf(final int i) {
        return i >= 0 && i < VALUES.length ? VALUES[i] : null;
    }

    public static @NotNull QoS from(final @NotNull Qos qos) {
        return LOOKUP[qos.ordinal()];
    }

    public static @NotNull QoS getMinQoS(final @NotNull QoS qosFirst, final @NotNull QoS qosSecond) {
        if (qosFirst.getQosNumber() < qosSecond.getQosNumber()) {
            return qosFirst;
        }

        return qosSecond;
    }
}
