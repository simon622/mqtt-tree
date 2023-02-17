package com.hivemq.mqtt.topic;

import com.hivemq.util.Bytes;

public enum SubscriptionFlag {

    // Value 0 has been used to represent a possibly stale subscription in the removed topic tree cleanup mechanism.
    SHARED_SUBSCRIPTION(1),
    RETAIN_AS_PUBLISHED(2),
    NO_LOCAL(3);

    private final int flagIndex;

    SubscriptionFlag(final int flagIndex) {
        this.flagIndex = flagIndex;
    }

    public int getFlagIndex() {
        return flagIndex;
    }

    public static byte getDefaultFlags(
            final boolean isSharedSubscription,
            final boolean retainAsPublished,
            final boolean noLocal) {

        byte flags = Bytes.setBit((byte) 0, SHARED_SUBSCRIPTION.getFlagIndex(), isSharedSubscription);
        flags = Bytes.setBit(flags, RETAIN_AS_PUBLISHED.getFlagIndex(), retainAsPublished);
        return Bytes.setBit(flags, NO_LOCAL.getFlagIndex(), noLocal);
    }
}
