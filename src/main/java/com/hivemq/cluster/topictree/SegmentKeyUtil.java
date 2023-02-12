package com.hivemq.cluster.topictree;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class SegmentKeyUtil {

    public static String segmentKey(final String topic, final int length) {
        checkNotNull(topic, "Topic must not be null");
        checkArgument(!topic.isEmpty(), "Topic must not be empty");
        checkArgument(length > 0, "Segment key length must be grater than zero");
        int end = -1;
        for (int i = 0; i < length; i++) {
            end = topic.indexOf('/', end + 1);
            if (end == -1) {
                return topic;
            }
        }
        return topic.substring(0, end);
    }

    public static boolean containsWildcard(final String segmentKey) {
        if (segmentKey.contains("+")) {
            return true;
        }
        if (segmentKey.contains("#")) {
            return true;
        }
        return false;
    }

    public static String firstSegmentKey(final String topic) {
        // topic can be a segment key in this case
        if (topic.isEmpty()) {
            return "";
        }
        return segmentKey(topic, 1);
    }
}
