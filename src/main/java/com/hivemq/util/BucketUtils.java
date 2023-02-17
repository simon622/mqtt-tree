package com.hivemq.util;

import com.google.common.collect.ImmutableMultimap;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import net.openhft.hashing.LongHashFunction;

import java.util.Collection;
import java.util.function.Function;

/**
 * @author Dominik Obermaier
 */
public class BucketUtils {

    private static final LongHashFunction XX = LongHashFunction.xx();

    public static int getBucket(@NotNull final String id, final int bucketSize) {
        return Math.abs((int) (XX.hashChars(id) % bucketSize));
    }

    /**
     * Creates an {@link ImmutableMultimap} which key are the bucket indexes from 0 to bucketSize,
     * the entries are the provided entries to the bucket index via the String key provided by the function.
     * @param entries that are mapped
     * @param bucketSize is the total amount of buckets
     * @param function that returns a string for which the bucket is determined for each entry
     */
    @NotNull
    public static <T> ImmutableMultimap<Integer, T> mapToBuckets(@NotNull final Collection<T> entries,
                                                                 final int bucketSize,
                                                                 @NotNull final Function<T, String> function) {
        final ImmutableMultimap.Builder<Integer, T> builder = ImmutableMultimap.builder();
        for (final T entry : entries) {
            final String key = function.apply(entry);
            final int bucket = getBucket(key, bucketSize);
            builder.put(bucket, entry);
        }
        return builder.build();
    }
}
