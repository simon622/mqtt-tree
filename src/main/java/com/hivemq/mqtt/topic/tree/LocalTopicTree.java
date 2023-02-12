package com.hivemq.mqtt.topic.tree;

import com.codahale.metrics.Counter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.ImmutableIntArray;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.mqtt.message.subscribe.Topic;
import com.hivemq.mqtt.topic.SubscriberWithIdentifiers;
import com.hivemq.mqtt.topic.SubscriberWithQoS;
import com.hivemq.util.BucketUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A topic tree implementation which works with a standard read write lock with fairness guarantees. Either the whole
 * tree is locked or unlocked.
 */
public class LocalTopicTree {

    private static final @NotNull Logger log = LoggerFactory.getLogger(LocalTopicTree.class);
    private static final int TOPIC_TREE_TOPIC_LENGTH_LIMIT_IN_SEGMENTS = 1000;

    final @NotNull CopyOnWriteArrayList<SubscriberWithQoS> rootWildcardSubscribers = new CopyOnWriteArrayList<>();
    private final SubscriptionCounters counters;
    private final int indexMapCreationThreshold;
    private final int subscriberMapCreationThreshold;
    private final int segmentKeyLength;
    private final int bucketForRootWildcard;

    final @NotNull HashMap<String, TopicTreeNode> @NotNull [] buckets;

    public LocalTopicTree(
            final int indexMapCreationThreshold,
            final int subscriberMapCreationThreshold,
            final int segmentKeyLength,
            final int bucketCount,
            final @NotNull Counter subscriptionCounter,
            final @NotNull Counter sharedSubscriptionCounter) {

        //noinspection unchecked
        buckets = new HashMap[bucketCount];
        Arrays.parallelSetAll(buckets, i -> new HashMap<>());
        this.indexMapCreationThreshold = indexMapCreationThreshold;
        this.subscriberMapCreationThreshold = subscriberMapCreationThreshold;
        this.segmentKeyLength = segmentKeyLength;
        bucketForRootWildcard = BucketUtils.getBucket("#", bucketCount);
        counters = new SubscriptionCounters(subscriptionCounter, sharedSubscriptionCounter);
    }

    /* *****************
        Adding a topic
     ******************/

    public boolean addTopic(
            final @NotNull String subscriber,
            final @NotNull Topic topic,
            final byte flags,
            final @Nullable String sharedName,
            final int bucketIndex) {

        checkNotNull(subscriber, "Subscriber must not be null");
        checkNotNull(topic, "Topic must not be null");

        final String[] contents = StringUtils.splitPreserveAllTokens(topic.getTopic(), '/');

        //Do not store subscriptions with more than 1000 segments
        if (contents.length > TOPIC_TREE_TOPIC_LENGTH_LIMIT_IN_SEGMENTS) {
            log.warn("Subscription from {} on topic {} exceeds maximum segment count of {} segments, ignoring it",
                    subscriber, topic, TOPIC_TREE_TOPIC_LENGTH_LIMIT_IN_SEGMENTS);
            return false;
        }

        if (contents.length == 0) {
            log.debug("Tried to add an empty topic to the topic tree.");
            return false;
        }

        final SubscriberWithQoS entry = new SubscriberWithQoS(subscriber, topic.getQoS().getQosNumber(), flags,
                sharedName, topic.getSubscriptionIdentifier(), null);

        if (contents.length == 1 && "#".equals(contents[0])) {
            if (!rootWildcardSubscribers.contains(entry)) {
                //Remove the same subscription with different QoS
                final boolean removed = removeRootWildcardSubscriber(subscriber, sharedName);
                rootWildcardSubscribers.add(entry);
                counters.getSubscriptionCounter().inc();
                if (entry.isSharedSubscription()) {
                    counters.getSharedSubscriptionCounter().inc();
                }
                return removed;
            }
            return true;
        }

        final String segmentKey = contents[0];

        TopicTreeNode node = buckets[bucketIndex].get(segmentKey);
        if (node == null) {
            node = new TopicTreeNode(segmentKey);
            buckets[bucketIndex].put(segmentKey, node);
        }

        if (contents.length == 1) {
            return node.exactSubscriptions.addSubscriber(entry, topic.getTopic(),
                    counters, subscriberMapCreationThreshold);
        } else {

            return addNode(entry, topic.getTopic(), contents, node, 1);
        }
    }

    public long getSubscriptionsCount() {
        return counters.getSubscriptionCounter().getCount();
    }

    public long getSharedSubscriptionsCount() {
        return counters.getSharedSubscriptionCounter().getCount();
    }

    public @NotNull TopicTreeSegment getSegment(final @NotNull String segmentKey, final int bucketIndex) {
        checkNotNull(segmentKey, "Segment key must not be null");
        checkArgument(!"#".equals(segmentKey), "getSegment cannot be called for root wildcard");

        final String nodeKey;
        if (segmentKey.endsWith("#")) {
            // Remove wildcard ending ('+' is a separate node and must therefore not be removed)
            nodeKey = segmentKey.substring(0, segmentKey.length() - 2);
        } else {
            nodeKey = segmentKey;
        }
        final String firstSegmentKey = SegmentKeyUtil.firstSegmentKey(segmentKey);
        final TopicTreeNode segmentNode = getSegmentKeyNode(firstSegmentKey, nodeKey, bucketIndex);
        if (segmentNode != null) {
            return new TopicTreeSegment(segmentKey, getAllSubscriber(segmentNode, nodeKey, segmentKey));
        }
        return new TopicTreeSegment(segmentKey, ImmutableMap.of());
    }

    /**
     * @param firstSegmentKey first topic level
     * @param segmentKey      segment key with configured length
     * @return the node of the segment key or null
     */
    private @Nullable TopicTreeNode getSegmentKeyNode(
            final @NotNull String firstSegmentKey,
            final @NotNull String segmentKey,
            final int bucketIndex) {

        final TopicTreeNode firstSegmentNode = buckets[bucketIndex].get(firstSegmentKey);
        if (firstSegmentNode == null) {
            return null;
        }
        TopicTreeNode current = firstSegmentNode;
        final String[] split = StringUtils.splitPreserveAllTokens(segmentKey, "/");
        for (int i = 1; i < split.length; i++) {
            if ("#".equals(split[i])) {
                return current;
            }
            if (current == null) {
                return null;
            }
            current = current.getChildNodeForTopicPart(split[i]);
        }
        return current;
    }

    private @NotNull ImmutableMap<String, Set<SubscriberWithQoS>> getAllSubscriber(
            final @NotNull TopicTreeNode node,
            final @NotNull String topic,
            final @NotNull String segmentKey) {

        final ImmutableMap.Builder<String, Set<SubscriberWithQoS>> resultMap = ImmutableMap.builder();

        if (!node.exactSubscriptions.isEmpty() && SegmentKeyUtil.segmentKey(topic, segmentKeyLength).equals(segmentKey)) {
            final Set<SubscriberWithQoS> subscribersToAdd = node.exactSubscriptions.getAllSubscribers();
            if (!subscribersToAdd.isEmpty()) {
                resultMap.put(topic, subscribersToAdd);
            }
        }
        if (!node.wildcardSubscriptions.isEmpty() && SegmentKeyUtil.segmentKey(topic + "/#", segmentKeyLength).equals(segmentKey)) {
            final Set<SubscriberWithQoS> subscribersToAdd = node.wildcardSubscriptions.getAllSubscribers();
            if (!subscribersToAdd.isEmpty()) {
                resultMap.put(topic + "/#", subscribersToAdd);
            }
        }

        if (node.children != null) {
            for (int i = 0; i < node.children.length; i++) {
                final TopicTreeNode child = node.children[i];
                if (child != null) {
                    final String newTopic = topic + "/" + child.getTopicPart();
                    resultMap.putAll(getAllSubscriber(child, newTopic, segmentKey));
                }
            }
        } else if (node.getChildrenMap() != null) {
            for (final TopicTreeNode child : node.getChildrenMap().values()) {
                if (child != null) {
                    final String newTopic = topic + "/" + child.getTopicPart();
                    resultMap.putAll(getAllSubscriber(child, newTopic, segmentKey));
                }
            }
        }
        return resultMap.build();
    }

    public @NotNull TopicSubscribers getRootLevelWildcardSubs(final @NotNull String topic, final int bucketIndex) {
        checkNotNull(topic);
        final String[] topicPart = StringUtils.splitPreserveAllTokens(topic, '/');
        final ImmutableList.Builder<SubscriberWithQoS> subscribers = ImmutableList.builder();
        final ImmutableSet.Builder<String> sharedSubscriptions = ImmutableSet.builder();

        final ClientQueueDispatchingSubscriptionInfoFinder subscriberConsumer = new ClientQueueDispatchingSubscriptionInfoFinder(subscribers, sharedSubscriptions);

        final TopicTreeNode firstSegmentNode = buckets[bucketIndex].get("+");
        if (firstSegmentNode != null) {
            traverseTree(firstSegmentNode, subscriberConsumer, topicPart, 0);
        }
        for (final SubscriberWithQoS rootWildcardSubscriber : rootWildcardSubscribers) {
            addWithTopicFilter(subscribers, sharedSubscriptions, rootWildcardSubscriber, "#");
        }

        final ImmutableSet<SubscriberWithIdentifiers> distinctSubscribers = createDistinctSubscribers(subscribers.build());

        return new TopicSubscribers(distinctSubscribers, sharedSubscriptions.build());
    }

    private void addSubscriptions(
            final @NotNull String topic,
            final @NotNull Set<SubscriberWithQoS> subscribers,
            final int bucketIndex) {

        for (final SubscriberWithQoS subscriberWithQoS : subscribers) {
            byte flags = subscriberWithQoS.getFlags();
            addTopic(subscriberWithQoS.getSubscriber(), new Topic(topic, subscriberWithQoS.getAndConvertToQoS(),
                            subscriberWithQoS.isNoLocal(), subscriberWithQoS.isRetainAsPublished()), flags,
                    subscriberWithQoS.getSharedName(), bucketIndex);
        }
    }

    private boolean addNode(
            final @NotNull SubscriberWithQoS subscriber,
            final @NotNull String topicFilter,
            final @NotNull String @NotNull [] contents,
            final @NotNull TopicTreeNode node,
            final int i) {

        final String content = contents[i];

        if ("#".equals(content)) {
            return node.wildcardSubscriptions.addSubscriber(subscriber, topicFilter, counters, subscriberMapCreationThreshold);
        }

        final TopicTreeNode subNode = node.addChildNodeIfAbsent(content, indexMapCreationThreshold);

        if (i + 1 == contents.length) {
            return subNode.exactSubscriptions.addSubscriber(subscriber, topicFilter, counters, subscriberMapCreationThreshold);
        } else {
            return addNode(subscriber, topicFilter, contents, subNode, i + 1);
        }
    }

    /* ********************
        Find Subscribers
     *********************/

    /**
     * All subscribers for a topic (PUBLISH)
     *
     * @param topic       the topic to publish to (no wildcards)
     * @param bucketIndex
     * @return the subscribers interested in this topic with all their identifiers
     */
    public @NotNull TopicSubscribers findTopicSubscribers(final @NotNull String topic, final int bucketIndex) {
        return findTopicSubscribers(topic, false, bucketIndex);
    }

    public @NotNull TopicSubscribers findTopicSubscribers(
            final @NotNull String topic,
            final boolean excludeRootLevelWildcard,
            final int bucketIndex) {

        final ImmutableList.Builder<SubscriberWithQoS> subscribers = ImmutableList.builder();
        final ImmutableSet.Builder<String> sharedSubscriptions = ImmutableSet.builder();

        final ClientQueueDispatchingSubscriptionInfoFinder subscriberConsumer = new ClientQueueDispatchingSubscriptionInfoFinder(subscribers, sharedSubscriptions);

        findSubscribers(topic, excludeRootLevelWildcard, subscriberConsumer, bucketIndex);

        final ImmutableSet<SubscriberWithIdentifiers> distinctSubscribers = createDistinctSubscribers(subscribers.build());

        return new TopicSubscribers(distinctSubscribers, sharedSubscriptions.build());
    }

    private void findSubscribers(
            final @NotNull String topic,
            final boolean excludeRootLevelWildcard,
            final @NotNull SubscriptionsConsumer subscriberAndTopicConsumer,
            final int bucketIndex) {

        checkNotNull(topic, "Topic must not be null");

        //Root wildcard subscribers always match
        if (!excludeRootLevelWildcard) {
            subscriberAndTopicConsumer.acceptRootState(rootWildcardSubscribers);
        }

        //This is a shortcut in case there are no nodes beside the root node
        if (buckets[bucketIndex].isEmpty() || topic.isEmpty()) {
            return;
        }

        final String[] topicPart = StringUtils.splitPreserveAllTokens(topic, '/');
        final String segmentKey = topicPart[0];

        TopicTreeNode firstSegmentNode = buckets[bucketIndex].get(segmentKey);
        if (firstSegmentNode != null) {
            traverseTree(firstSegmentNode, subscriberAndTopicConsumer, topicPart, 0);
        }

        //We now have to traverse the wildcard node if something matches here
        if (!excludeRootLevelWildcard) {

            firstSegmentNode = buckets[bucketIndex].get("+");
            if (firstSegmentNode != null) {
                traverseTree(firstSegmentNode, subscriberAndTopicConsumer, topicPart, 0);
            }
        }
    }

    /**
     * Add a subscriber to the first given builder.
     * In case it is a shared subscription, the shared subscription string is added to the second builder.
     */
    private static void addWithTopicFilter(
            final @NotNull ImmutableList.Builder<SubscriberWithQoS> subscribersBuilder,
            final @NotNull ImmutableSet.Builder<String> sharedSubscriptionsBuilder,
            final @NotNull SubscriberWithQoS subscriber,
            final @NotNull String topicFilter) {

        if (subscriber.isSharedSubscription()) {
            final String sharedSubscription = subscriber.getSharedName() + "/" + topicFilter;
            sharedSubscriptionsBuilder.add(sharedSubscription);
        } else {
            subscribersBuilder.add(subscriber);
        }
    }

    /**
     * Returns a distinct immutable Set of SubscribersWithQoS. The set is guaranteed to only contain one entry per
     * subscriber string. This entry has the maximum QoS found in the topic tree and the subscription identifiers of all
     * subscriptions for the client.
     *
     * @param subscribers a list of subscribers
     * @return a immutable Set of distinct Subscribers with the maximum QoS.
     */
    static @NotNull ImmutableSet<SubscriberWithIdentifiers> createDistinctSubscribers(
            final @NotNull ImmutableList<SubscriberWithQoS> subscribers) {

        final ImmutableSet.Builder<SubscriberWithIdentifiers> newSet = ImmutableSet.builder();

        final ImmutableList<SubscriberWithQoS> subscriberWithQoS = ImmutableList.sortedCopyOf(Comparator.naturalOrder(), subscribers);

        final Iterator<SubscriberWithQoS> iterator = subscriberWithQoS.iterator();

        SubscriberWithIdentifiers last = null;

        // Create a single entry per client id, with the highest QoS an all subscription identifiers
        while (iterator.hasNext()) {
            final SubscriberWithQoS current = iterator.next();

            if (last != null) {

                if (!equalSubscription(current, last)) {
                    newSet.add(last);
                    last = new SubscriberWithIdentifiers(current);
                } else {
                    last.setQos(current.getQos());
                    if (current.getSubscriptionIdentifier() != null) {
                        final ImmutableIntArray subscriptionIds = last.getSubscriptionIdentifier();
                        final Integer subscriptionId = current.getSubscriptionIdentifier();
                        final ImmutableIntArray mergedSubscriptionIds = ImmutableIntArray.builder(subscriptionIds.length() + 1)
                                .addAll(subscriptionIds)
                                .add(subscriptionId)
                                .build();
                        last.setSubscriptionIdentifiers(mergedSubscriptionIds);
                    }
                }
            } else {
                last = new SubscriberWithIdentifiers(current);
            }

            if (!iterator.hasNext()) {
                newSet.add(last);
            }
        }

        return newSet.build();
    }

    private static boolean equalSubscription(
            final @NotNull SubscriberWithQoS first,
            final @NotNull SubscriberWithIdentifiers second) {

        return equalSubscription(first, second.getSubscriber(), second.getTopicFilter(),
                second.getSharedName());
    }

    private static boolean equalSubscription(
            final @NotNull SubscriberWithQoS first,
            final @NotNull SubscriberWithQoS second) {

        return equalSubscription(first, second.getSubscriber(), second.getTopicFilter(),
                second.getSharedName());
    }

    private static boolean equalSubscription(
            final @NotNull SubscriberWithQoS first,
            final @NotNull String secondClient,
            final @Nullable String secondTopicFilter,
            final @Nullable String secondSharedName) {

        if (!first.getSubscriber().equals(secondClient)) {
            return false;
        }
        if (!Objects.equals(first.getTopicFilter(), secondTopicFilter)) {
            return false;
        }
        return Objects.equals(first.getSharedName(), secondSharedName);
    }

    private static void traverseTree(
            final @NotNull TopicTreeNode node,
            final @NotNull SubscriptionsConsumer subscriberAndTopicConsumer,
            final @NotNull String @NotNull [] topicPart,
            final int depth) {

        if (!topicPart[depth].equals(node.getTopicPart()) && !"+".equals(node.getTopicPart())) {
            return;
        }

        subscriberAndTopicConsumer.acceptNonRootState(node.wildcardSubscriptions);

        final boolean end = topicPart.length - 1 == depth;
        if (end) {
            subscriberAndTopicConsumer.acceptNonRootState(node.exactSubscriptions);
        } else {
            if (getChildrenCount(node) == 0) {
                return;
            }

            final int nextDepth = depth + 1;

            //if the node has an index, we can just use the index instead of traversing the whole node set
            if (node.getChildrenMap() != null) {

                //Get the exact node by the index
                final TopicTreeNode matchingChildNode = getIndexForChildNode(topicPart[nextDepth], node);
                if (matchingChildNode != null) {
                    traverseTree(matchingChildNode, subscriberAndTopicConsumer, topicPart, nextDepth);
                }

                //We also need to check if there is a wildcard node
                final TopicTreeNode matchingWildcardNode = getIndexForChildNode("+", node);
                if (matchingWildcardNode != null) {
                    traverseTree(matchingWildcardNode, subscriberAndTopicConsumer, topicPart, nextDepth);
                }
                //We can return without any further recursion because we found all matching nodes
                return;
            }

            //The children are stored as array
            final TopicTreeNode[] children = node.getChildren();
            if (children == null) {
                return;
            }

            for (final TopicTreeNode childNode : children) {
                if (childNode != null) {
                    traverseTree(childNode, subscriberAndTopicConsumer, topicPart, nextDepth);
                }
            }
        }
    }

    private static @Nullable TopicTreeNode getIndexForChildNode(final @NotNull String key, final @NotNull TopicTreeNode node) {
        final Map<String, TopicTreeNode> childrenMap = node.getChildrenMap();
        if (childrenMap == null) {
            return null;
        }
        return childrenMap.get(key);
    }

    /* ***************************************
        Subscriber Removal for all nodes
     ****************************************/

    /**
     * Remove all subscriptions that meet the specified condition
     *
     * @param condition   that match the subscription to remove
     * @param bucketIndex in which the subscribers are removed,
     */
    public void removeSubscriber(final @NotNull BiPredicate<SubscriberWithQoS, String> condition, final int bucketIndex) {
        checkNotNull(condition, "condition must not be null");

        removeRootWildcardSubscriber(condition);
        //We can remove the segment if it's not needed anymore
        buckets[bucketIndex].values().removeIf(node -> removeSubscriberFromAllSubnodes(node, condition, ""));
    }

    /**
     * removes the specified client from the root wildcard subscribers
     *
     * @param subscriber the clientId
     * @return if there was already a subscription for this client
     */
    private boolean removeRootWildcardSubscriber(final @NotNull String subscriber, final @Nullable String sharedName) {
        final ImmutableList.Builder<SubscriberWithQoS> foundSubscribers = ImmutableList.builder();
        int sharedSubscriptionCount = 0;
        for (final SubscriberWithQoS rootWildcardSubscriber : rootWildcardSubscribers) {
            if (rootWildcardSubscriber.getSubscriber().equals(subscriber) &&
                    Objects.equals(rootWildcardSubscriber.getSharedName(), sharedName)) {
                foundSubscribers.add(rootWildcardSubscriber);
                if (rootWildcardSubscriber.isSharedSubscription()) {
                    sharedSubscriptionCount++;
                }
            }
        }
        final ImmutableList<SubscriberWithQoS> foundSubscriberList = foundSubscribers.build();
        rootWildcardSubscribers.removeAll(foundSubscriberList);
        counters.getSubscriptionCounter().dec(foundSubscriberList.size());
        counters.getSharedSubscriptionCounter().dec(sharedSubscriptionCount);
        return !foundSubscriberList.isEmpty();
    }

    /**
     * removes the clients from the root wildcard subscribers that match the condition
     *
     * @param condition a condition which is checked for every subscriber
     */
    private void removeRootWildcardSubscriber(final @NotNull BiPredicate<SubscriberWithQoS, String> condition) {
        final ImmutableList.Builder<SubscriberWithQoS> foundSubscribers = ImmutableList.builder();
        int sharedSubscriptionCount = 0;
        for (final SubscriberWithQoS rootWildcardSubscriber : rootWildcardSubscribers) {
            if (condition.test(rootWildcardSubscriber, "#")) {
                foundSubscribers.add(rootWildcardSubscriber);
                if (rootWildcardSubscriber.isSharedSubscription()) {
                    sharedSubscriptionCount++;
                }
            }
        }
        final ImmutableList<SubscriberWithQoS> foundSubscriberList = foundSubscribers.build();
        rootWildcardSubscribers.removeAll(foundSubscriberList);
        counters.getSubscriptionCounter().dec(foundSubscriberList.size());
        counters.getSharedSubscriptionCounter().dec(sharedSubscriptionCount);
    }

    private boolean removeSubscriberFromAllSubnodes(
            final @NotNull TopicTreeNode node,
            final @NotNull BiPredicate<SubscriberWithQoS, String> condition,
            final @NotNull String topic) {

        node.exactSubscriptions.removeSubscriberByCondition(condition, topic + node.getTopicPart(), counters);
        node.wildcardSubscriptions.removeSubscriberByCondition(condition, topic + node.getTopicPart() + "/#", counters);

        final Map<String, TopicTreeNode> childrenMap = node.getChildrenMap();
        if (childrenMap != null) {
            childrenMap.entrySet().removeIf(entry -> entry == null ||
                    removeSubscriberFromAllSubnodes(entry.getValue(), condition, topic + node.getTopicPart() + "/"));
        }

        final TopicTreeNode[] children = node.getChildren();
        if (children != null) {
            //Since we don't have any topics
            for (int i = 0; i < children.length; i++) {
                final TopicTreeNode child = children[i];
                if (child != null) {

                    final boolean canGetRemoved = removeSubscriberFromAllSubnodes(child, condition, topic + node.getTopicPart() + "/");
                    if (canGetRemoved) {
                        children[i] = null;
                    }
                }
            }
        }

        /* If there are no children available and no one subscribes (neither wildcard nor exact),
           this makes a good candidate for deletion */
        return node.isNodeEmpty();
    }


    /* ***************************************
        Subscriber Removal for single topic
     ****************************************/

    /**
     * Remove a subscription for a client
     *
     * @param subscriber  for which the subscription should be removed
     * @param topic       of the subscription
     * @param sharedName  of the subscription or null if it is not a shared subscription
     * @param bucketIndex
     */
    public void removeSubscriber(
            final @NotNull String subscriber,
            final @NotNull String topic,
            final @Nullable String sharedName,
            final int bucketIndex) {

        checkNotNull(subscriber);
        checkNotNull(topic);

        if ("#".equals(topic)) {
            removeRootWildcardSubscriber(subscriber, sharedName);
            return;
        }
        //We can shortcut here in case we don't have any segments
        if (buckets[bucketIndex].isEmpty()) {
            return;
        }

        if (topic.isEmpty()) {
            log.debug("Tried to remove an empty topic from the topic tree.");
            return;
        }

        final String[] topicPart = StringUtils.splitPreserveAllTokens(topic, "/");

        final TopicTreeNode[] nodes = new TopicTreeNode[topicPart.length];
        final String segmentKey = topicPart[0];

        //The segment doesn't exist, we can abort
        final TopicTreeNode segmentNode = buckets[bucketIndex].get(segmentKey);
        if (segmentNode == null) {
            return;
        }

        if (topicPart.length == 1) {
            segmentNode.exactSubscriptions.removeSubscriber(subscriber, sharedName, topic, counters);
        }

        if (topicPart.length == 2 && "#".equals(topicPart[1])) {
            segmentNode.wildcardSubscriptions.removeSubscriber(subscriber, sharedName, topic, counters);
        }

        iterateChildNodesForSubscriberRemoval(segmentNode, topicPart, nodes, 0);

        final TopicTreeNode lastFoundNode = getLastNode(nodes);
        if (lastFoundNode != null) {
            final String lastTopicPart = topicPart[topicPart.length - 1];
            if ("#".equals(lastTopicPart)) {
                lastFoundNode.wildcardSubscriptions.removeSubscriber(subscriber, sharedName, topic, counters);

            } else if (lastTopicPart.equals(lastFoundNode.getTopicPart())) {
                lastFoundNode.exactSubscriptions.removeSubscriber(subscriber, sharedName, topic, counters);
            }
        }

        //Delete all nodes recursively if they are not needed anymore

        for (int i = nodes.length - 1; i > 0; i--) {
            final TopicTreeNode node = nodes[i];
            if (node != null) {

                if (node.isNodeEmpty()) {
                    TopicTreeNode parent = nodes[i - 1];
                    if (parent == null) {
                        parent = segmentNode;
                    }
                    final TopicTreeNode[] childrenOfParent = parent.getChildren();
                    if (childrenOfParent != null) {
                        for (int j = 0; j < childrenOfParent.length; j++) {
                            if (childrenOfParent[j] == node) {
                                childrenOfParent[j] = null;
                            }
                        }
                    } else if (parent.getChildrenMap() != null) {
                        final TopicTreeNode childOfParent = parent.getChildrenMap().get(node.getTopicPart());
                        if (childOfParent == node) {
                            parent.getChildrenMap().remove(childOfParent.getTopicPart());
                        }
                    }
                }
            }
        }
        //We can remove the segment if it's not needed anymore
        if (getChildrenCount(segmentNode) == 0 &&
                segmentNode.exactSubscriptions.getSubscriberCount() == 0 &&
                segmentNode.wildcardSubscriptions.getSubscriberCount() == 0) {
            buckets[bucketIndex].remove(segmentNode.getTopicPart());
        }
    }

    private static @Nullable TopicTreeNode getLastNode(final @Nullable TopicTreeNode @NotNull [] nodes) {
        //Search for the last node which is not null
        for (int i = nodes.length - 1; i >= 0; i--) {
            final TopicTreeNode node = nodes[i];

            if (node != null) {
                return node;
            }
        }
        return null;
    }

    /**
     * Recursively iterates all children nodes of a given node and does a look-up if one of the children nodes matches
     * the next topic level. If this is the case, the next node will be added to the given nodes array.
     * <p>
     * Please
     *
     * @param node       the node to iterate children for
     * @param topicParts the complete topic array
     * @param results    the result array
     * @param depth      the current topic level depth
     */
    private static void iterateChildNodesForSubscriberRemoval(
            final @NotNull TopicTreeNode node,
            final String @NotNull [] topicParts,
            final TopicTreeNode @NotNull [] results,
            final int depth) {

        //Note dobermai: We don't need to check for "+" subscribers explicitly, because unsubscribes are always absolute

        TopicTreeNode foundNode = null;

        if (node.getChildrenMap() != null) {
            if (topicParts.length > depth + 1) {
                //We have an index available, so we can use it
                final TopicTreeNode indexNode = node.getChildrenMap().get(topicParts[depth + 1]);
                if (indexNode == null) {
                    //No child topic found, we can abort
                    return;
                } else {
                    foundNode = indexNode;
                }
            }
        } else if (node.getChildren() != null) {

            //No index available, we must iterate all child nodes

            for (int i = 0; i < node.getChildren().length; i++) {
                final TopicTreeNode child = node.getChildren()[i];
                if (child != null && depth + 2 <= topicParts.length) {

                    if (child.getTopicPart().equals(topicParts[depth + 1])) {
                        foundNode = child;
                        break;
                    }
                }
            }
        }

        if (foundNode != null) {
            //Child node found, traverse the tree one level deeper
            results[depth + 1] = foundNode;
            iterateChildNodesForSubscriberRemoval(foundNode, topicParts, results, depth + 1);
        }
    }

    /**
     * Returns all the subscribers that share a given subscription.
     *
     * @param shareName   of the shared subscription
     * @param topicFilter of the shared subscription
     * @param bucketIndex
     * @return all subscriber that share the given subscription
     */
    public @NotNull ImmutableSet<SubscriberWithQoS> getSharedSubscription(
            final @NotNull String shareName,
            final @NotNull String topicFilter,
            final int bucketIndex) {

        return getSubscriptionsByTopicFilter(topicFilter, subscriber -> subscriber.isSharedSubscription()
                && subscriber.getSharedName() != null
                && subscriber.getSharedName().equals(shareName), bucketIndex);
    }

    /**
     * All subscribers that have subscribed to this exact topic filter
     *
     * @param topicFilter the topic filter (including wildcards)
     * @param bucketIndex
     * @return the subscribers with a subscription with this topic filter
     */
    public @NotNull ImmutableSet<String> getSubscribersWithFilter(
            final @NotNull String topicFilter,
            final @NotNull Predicate<SubscriberWithQoS> itemFilter,
            final int bucketIndex) {

        return createDistinctSubscriberIds(getSubscriptionsByTopicFilter(topicFilter, itemFilter, bucketIndex));
    }

    /**
     * All subscribers that have a subscription matching this topic
     *
     * @param topic                    the topic to match (no wildcards)
     * @param itemFilter
     * @param excludeRootLevelWildcard
     * @param bucketIndex
     * @return the subscribers with a subscription for this topic
     */
    public @NotNull ImmutableSet<String> getSubscribersForTopic(
            final @NotNull String topic,
            final @NotNull Predicate<SubscriberWithQoS> itemFilter,
            final boolean excludeRootLevelWildcard,
            final int bucketIndex) {

        checkNotNull(topic, "Topic must not be null");

        final ImmutableSet.Builder<String> subscribers = ImmutableSet.builder();

        //Root wildcard subscribers always match
        if (!excludeRootLevelWildcard) {
            for (final SubscriberWithQoS rootWildcardSubscriber : rootWildcardSubscribers) {
                addAfterItemCallback(itemFilter, subscribers, rootWildcardSubscriber);
            }
        }

        //This is a shortcut in case there are no nodes beside the root node
        if (buckets[bucketIndex].isEmpty() || topic.isEmpty()) {
            return subscribers.build();
        }

        final String[] topicPart = StringUtils.splitPreserveAllTokens(topic, '/');
        final String segmentKey = topicPart[0];

        TopicTreeNode firstSegmentNode = buckets[bucketIndex].get(segmentKey);
        if (firstSegmentNode != null) {
            traverseTreeWithFilter(firstSegmentNode, subscribers, topicPart, 0, itemFilter);
        }

        //We now have to traverse the wildcard node if something matches here
        if (!excludeRootLevelWildcard) {

            firstSegmentNode = buckets[bucketIndex].get("+");
            if (firstSegmentNode != null) {
                traverseTreeWithFilter(firstSegmentNode, subscribers, topicPart, 0, itemFilter);
            }
        }

        return subscribers.build();
    }

    private static void traverseTreeWithFilter(
            final @NotNull TopicTreeNode node,
            final @NotNull ImmutableSet.Builder<String> subscribers,
            final String[] topicPart,
            final int depth,
            final @NotNull Predicate<SubscriberWithQoS> itemFilter) {

        if (!topicPart[depth].equals(node.getTopicPart()) && !"+".equals(node.getTopicPart())) {
            return;
        }

        node.wildcardSubscriptions.populateWithSubscriberNamesUsingFilter(itemFilter, subscribers);

        final boolean end = topicPart.length - 1 == depth;
        if (end) {
            node.exactSubscriptions.populateWithSubscriberNamesUsingFilter(itemFilter, subscribers);
        } else {
            if (getChildrenCount(node) == 0) {
                return;
            }

            //if the node has an index, we can just use the index instead of traversing the whole node set
            if (node.getChildrenMap() != null) {

                //Get the exact node by the index
                final TopicTreeNode matchingChildNode = getIndexForChildNode(topicPart[depth + 1], node);
                //We also need to check if there is a wildcard node
                final TopicTreeNode matchingWildcardNode = getIndexForChildNode("+", node);

                if (matchingChildNode != null) {
                    traverseTreeWithFilter(matchingChildNode, subscribers, topicPart, depth + 1, itemFilter);
                }

                if (matchingWildcardNode != null) {
                    traverseTreeWithFilter(matchingWildcardNode, subscribers, topicPart, depth + 1, itemFilter);
                }
                //We can return without any further recursion because we found all matching nodes
                return;
            }

            //The children are stored as array
            final TopicTreeNode[] children = node.getChildren();
            if (children == null) {
                return;
            }

            for (final TopicTreeNode childNode : children) {
                if (childNode != null) {
                    traverseTreeWithFilter(childNode, subscribers, topicPart, depth + 1, itemFilter);
                }
            }
        }
    }

    private static @NotNull ImmutableSet<String> createDistinctSubscriberIds(
            final @NotNull ImmutableSet<SubscriberWithQoS> subscriptionsByFilters) {

        final ImmutableSet.Builder<String> builder = ImmutableSet.builderWithExpectedSize(subscriptionsByFilters.size());
        for (final SubscriberWithQoS subscription : subscriptionsByFilters) {
            builder.add(subscription.getSubscriber());
        }
        return builder.build();
    }

    private @NotNull ImmutableSet<SubscriberWithQoS> getSubscriptionsByTopicFilter(
            final @NotNull String topicFilter,
            final @NotNull Predicate<SubscriberWithQoS> itemFilter,
            final int bucketIndex) {

        final ImmutableSet.Builder<SubscriberWithQoS> subscribers = ImmutableSet.builder();
        if ("#".equals(topicFilter)) {
            for (final SubscriberWithQoS rootWildcardSubscriber : rootWildcardSubscribers) {
                addAfterCallback(itemFilter, subscribers, rootWildcardSubscriber);
            }
            return subscribers.build();
        }

        final String[] contents = StringUtils.splitPreserveAllTokens(topicFilter, '/');
        final String firstSegment = contents[0];

        TopicTreeNode node = buckets[bucketIndex].get(firstSegment);
        if (node == null) {
            return subscribers.build();
        }

        contentLoop:
        for (int i = 1; i < contents.length; i++) {
            if ("#".equals(contents[i])) {
                break;
            }

            if (node.getChildren() == null && node.getChildrenMap() == null) {
                // No matching node in the topic tree
                return subscribers.build();
            }

            final TopicTreeNode[] children = node.getChildren();
            if (children != null) {
                for (final TopicTreeNode child : children) {
                    if (child != null && child.getTopicPart().equals(contents[i])) {
                        node = child;
                        continue contentLoop;
                    }
                    // No matching node in the topic tree
                }
                return subscribers.build();
            } else if (node.getChildrenMap() != null) {

                for (final TopicTreeNode child : node.getChildrenMap().values()) {
                    if (child != null && child.getTopicPart().equals(contents[i])) {
                        node = child;
                        continue contentLoop;
                    }
                    // No matching node in the topic tree
                }
            }
            return subscribers.build();
        }

        if ("#".equals(contents[contents.length - 1])) {
            node.wildcardSubscriptions.populateWithSubscribersUsingFilter(itemFilter, subscribers);
        } else {
            node.exactSubscriptions.populateWithSubscribersUsingFilter(itemFilter, subscribers);
        }
        return subscribers.build();
    }

    private static void addAfterCallback(
            final @NotNull Predicate<SubscriberWithQoS> itemFilter,
            final @NotNull ImmutableSet.Builder<SubscriberWithQoS> subscribers,
            final @Nullable SubscriberWithQoS subscriber) {

        if (subscriber != null) {
            if (itemFilter.test(subscriber)) {
                subscribers.add(subscriber);
            }
        }
    }

    private static void addAfterItemCallback(
            final @NotNull Predicate<SubscriberWithQoS> itemFilter,
            final @NotNull ImmutableSet.Builder<String> subscribers,
            final @Nullable SubscriberWithQoS subscriber) {

        if (subscriber != null) {
            if (itemFilter.test(subscriber)) {
                subscribers.add(subscriber.getSubscriber());
            }
        }
    }

    /**
     * Get the subscription for a specific client matching a given topic.
     * The returned subscription has the highest qos of all matching subscriptions and all its identifiers.
     *
     * @param client      of the subscription
     * @param topic       matching the subscriptions
     * @param bucketIndex
     * @return {@link SubscriberWithIdentifiers} with the highest QoS and all identifiers of the matching subscriptions
     * or null if no subscription match for the client
     */
    public @Nullable SubscriberWithIdentifiers findSubscriber(
            final @NotNull String client,
            final @NotNull String topic,
            final int bucketIndex) {

        final ClientPublishDeliverySubscriptionInfoFinder subscriberConsumer = new ClientPublishDeliverySubscriptionInfoFinder(client);

        findSubscribers(topic, false, subscriberConsumer, bucketIndex);

        return subscriberConsumer.getMatchingSubscriber();
    }

    /* *************
        Utilities
     **************/

    /**
     * Returns the number of children of a node.
     *
     * @param node the node
     * @return the number of children nodes for the given node
     */
    public static int getChildrenCount(final @NotNull TopicTreeNode node) {
        checkNotNull(node, "Node must not be null");

        //If the node has a children map instead of the array, we don't need to count
        if (node.childrenMap != null) {
            return node.childrenMap.size();
        }

        final TopicTreeNode[] children = node.getChildren();

        if (children == null) {
            return 0;
        }

        int count = 0;
        for (final TopicTreeNode child : children) {
            if (child != null) {
                count++;
            }
        }
        return count;
    }

    interface SubscriptionsConsumer {

        /**
         * Processes the subscription information in the nodes of the topic tree.
         *
         * @param matchingNodeSubscriptions subscriptions that are stored within the topic tree node.
         */
        void acceptNonRootState(final @NotNull MatchingNodeSubscriptions matchingNodeSubscriptions);

        /**
         * Processes the subscription information of the root wildcard subscriptions, i.e. subscriptions to the # topic filter.
         *
         * @param rootWildcardSubscriptions root wildcard subscriptions of the topic tree.
         */
        void acceptRootState(final @NotNull List<SubscriberWithQoS> rootWildcardSubscriptions);
    }

    /**
     * Filters subscription information for the purpose of dispatching the incoming PUBLISH control packet to the client queues.
     * Inbound flow.
     */
    static class ClientQueueDispatchingSubscriptionInfoFinder implements SubscriptionsConsumer {

        private final @NotNull ImmutableList.Builder<SubscriberWithQoS> subscribersBuilder;
        private final @NotNull ImmutableSet.Builder<String> sharedSubscriptionsBuilder;

        ClientQueueDispatchingSubscriptionInfoFinder(
                final @NotNull ImmutableList.Builder<SubscriberWithQoS> subscribersBuilder,
                final @NotNull ImmutableSet.Builder<String> sharedSubscriptionsBuilder) {
            this.subscribersBuilder = subscribersBuilder;
            this.sharedSubscriptionsBuilder = sharedSubscriptionsBuilder;
        }

        @Override
        public void acceptNonRootState(final @NotNull MatchingNodeSubscriptions matchingNodeSubscriptions) {

            sharedSubscriptionsBuilder.addAll(matchingNodeSubscriptions.sharedSubscribersMap.keySet());

            if (matchingNodeSubscriptions.nonSharedSubscribersMap != null) {
                subscribersBuilder.addAll(matchingNodeSubscriptions.nonSharedSubscribersMap.values());
            } else if (matchingNodeSubscriptions.nonSharedSubscribersArray != null) {
                for (final SubscriberWithQoS exactSubscriber : matchingNodeSubscriptions.nonSharedSubscribersArray) {
                    if (exactSubscriber != null) {
                        subscribersBuilder.add(exactSubscriber);
                    }
                }
            }
        }

        @Override
        public void acceptRootState(final @NotNull List<SubscriberWithQoS> rootWildcardSubscriptions) {
            for (final SubscriberWithQoS rootWildcardSubscriber : rootWildcardSubscriptions) {
                if (rootWildcardSubscriber.isSharedSubscription()) {
                    sharedSubscriptionsBuilder.add(rootWildcardSubscriber.getSharedName() + "/#");
                } else {
                    subscribersBuilder.add(rootWildcardSubscriber);
                }
            }
        }
    }

    /**
     * Filters subscription information for the purpose of delivering PUBLISH control packet to the subscriber.
     * Outbound flow.
     */
    private static class ClientPublishDeliverySubscriptionInfoFinder implements SubscriptionsConsumer {

        private final @NotNull String client;
        private @Nullable SubscriberWithIdentifiers sharedSubscriber;
        private final @NotNull ImmutableList.Builder<SubscriberWithQoS> subscribers = ImmutableList.builder();
        private boolean nonSharedSubscriberFound;

        ClientPublishDeliverySubscriptionInfoFinder(final @NotNull String client) {
            this.client = client;
        }

        @Override
        public void acceptNonRootState(final @NotNull MatchingNodeSubscriptions matchingNodeSubscriptions) {

            final Stream<SubscriberWithQoS> nonSharedSubscriptions = matchingNodeSubscriptions.getNonSharedSubscriptionsStream();
            if (nonSharedSubscriptions != null) {
                nonSharedSubscriptions
                        .filter(subscriberWithQoS -> subscriberWithQoS.getSubscriber().equals(client))
                        .forEach(subscriberWithQoS -> {
                            subscribers.add(subscriberWithQoS);
                            nonSharedSubscriberFound = true;
                        });
            }

            // If no non-shared subscriptions for the provided subscriber client were found SO FAR,
            // take the shared subscription with the highest QoS level.
            //
            // Note: if the non-shared subscriptions are found later on,
            // they will override the shared subscriptions (see getMatchingSubscriber method)!
            if (!nonSharedSubscriberFound) {
                matchingNodeSubscriptions.getSharedSubscriptionsStream()
                        .filter(subscriberWithQoS -> subscriberWithQoS.getSubscriber().equals(client))
                        .forEach(subscriberWithQoS -> {
                            if (sharedSubscriber == null || sharedSubscriber.getQos() < subscriberWithQoS.getQos()) {
                                sharedSubscriber = new SubscriberWithIdentifiers(subscriberWithQoS);
                            }
                        });
            }
        }

        @Override
        public void acceptRootState(final @NotNull List<SubscriberWithQoS> rootWildcardSubscriptions) {
            for (final SubscriberWithQoS rootWildcardSubscriber : rootWildcardSubscriptions) {
                if (rootWildcardSubscriber.getSubscriber().equals(client)) {
                    if (!rootWildcardSubscriber.isSharedSubscription()) {
                        subscribers.add(rootWildcardSubscriber);
                        nonSharedSubscriberFound = true;
                    } else if (!nonSharedSubscriberFound &&
                            (sharedSubscriber == null || sharedSubscriber.getQos() < rootWildcardSubscriber.getQos())) {
                        sharedSubscriber = new SubscriberWithIdentifiers(rootWildcardSubscriber);
                    }
                }
            }
        }

        public @Nullable SubscriberWithIdentifiers getMatchingSubscriber() {
            final ImmutableList<SubscriberWithQoS> subscribers = this.subscribers.build();
            if (subscribers.isEmpty()) {
                return sharedSubscriber;
            } else {
                return createDistinctSubscribers(subscribers).asList().get(0);
            }
        }
    }
}
