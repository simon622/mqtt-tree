package org.slj.mqtt.tree;

import java.util.Set;

public interface IMqttTree<T> {

    void addSubscription(final String path, final T... members)
            throws MqttTreeException, MqttTreeLimitExceededException;

    boolean removeSubscriptionFromPath(final String path, T member)
            throws MqttTreeException;

    Set<T> search(final String path);

    boolean hasMembers(final String path);

    boolean hasPath(String path);

    MqttTreeNode<T> getRootNode();

    void visit(MqttTreeNodeVisitor visitor);

    Set<String> getDistinctPaths(boolean considerMembership);

    int countDistinctPaths(boolean considerMembership);

    int getBranchCount();
}
