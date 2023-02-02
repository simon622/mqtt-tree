package org.slj.mqtt.tree;

import java.util.List;

/**
 * Provides index methods to search the Mqtt Tree character by character.
 */
public interface ISearchableMqttTree<T> extends IMqttTree<T>{

    List<MqttTreeNode<T>> prefixSearch(String path, int max);
}
