
# MQTT Tree
MQTT-TREE is a dependency free java implementation, designed to be used for storing, matching and retrieving MQTT subscriptions
in a highly concurrent environment.

## Table of Contents
1. [About](#about) 
2. [Example Usage](#example-usage)
   
## About
Broker and gateway implementations of MQTT will all need to ability to quickly and efficiently look up subscriptions. Subscriptions
should be indexed (these contain wildcard characters) and then PUBLISH paths (which do not) ask the tree for all those subscriptions
held matching the PUBLISH topic.

I have found myself writing this same code a number of times for different broker/GW implementations, so it made sense to make it 
available for others who may need to same thing.

Typical use would be to index your client session subscriptions using this tree for exceptionally fast lookup of subscriptions
in high load environments. Modifications and Reads are entirely synchronized and can be performed without external synchronization.

## Example Usage

```java
public class Example {
   public static void main(String[] args) throws MqttSubscriptionTreeLimitExceededException {
      MqttSubscriptionTree<String> tree = new MqttSubscriptionTree<String>(MqttSubscriptionTree.DEFAULT_SPLIT, true);
      tree.withWildcard(MqttSubscriptionTree.DEFAULT_WILDCARD);
      tree.withWildpath(MqttSubscriptionTree.DEFAULT_WILDPATH);

      tree.withMaxPathSegments(1024);
      tree.withMaxMembersAtLevel(1024);

      tree.addPath("/this/is/a/topic", "ClientId1", "ClientId2");

      tree.addPath("/this/+/a/topic", "ClientId3");

      tree.addPath("/this/#", "ClientId4");

      Set<String> m = tree.search("/this/is/a/topic");

      System.out.println(String.format("matching search had [%s] members", m.size()));

      m = tree.search("/is/a/different/topic");

      System.out.println(String.format("non-matching search had [%s] members", m.size()));
   }
}
```
