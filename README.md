
# MQTT Tree
MQTT Tree is a dependency free java implementation, designed to be used for storing, matching and retrieving MQTT subscriptions
in a highly concurrent environment.

## Table of Contents
1. [About](#about)
2. [Memory & Time Performance](#memory--time-performance)
3. [Benchmarking](#benchmarking)
4. [Example Usage](#example-usage)
   
## About
Broker and gateway implementations of MQTT will all need the ability to quickly and efficiently look up subscriptions. Subscriptions
should be indexed (these contain wildcard characters) and then PUBLISH paths (which do not) ask the tree for all those subscriptions
held matching the PUBLISH topic.

I have found myself writing this same code a number of times for different broker/GW implementations, so it made sense to make it 
available for others who may need to same thing.

Typical use would be to index your client session subscriptions using this tree for exceptionally fast lookup of subscriptions
in high load environments. Modifications and Reads are entirely synchronized and can be performed without external synchronization.

## Memory & Time Performance
The tree uses a Tries algorithm internally (prefix tree), where each level of the tree is a normalized path segment. Thus a million subscriptions to the
same topic path would only result in that topic path being stored once. Branch nodes along the path will diverge where new path segments are added. Therefore
search performance and modification performance are a function of the number of levels of the tree traversed.

Memberships to a given leaf node (subscriptions) are purposefully arbitrary types, allowing the calling code to specify their own member types. The stipulation is 
that members are maintained in a HashSet, therefore ***the equals and hashCode methods of any chosen member type should be consistent***. 

## Benchmarking
I have yet to undertake a formal benchmark of the tree, but I would be happy to hear the results should someone else wish to compare against
other implementations. I would advise that for a real comparison, a benchmark should incorporate concurrent read/updates to the tree.

## Example Usage

```java
public class Example {
    public static void main(String[] args) throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = new MqttTree<String>(MqttTree.DEFAULT_SPLIT, true);
        tree.withWildcard(MqttTree.DEFAULT_WILDCARD);
        tree.withWildpath(MqttTree.DEFAULT_WILDPATH);

        tree.withMaxPathSegments(1024);
        tree.withMaxMembersAtLevel(1024);

        tree.addSubscription("/this/is/a/topic", "ClientId1", "ClientId2");

        tree.addSubscription("/this/+/a/topic", "ClientId3");

        tree.addSubscription("/this/#", "ClientId4");

        Set<String> m = tree.search("/this/is/a/topic");

        System.out.println(String.format("matching search had [%s] members", m.size()));

        m = tree.search("/is/a/different/topic");

        System.out.println(String.format("non-matching search had [%s] members", m.size()));

        tree.removeSubscriptionFromPath("/this/is/a/topic", "ClientId2");

        m = tree.search("/this/is/a/topic");

        System.out.println(String.format("matching search had [%s] members", m.size()));
    }
}
```
