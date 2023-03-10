
![Logo](/ext/slj-mqtt-tree-sm.png)

# MQTT Tree
MQTT Tree is a dependency free java implementation, designed to be used for storing, matching and retrieving MQTT subscriptions
in a highly concurrent environment.

## Table of Contents
1. [About](#about)
2. [Specification Conformance](#specification-conformance)
2. [Memory & Time Performance](#memory--time-performance)
3. [Prefix Search](#prefix-search)
4. [Benchmarking](#benchmarking)
5. [Known Issues](#known-issues)
6. [Example Usage](#example-usage)
   
## About
Broker and gateway implementations of MQTT will all need the ability to quickly and efficiently look up subscriptions. Subscriptions
should be indexed (these contain wildcard characters) and then PUBLISH paths (which do not) ask the tree for all those subscriptions
held matching the PUBLISH topic.

I have found myself writing this same code a number of times for different broker/GW implementations, so it made sense to make it 
available for others who may need to same thing.

Typical use would be to index your client session subscriptions using this tree for exceptionally fast lookup of subscriptions
in high load environments. Modifications and Reads are entirely synchronized and can be performed without external synchronization.

## Specification Conformance
I have attempted to make the tree compliant with both the normative and non-normative conformance statements in the [MQTT Version 5 specification](http://docs.oasis-open.org/mqtt/mqtt/v5.0/cs01/mqtt-v5.0-cs01.pdf) document. You will find specification compliance unit tests located in [MqttSpecificationComplianceTests.java](/src/test/java/MqttSpecificationComplianceTests.java). Whilst these test pass, there are certain aspects of path segmentation matching that are subjective. I will be seeking clarification, but there are tests in the non-formal test class which demonstrate this.  

## Memory & Time Performance
The tree uses a Tries algorithm internally (prefix tree), where each level of the tree is a normalized path segment. Thus a million subscriptions to the
same topic path would only result in that topic path being stored once. Branch nodes along the path will diverge where new path segments are added. Therefore
search performance and modification performance are a function of the number of levels of the tree traversed.

Memberships to a given leaf node (subscriptions) are purposefully arbitrary types, allowing the calling code to specify their own member types. The stipulation is 
that members are maintained in a HashSet, therefore ***the equals and hashCode methods of any chosen member type should be consistent***. 

## Prefix Search
Being able to visualise the large datasets that are generated by topic trees is a constant struggle. I have included an optional wrapper class which you can choose to wrap your MqttTree in, which will index all of your paths and make them searchable against a single character prefix. I have done this using a radix tree, which indexes the topics during creation for look-ahead searching. This is a decent enough index for thousands of nodes, when there are millions, the overhead of the extra index will be burdensome, so use wisely. I have knocked together a quick Java swing application to show how the prefix searching works.

```java
//Wrap your tree in a searchable tree, and expose the new method
ISearchableMqttTree<String> searchableTree = new SearchableMqttTree(
                new MqttTree<String>(MqttTree.DEFAULT_SPLIT, true));

searchableTree.prefixSearch("some/pre", 100);
```

## Benchmarking
I have yet to undertake a formal benchmark of the tree, but I would be happy to hear the results should someone else wish to compare against
other implementations. I would advise that for a real comparison, a benchmark should incorporate concurrent read/updates to the tree.

## Known Issues
As outlined in the compliance section, certain aspects of path matching are open to interpretation. Where consecutive path segments are defined the tree will presently match separators in these locations. I will look to change this when time allows. Further, I will be looking to optimize the matching algorithm further. 

## Example Usage

```java
public class Example {
    public static void main(String[] args) throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = new MqttTree<String>(MqttTree.DEFAULT_SPLIT, true);
        tree.withMaxPathSegments(1024);
        tree.withMaxMembersAtLevel(1024);

        tree.subscribe("/this/is/a/topic", "ClientId1", "ClientId2");
        tree.subscribe("/this/+/a/topic", "ClientId3");
        tree.subscribe("/this/#", "ClientId4");
        Set<String> m = tree.search("/this/is/a/topic");
        System.out.println(String.format("matching search had [%s] members", m.size()));
        m = tree.search("/is/a/different/topic");
        System.out.println(String.format("non-matching search had [%s] members", m.size()));
        tree.unsubscribe("/this/is/a/topic", "ClientId2");
        m = tree.search("/this/is/a/topic");
        System.out.println(String.format("matching search had [%s] members", m.size()));
    }
}
```
