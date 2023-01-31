/*
 * Copyright (c) 2021 Simon Johnson <simon622 AT gmail DOT com>
 *
 * Find me on GitHub:
 * https://github.com/simon622
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.slj.mqtt.tree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TriesTree implementation designed to add members at each level of the tree and normalise the storage
 * retaining the separators as tree nodes per the specification.
 */
public class MqttTree<T> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    public static final char DEFAULT_SPLIT = '/';
    public static final String DEFAULT_WILDCARD = "#";
    public static final String DEFAULT_WILDPATH = "+";
    private static final int DEFAULT_MAX_PATH_SIZE = 1024;
    private static final int DEFAULT_MAX_PATH_SEGMENTS = 1024;
    private static final int DEFAULT_MAX_MEMBERS_AT_LEVEL = 1024 * 10;
    private final Set<String> wildcards = new HashSet<>(4);
    private final Set<String> wildpaths = new HashSet<>(4);
    private final boolean selfPruningTree;
    private final char split;
    private final TrieNode<T> root;
    private long maxPathSize = DEFAULT_MAX_PATH_SIZE;
    private long maxPathSegments = DEFAULT_MAX_PATH_SEGMENTS;
    private long maxMembersAtLevel = DEFAULT_MAX_MEMBERS_AT_LEVEL;

    /**
     * An MQTT subscription tree is a tries based thread-safe tree, designed for storage of arbitrary membership data at any given level
     * in the tree.
     *
     * The tree allows you to pass in configuration according to your requirements, but traditionally this would be constructed using '/'
     * as the path seperater. Wildcard and wildpath tokens should also be supplied.
     *
     * @param selfPruningTree - when removing members, when a leaf is determined to be empty subsequent to the removal operation, should the
     *                        tree at that level be pruned (where it is the last level of the tree)
     */
    public MqttTree(final char splitChar, final boolean selfPruningTree){
        this.split = splitChar;
        this.selfPruningTree = selfPruningTree;
        this.root = new TrieNode<T>( null, null);
    }

    public long getMaxPathSize() {
        return maxPathSize;
    }

    public MqttTree withMaxPathSize(long maxPathSize) {
        this.maxPathSize = maxPathSize;
        return this;
    }

    public long getMaxPathSegments() {
        return maxPathSegments;
    }

    public MqttTree withMaxPathSegments(long maxPathSegments) {
        this.maxPathSegments = maxPathSegments;
        return this;
    }

    public long getMaxMembersAtLevel() {
        return maxMembersAtLevel;
    }

    public MqttTree withMaxMembersAtLevel(long maxMembersAtLevel) {
        this.maxMembersAtLevel = maxMembersAtLevel;
        return this;
    }

    public MqttTree withWildcard(String wildcard){
        if(wildcard == null) throw new NullPointerException("wild card cannot be <null>");
        wildcards.add(wildcard);
        return this;
    }

    public MqttTree withWildpath(String wildpath){
        if(wildpaths == null) throw new NullPointerException("wild path cannot be <null>");
        wildpaths.add(wildpath);
        return this;
    }

    public void addSubscription(final String path, final T... members)
            throws MqttTreeException, MqttTreeLimitExceededException {

        if(path == null || path.length() == 0) throw new MqttTreeException("invalid subscription path");

        if(path.length() > maxPathSize)
            throw new MqttTreeLimitExceededException("cannot add paths lengths exceeding the configured max '"+maxPathSize+"' - ("+path.length()+")");

        String[] segments = split(path);

        if(segments.length > maxPathSegments)
            throw new MqttTreeLimitExceededException("cannot add paths exceeding the configured max segments '"+maxPathSegments+"' - ("+segments.length+")");

        if(members != null && members.length > maxMembersAtLevel)
            throw new MqttTreeLimitExceededException("cannot add paths with the number of members exceeding max '"+maxMembersAtLevel+"'");

        TrieNode<T> node = root;
        for (int i=0; i<segments.length; i++){
            if(i == segments.length - 1){
                node = node.addChild(segments[i], members);
            } else {
                node = node.addChild(segments[i]);
            }
        }
    }

    public boolean removeSubscriptionFromPath(final String path, T member) throws MqttTreeException {
        if(path == null || path.length() == 0) throw new MqttTreeException("invalid subscription path");
        TrieNode<T> node = getNodeIfExists(path);
        if(node != null){
            //if the leaf now contains no members AND its not a branch, cut the leaf off the tree
            boolean removed = node.removeMember(member);
            if(removed && selfPruningTree &&
                    node.getMembers().isEmpty() && !node.hasChildren()){
                node.getParent().removeChild(node);
            }
            return removed;
        }
        return false;
    }

    public Set<T> search(final String path)
            throws MqttTreeException {

        if(path == null || path.length() == 0) throw new MqttTreeException("cannot search <null> or empty path");
        for (String wildcard: wildcards) {
            if(path.contains(wildcard)) throw new MqttTreeException("search path cannot contain wildcard, please refer to MQTT specification");
        }

        for (String wildpath: wildpaths) {
            if(path.contains(wildpath)) throw new MqttTreeException("search path cannot contain wildpath, please refer to MQTT specification");
        }

        String[] segments = split(path);
        return searchTreeForMembers(root, segments);
    }

    public boolean hasMembers(final String path){
        TrieNode<T> node = getNodeIfExists(path);
        return node == null ? false: node.hasMembers();
    }

    public boolean hasPath(String path){
        return getNodeIfExists(path) != null;
    }

    protected final TrieNode<T> getNodeIfExists(final String path){

        String[] segments = split(path);
        TrieNode node = root;
        for (int i=0; i < segments.length; i++){
            node = node.getChild(segments[i]);
            if(node == null) {
                return null;
            }
        }
        return node;
    }

    public void visitChildren(MqttTreeNodeVisitor visitor) {
        visitChildren(root, visitor);
    }

    private static void visitChildren(MqttTreeNode node, MqttTreeNodeVisitor visitor) {
        if (node != null) {
            Set<String> children = node.getChildPaths();
            Iterator<String> itr = children.iterator();
            while (itr.hasNext()) {
                String path = itr.next();
                MqttTreeNode child = node.getChild(path);
                if (child == null) {
                    throw new RuntimeException("encountered invalid tree state");
                } else {
                    visitChildren(child, visitor);
                    visitor.visit(child);
                }
            }
        }
    }

    public MqttTreeNode<T> getRootNode(){
        return root;
    }

    public void visit(MqttTreeNodeVisitor visitor) {
        visit(root, visitor);
    }
    private static void visit(MqttTreeNode node, MqttTreeNodeVisitor visitor) {
        if (node != null) {
            visitor.visit(node);
            Set<String> children = node.getChildPaths();
            Iterator<String> itr = children.iterator();
            while (itr.hasNext()) {
                String path = itr.next();
                MqttTreeNode child = node.getChild(path);
                if (child == null) {
                    throw new RuntimeException("encountered invalid tree state");
                } else {
                    visit(child, visitor);
                }
            }
        }
    }

    private Set<T> searchTreeForMembers(TrieNode<T> node, String[] segments){

        if(node == null) throw new NullPointerException("cannot search a null node");

        Set<T> wildcardMembers = null;
        Set<T> wildSegmentMembers = null;
        Set<T> fullMembers = null;

        for (int i=0; i < segments.length; i++){
            if(!wildcards.isEmpty()){
                for (String wildcard: wildcards) {
                    TrieNode<T> wild = node.getChild(wildcard);
                    if(wild != null){
                        Set<T> wildcardMembersAtLevel = wild.getMembers();
                        if(wildcardMembersAtLevel != null && !wildcardMembersAtLevel.isEmpty()){
                            if(wildcardMembers == null) wildcardMembers = new HashSet<>();
                            wildcardMembers.addAll(wildcardMembersAtLevel);
                        }
                    }
                }
            }

            if(!wildpaths.isEmpty()){
                for (String wildpath: wildpaths) {
                    TrieNode<T> wild = node.getChild(wildpath);
                    if(wild != null){
                        String[] remainingSegments =
                                Arrays.copyOfRange(segments, i + 1, segments.length);
                        //recurse point
                        Set<T> wildSegmentMembersAtLevel = searchTreeForMembers(wild, remainingSegments);
                        if(wildSegmentMembersAtLevel != null && !wildSegmentMembersAtLevel.isEmpty()){
                            if(wildSegmentMembers == null) wildSegmentMembers = new HashSet<>();
                            wildSegmentMembers.addAll(wildSegmentMembersAtLevel);
                        }
                    }
                }
            }
            node = node.getChild(segments[i]);
            if(node == null) break;
        }

        if(node != null){
            fullMembers = node.getMembers();
        }

        //avoiding having to resize
        int size = wildcardMembers != null ? wildcardMembers.size() : 0;
        size += (wildSegmentMembers != null ? wildSegmentMembers.size() : 0);
        size += (fullMembers != null ? fullMembers.size() : 0);
        Set<T> merged = new HashSet<>(size);

        if(wildcardMembers != null &&
                !wildcardMembers.isEmpty()){
            merged.addAll(wildcardMembers);
        }

        if(wildSegmentMembers != null &&
                !wildSegmentMembers.isEmpty()){
            merged.addAll(wildSegmentMembers);
        }

        if(fullMembers != null &&
                !fullMembers.isEmpty()){
            merged.addAll(fullMembers);
        }

        return merged;
    }

    public String toTree(String lineSep){
        StringBuilder sb = new StringBuilder();
        visitChildren(root, n -> {
            if(!n.hasChildren()){
                if(sb.length() > 0){
                    sb.append(lineSep);
                }
                sb.append(n.toPath(true));
                sb.append(" (");
                sb.append(n.getMembers().size());
                sb.append(")");
            }
        });
        return sb.toString();
    }

    /**
     * Return a set of paths which are either entirely distinct or have been made distinct with memberships.
     * @param considerMembership - true if you wish to include sub paths with memberships as distinct from
     *                           the child paths thereafter.
     *                           For example, '/this/path/here' and '/this/path' with membership included at '/this/path' would be included once without
     *                           considering membership just returning '/this/path/here' or 2 dintinct entries if memberships were considered
     * @return the distinct paths
     */
    public Set<String> getDistinctPaths(boolean considerMembership){
        return getDistinctPathsFromNode(root, considerMembership);
    }

    public int countDistinctPaths(boolean considerMembership){
        return countDistinctPathsFromNode(root, considerMembership);
    }

    public int getBranchCount(){
        return root.getChildPaths().size();
    }

    protected Set<String> getDistinctPathsFromNode(TrieNode node, boolean considerMembership){
        Set<String> paths = new HashSet<>();
        visitChildren(node, n -> {
            //-- either its a leaf node or a node with children but also members
            if(n.isLeaf() || (considerMembership && n.hasMembers())){
                paths.add(n.toPath(true));
            }
        });
        return paths;
    }

    protected int countDistinctPathsFromNode(TrieNode node, boolean considerMembership){
        final AtomicInteger i = new AtomicInteger();
        visitChildren(node, n -> {
            if(n.isLeaf() || (considerMembership && n.hasMembers())){
                i.incrementAndGet();
            }
        });
        return i.get();
    }

    protected String[] split(final String path){
        return MqttTreeUtils.splitPathRetainingSplitChar(path, split);
    }

    public class TrieNode<T> implements MqttTreeNode{
        private volatile Map<String, TrieNode<T>> children;
        private volatile Set<T> members;
        private String pathSegment;
        private TrieNode parent;
        private final boolean isRoot;
        private final Object memberMutex = new Object();
        private final Object childrenMutex = new Object();
        private AtomicInteger memberCount = new AtomicInteger(0);

        protected TrieNode(final TrieNode parent, final String pathSegment){
            this.parent = parent;
            this.pathSegment = pathSegment;
            this.isRoot = parent == null;
            if(!isRoot){
                if(pathSegment == null)
                    throw new IllegalArgumentException("unable to add null element child to " + parent.pathSegment);
            }
        }

        public TrieNode addChild(final String pathSegment, final T... membersIn) throws MqttTreeLimitExceededException {
            if(pathSegment == null) throw new IllegalArgumentException("unable to mount <null> leaf to tree");
            TrieNode child;
            if(children == null) {
                synchronized (childrenMutex) {
                    if (children == null) {
                        children = new HashMap(4);
                    }
                }
            }
            if(!children.containsKey(pathSegment)){
                synchronized (childrenMutex){
                    if(!children.containsKey(pathSegment)){
                        child = new TrieNode<>(this, pathSegment);
                        children.put(pathSegment, child);
                    } else {
                        child = getChild(pathSegment);
                    }
                }
            } else {
                child = getChild(pathSegment);
            }
            if(membersIn.length > 0) child.addMembers(membersIn);
            return child;
        }

        public TrieNode getChild(final String path){
            return children == null ? null : children.get(path);
        }

        public boolean hasChild(String path){
            return children != null && children.containsKey(path);
        }

        public boolean hasChildren(){
            return children != null && !children.isEmpty();
        }

        public boolean hasMembers(){
            return members != null && !members.isEmpty();
        }

        public TrieNode getParent(){
            return parent;
        }

        public boolean isRoot(){
            return isRoot;
        }

        public String getPathSegment(){
            return pathSegment;
        }

        public int getMemberCount(){
            return memberCount.get();
        }

        public void removeChild(TrieNode node){
            if(children != null &&
                    children.containsKey(node.pathSegment)){
                synchronized (childrenMutex){
                    TrieNode removed = children.remove(node.pathSegment);
                    if(removed == node){
                        removed.clear();
                    } else {
                        //only add clear the node if the removed node matched (it may have been recreated
                        //under high load)
                    }
                }
                node.parent = null;
            }
        }

        public  void  addMembers(T... membersIn) throws MqttTreeLimitExceededException {

            if(members == null && membersIn != null && membersIn.length > 0) {
                synchronized (memberMutex) {
                    if(members == null){
                        members = ConcurrentHashMap.newKeySet();
                    }
                }
            }

            if(membersIn != null && membersIn.length > 0){
                if(members.size() + membersIn.length > MqttTree.this.getMaxMembersAtLevel()){
                    throw new MqttTreeLimitExceededException("member limit exceeded at level");
                }
                for(T m : membersIn){
                    if(members.add(m)){
                        memberCount.incrementAndGet();
                    }
                }
            }
        }

        public void clear(){
            if(members != null){
                members.clear();
                members = null;
                memberCount.set(0);
            }
        }
        public boolean removeMember(T member){
            if(members != null){
                boolean removed = members.remove(member);
                if(removed) memberCount.decrementAndGet();
                return removed;
            }
            return false;
        }

        public Set<T> getMembers(){
            if(members == null || parent == null){
                return Collections.emptySet();
            } else {
                long start = System.currentTimeMillis();
                Set<T> t = Collections.unmodifiableSet(members);
                if(System.currentTimeMillis() - start > 50){
                    logger.warn("member copy operation took {} for {} members",
                            System.currentTimeMillis() - start, t.size());
                }
                return t;
            }
        }

        public Set<String> getChildPaths(){
            if(children == null){
                return Collections.emptySet();
            } else {
                synchronized (children){
                    return Collections.unmodifiableSet(children.keySet());
                }
            }
        }

        public boolean isLeaf(){
            return !hasChildren();
        }

        @Override
        public String toString() {
            return "TrieNode{" +
                    "pathSegment='" + pathSegment + '\'' +
                    ", parent=" + parent +
                    ", isRoot=" + isRoot +
                    '}';
        }


        public String toPath(boolean climb){
            if(climb){
                List<String> l = new ArrayList<>();
                TrieNode<T> leaf = this;
                while(leaf != null){
                    if(leaf.isRoot()) break;
                    l.add(leaf.pathSegment);
                    leaf = leaf.getParent();
                }
                StringBuilder sb = new StringBuilder();
                for (int i = l.size(); i-- > 0; ) {
                    sb.append(l.get(i));
                }
                return sb.toString();
            } else {
                return pathSegment;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TrieNode<?> trieNode = (TrieNode<?>) o;
            if (!pathSegment.equals(trieNode.pathSegment)) return false;
            return parent != null ? parent.equals(trieNode.parent) : trieNode.parent == null;
        }

        @Override
        public int hashCode() {
            int result = pathSegment.hashCode();
            result = 31 * result + (parent != null ? parent.hashCode() : 0);
            return result;
        }
    }
}

