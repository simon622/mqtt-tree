package org.slj;

/**
 * @author Simon L Johnson
 */
public interface ITreeProvider<T> {

    int search(String path);

    void add(String path, T member) throws Exception;
}
