/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Jul 3, 2004
 */
package com.persistit.encoding;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

import com.persistit.Key;
import com.persistit.KeyState;

/**
 * <p>
 * A simple cache for deserialized objects. This cache is loosely patterned
 * after WeakHashMap, except that an entry is eligible to be removed when the
 * value associated with a key is weakly referenced, not the key.
 * </p>
 * <p>
 * Note: although many of the methods of this class take an argument of type
 * {@link com.persistit.Key}, the map is actually stored using immutable
 * {@link com.persistit.KeyState} objects. Therefore if you modify the state of
 * a <tt>Key</tt> after using it in a call to {@link #put}, the mapping will
 * remain intact. A new <tt>KeyState</tt> object is created only when a new
 * member is being added to the cache. Looking up, removing or replacing a value
 * by key does not require construction of a new <tt>KeyState</tt> because
 * <tt>Key</tt> and <tt>KeyState</tt> implement compatible <tt>equals</tt> and
 * <tt>hashCode</tt> methods.
 * </p>
 * 
 */
public class ObjectCache {
    /**
     * Constant used to indicate the value associated with a particular
     * <tt>Key</tt> is known to be <tt>null</tt> rather than unknown.
     */
    public final static Object NULL = new Object();
    /**
     * Initial size of the backing hash table.
     */
    public final static int DEFAULT_INITIAL_SIZE = 16;

    private Entry[] _entries = new Entry[DEFAULT_INITIAL_SIZE];
    private int _size;
    private int _deadCount;
    private int _deadCountThreshold = 25;

    private ReferenceQueue _queue = new ReferenceQueue();

    private static class Entry {
        Object _key;
        WeakReference _reference;
        Entry _next;
    }

    /**
     * Remove all entries from this cache.
     */
    public synchronized void clear() {
        _entries = new Entry[DEFAULT_INITIAL_SIZE];
        _size = 0;
        _deadCount = 0;
    }

    /**
     * Remove all dead entries from this cache. A dead reference is one for
     * which the referent value of the <tt>WeakReference</tt> has been removed
     * by the garbage collector.
     */
    public synchronized void clean() {
        processQueue(0);
        resize();
    }

    /**
     * <p>
     * Inserts a key/value pair in the cache and returns the formerly cached
     * value, if there is one. The object used to identify the value in the
     * cache is a {@link com.persistit.KeyState} constructed from the the
     * supplied {@link com.persistit.Key}. <tt>Key</tt> and <tt>KeyState</tt>
     * implement compatible <tt>equals</tt> and <tt>hashCode</tt> methods so
     * that they can be used interchangeably to access values in maps.
     * </p>
     * <p>
     * The value is held within a <tt>WeakReference</tt>. This means that as
     * soon as no other object holds a strong reference to the value, the
     * garbage collector is permitted to remove it and to set the referent of
     * the <tt>WeakReference</tt> to <tt>null</tt>. Therefore an application may
     * <tt>put</tt> a value into this cache and then at some later time receive
     * <tt>null</tt> from the <tt>get</tt> method.
     * </p>
     * <p>
     * This method handles <tt>null</tt> values in a special way. The code
     * <blockquote>
     * 
     * <pre>
     * objectCache.put(key, null)
     * </pre>
     * 
     * </blockquote> stores the constant {@link #NULL} to represent the
     * knowledge that the value associated with key is <tt>null</tt>, rather
     * than unknown. The {@link #get} method returns <tt>null</tt> regardless of
     * whether the value in the cache is NULL or is not present. Applications
     * should use the {@link #isCached} method to determine whether the value is
     * affirmatively <tt>null</tt>. Alternatively, applications may use the
     * {@link #getWithNull} method and then test the result for equality with
     * <tt>NULL</tt> as a distinct value.
     * </p>
     * 
     * @param key
     *            The <tt>Key</tt> used to identify the value. When storing a
     *            new key, this method constructs and stores an immutable
     *            <tt>KeyState</tt> from the <tt>Key</tt> so that the mapping
     *            remains valid even if the <tt>Key</tt> subsequently changes.
     * 
     * @param value
     *            An Object that is to be associated with key. The value may be
     *            any class, and it may be <tt>null</tt>.
     * 
     * @return The former value.
     */
    public synchronized Object put(Key key, Object value) {
        if (value == null)
            value = NULL;
        Object previousValue = null;

        boolean found = false;
        int offset = offset(key, _entries.length);
        Entry entry = _entries[offset];
        while (entry != null && !found) {
            if (entry._key.equals(key)) {
                found = true;
                previousValue = entry._reference.get();
                if (previousValue == NULL)
                    previousValue = null;
            } else
                entry = entry._next;
        }
        if (!found) {
            entry = new Entry();
            entry._key = new KeyState(key);
            entry._next = _entries[offset];
            _entries[offset] = entry;
            _size++;
        }
        entry._reference = new WeakReference(value, _queue);
        processQueue(_deadCountThreshold);
        if (_size > _entries.length * 0.75)
            resize();
        return previousValue;
    }

    /**
     * <p>
     * Return the Object value associated with the key if it is present in the
     * cache; otherwise <tt>null</tt>. This method does not differentiate
     * between a stored <tt>null</tt> value and a value missing from the cache.
     * Applications should use the {@link #isCached} method or
     * {@link #getWithNull} to determine whether the there is a value associated
     * with the key.
     * </p>
     * 
     * @param key
     *            The key to which the value is associated.
     * 
     * @return The value, or <tt>null</tt> if there is none.
     */
    public synchronized Object get(Key key) {
        int offset = offset(key, _entries.length);
        Entry entry = _entries[offset];
        while (entry != null) {
            if (entry._key.equals(key)) {
                Object value = entry._reference.get();
                if (value == NULL)
                    return null;
                else
                    return value;
            }
            entry = entry._next;
        }
        return null;
    }

    /**
     * <p>
     * Return the Object value associated with the key if it is present in the
     * cache; otherwise <tt>null</tt>. This method differentiates between
     * between a stored <tt>null</tt> value and a value missing from the cache.
     * In the former case, this method returns {@link #NULL}. Applications can
     * test the result as follows: <blockquote>
     * 
     * <pre>
     * Object o = cache.getWithNull(key);
     * if (o == null) ... //not cached
     * else if (o == ObjectCache.NULL) .. // value is known to be null
     * else ... // o represents a cached object
     * </pre>
     * 
     * </blockquote>
     * </p>
     * 
     * @param key
     *            The key to which the value is associated.
     * 
     * @return The value, {@link #NULL} if the value associated with the key is
     *         known to be null, or <tt>null</tt> if the there is no cached
     *         value for this key.
     */
    public synchronized Object getWithNull(Key key) {
        int offset = offset(key, _entries.length);
        Entry entry = _entries[offset];
        while (entry != null) {
            if (entry._key.equals(key)) {
                Object value = entry._reference.get();
                return value;
            }
            entry = entry._next;
        }
        return null;
    }

    /**
     * Indicates whether there is a value associated with the key.
     * 
     * @param key
     * @return <tt>true</tt> if the cache contains a representation of the value
     *         associated with the key; otherwise <tt>false</tt>
     */
    public synchronized boolean isCached(Key key) {
        int offset = offset(key, _entries.length);
        Entry entry = _entries[offset];
        while (entry != null) {
            if (entry._key.equals(key)) {
                return true;
            }
            entry = entry._next;
        }
        return false;
    }

    /**
     * Remove the entry for the specified key, if present, and return its former
     * value.
     * 
     * @param key
     *            The <tt>Key</tt>
     * 
     * @return The value formerly associated in the cache with the supplied
     *         <tt>key</tt>. The behavior is the same as {@link #getWithNull};
     *         that is, the returned value value is an object, {@link #NULL} or
     *         <tt>null</tt>.
     */
    public synchronized Object remove(Key key) {
        Object value = null;
        int offset = offset(key, _entries.length);
        Entry entry = _entries[offset];
        if (entry == null)
            return null;
        if (entry._key.equals(key)) {
            _entries[offset] = entry._next;
            _size--;
            value = entry._reference.get();
        } else {
            Entry next = entry._next;
            while (next != null) {
                if (next._key.equals(key)) {
                    entry._next = next._next;
                    _size--;
                    value = next._reference.get();
                    break;
                }
                entry = next;
                next = entry._next;
            }
        }
        processQueue(_deadCountThreshold);
        if (_size < _entries.length * .25)
            resize();
        return value;
    }

    /**
     * Recomputes the size needed to store the entries in the cache efficienty,
     * and if necessary, replaces the backing array store.
     */
    private void resize() {
        int newSize = _size * 2;
        if (newSize < DEFAULT_INITIAL_SIZE)
            newSize = DEFAULT_INITIAL_SIZE;

        // If the length of the table is already in the ballpark, just
        // leave it.
        if (_entries.length > newSize
                && _entries.length < newSize + DEFAULT_INITIAL_SIZE)
            return;

        Entry[] newEntries = new Entry[newSize];
        for (int offset = 0; offset < _entries.length; offset++) {
            Entry entry = _entries[offset];
            while (entry != null) {
                int newOffset = offset(entry._key, newSize);
                Entry next = entry._next;
                entry._next = newEntries[newOffset];
                newEntries[newOffset] = entry;
                entry = next;
            }
        }
        _entries = newEntries;
    }

    /**
     * <p>
     * Counts the dead WeakReferences. If the total is above the supplied
     * threshold, scans the cache for and removes entries having dead
     * references.
     * </p>
     * <p>
     * Removing entries containing dead entries reduces memory consumption only
     * modestly, so it may be appropriate to raise the default dead count
     * threshold.
     * </p>
     * 
     * @param deadCountThreshold
     */
    private void processQueue(int deadCountThreshold) {
        while (_queue.poll() != null)
            _deadCount++;
        if (_deadCount > deadCountThreshold) {
            for (int index = 0; index < _entries.length; index++) {
                Entry entry = _entries[index];
                while (entry != null && entry._reference.get() == null) {
                    entry = _entries[index] = entry._next;
                    _size--;
                }
                if (entry != null) {
                    Entry next;
                    while ((next = entry._next) != null) {
                        if (next._reference.get() == null) {
                            entry._next = next._next;
                            _size--;
                        } else {
                            entry = entry._next;
                        }
                    }
                }
            }
            _deadCount = 0;
        }
    }

    private int offset(Object key, int length) {
        return ((key.hashCode()) & 0x7FFFFFFF) % length;
    }

}
