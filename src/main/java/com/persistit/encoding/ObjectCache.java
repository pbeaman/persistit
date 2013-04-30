/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.persistit.encoding;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;

import com.persistit.Key;
import com.persistit.KeyState;

/**
 * <p>
 * A simple cache for deserialized objects. This cache is loosely patterned
 * after WeakHashMap, except that an entry is eligible to be removed when the
 * value associated with a key is softly referenced, not the key.
 * </p>
 * <p>
 * Note: although many of the methods of this class take an argument of type
 * {@link com.persistit.Key}, the map is actually stored using immutable
 * {@link com.persistit.KeyState} objects. Therefore if you modify the state of
 * a <code>Key</code> after using it in a call to {@link #put}, the mapping will
 * remain intact. A new <code>KeyState</code> object is created only when a new
 * member is being added to the cache. Looking up, removing or replacing a value
 * by key does not require construction of a new <code>KeyState</code> because
 * <code>Key</code> and <code>KeyState</code> implement compatible
 * <code>equals</code> and <code>hashCode</code> methods.
 * </p>
 * 
 */
public class ObjectCache {
    /**
     * Constant used to indicate the value associated with a particular
     * <code>Key</code> is known to be <code>null</code> rather than unknown.
     */
    public final static Object NULL = new Object();
    /**
     * Initial size of the backing hash table.
     */
    public final static int DEFAULT_INITIAL_SIZE = 16;

    private Entry[] _entries = new Entry[DEFAULT_INITIAL_SIZE];
    private int _size;
    private int _deadCount;
    private final int _deadCountThreshold = 25;

    private final ReferenceQueue<SoftReference<?>> _queue = new ReferenceQueue<SoftReference<?>>();

    private static class Entry {
        Object _key;
        SoftReference<?> _reference;
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
     * which the referent value of the <code>SoftReference</code> has been
     * removed by the garbage collector.
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
     * supplied {@link com.persistit.Key}. <code>Key</code> and
     * <code>KeyState</code> implement compatible <code>equals</code> and
     * <code>hashCode</code> methods so that they can be used interchangeably to
     * access values in maps.
     * </p>
     * <p>
     * The value is held within a <code>SoftReference</code>. This means that as
     * soon as no other object holds a strong reference to the value, the
     * garbage collector is permitted to remove it and to set the referent of
     * the <code>SoftReference</code> to <code>null</code>. Therefore an
     * application may <code>put</code> a value into this cache and then at some
     * later time receive <code>null</code> from the <code>get</code> method.
     * </p>
     * <p>
     * This method handles <code>null</code> values in a special way. The code
     * <blockquote>
     * 
     * <pre>
     * objectCache.put(key, null)
     * </pre>
     * 
     * </blockquote> stores the constant {@link #NULL} to represent the
     * knowledge that the value associated with key is <code>null</code>, rather
     * than unknown. The {@link #get} method returns <code>null</code>
     * regardless of whether the value in the cache is NULL or is not present.
     * Applications should use the {@link #isCached} method to determine whether
     * the value is affirmatively <code>null</code>. Alternatively, applications
     * may use the {@link #getWithNull} method and then test the result for
     * equality with <code>NULL</code> as a distinct value.
     * </p>
     * 
     * @param key
     *            The <code>Key</code> used to identify the value. When storing
     *            a new key, this method constructs and stores an immutable
     *            <code>KeyState</code> from the <code>Key</code> so that the
     *            mapping remains valid even if the <code>Key</code>
     *            subsequently changes.
     * 
     * @param value
     *            An Object that is to be associated with key. The value may be
     *            any class, and it may be <code>null</code>.
     * 
     * @return The former value.
     */
    public synchronized Object put(final Key key, Object value) {
        if (value == null)
            value = NULL;
        Object previousValue = null;

        boolean found = false;
        final int offset = offset(key, _entries.length);
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
        entry._reference = new SoftReference(value, _queue);
        processQueue(_deadCountThreshold);
        if (_size > _entries.length * 0.75)
            resize();
        return previousValue;
    }

    /**
     * <p>
     * Return the Object value associated with the key if it is present in the
     * cache; otherwise <code>null</code>. This method does not differentiate
     * between a stored <code>null</code> value and a value missing from the
     * cache. Applications should use the {@link #isCached} method or
     * {@link #getWithNull} to determine whether the there is a value associated
     * with the key.
     * </p>
     * 
     * @param key
     *            The key to which the value is associated.
     * 
     * @return The value, or <code>null</code> if there is none.
     */
    public synchronized Object get(final Key key) {
        final int offset = offset(key, _entries.length);
        Entry entry = _entries[offset];
        while (entry != null) {
            if (entry._key.equals(key)) {
                final Object value = entry._reference.get();
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
     * cache; otherwise <code>null</code>. This method differentiates between
     * between a stored <code>null</code> value and a value missing from the
     * cache. In the former case, this method returns {@link #NULL}.
     * Applications can test the result as follows: <blockquote>
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
     *         known to be null, or <code>null</code> if the there is no cached
     *         value for this key.
     */
    public synchronized Object getWithNull(final Key key) {
        final int offset = offset(key, _entries.length);
        Entry entry = _entries[offset];
        while (entry != null) {
            if (entry._key.equals(key)) {
                final Object value = entry._reference.get();
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
     * @return <code>true</code> if the cache contains a representation of the
     *         value associated with the key; otherwise <code>false</code>
     */
    public synchronized boolean isCached(final Key key) {
        final int offset = offset(key, _entries.length);
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
     *            The <code>Key</code>
     * 
     * @return The value formerly associated in the cache with the supplied
     *         <code>key</code>. The behavior is the same as
     *         {@link #getWithNull}; that is, the returned value value is an
     *         object, {@link #NULL} or <code>null</code>.
     */
    public synchronized Object remove(final Key key) {
        Object value = null;
        final int offset = offset(key, _entries.length);
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
        if (_entries.length > newSize && _entries.length < newSize + DEFAULT_INITIAL_SIZE)
            return;

        final Entry[] newEntries = new Entry[newSize];
        for (int offset = 0; offset < _entries.length; offset++) {
            Entry entry = _entries[offset];
            while (entry != null) {
                final int newOffset = offset(entry._key, newSize);
                final Entry next = entry._next;
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
    private void processQueue(final int deadCountThreshold) {
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

    private int offset(final Object key, final int length) {
        return ((key.hashCode()) & 0x7FFFFFFF) % length;
    }

}
