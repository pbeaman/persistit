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

package com.persistit;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;

import com.persistit.exception.PersistitException;

/**
 * <p>
 * A persistent <code>java.util.SortedMap</code> over a Persistit database. Keys
 * and values inserted into this map are serialized and stored within Persistit
 * using the encoding methods of {@link Key} and {@link Value}.
 * </p>
 * <p>
 * To construct a <code>PersistitMap</code> you supply an {@link Exchange}. The
 * keys inserted into this map are appended to the key within the {@link Tree}
 * originally supplied by the <code>Exchange</code>. For example, the code
 * 
 * <pre>
 * <code>
 * Exchange ex = new Exchange(&quot;demo&quot;, &quot;composers&quot;, true);
 * ex.append(&quot;My Favorite Johns&quot;);
 * Map&lt;String, String&gt; map = new PersistitMap&lt;String, String&gt;(ex);
 * map.put(&quot;Brahms&quot;, &quot;Johannes&quot;);
 * map.put(&quot;Bach&quot;, &quot;Johann&quot;);
 * </code>
 * </pre>
 * 
 * is equivalent to
 * 
 * <pre>
 * <code>
 * Exchange ex = new Exchange(&quot;demo&quot;, &quot;composers&quot;, true);
 * ex.getValue().put(&quot;Johannes&quot;);
 * ex.clear().append(&quot;My Favorite Johns&quot;, &quot;Brahms&quot;).store();
 * ex.getValue().put(&quot;Johann&quot;);
 * ex.clear().append(&quot;My Favorite Johns&quot;, &quot;Bach&quot;).store();
 * </code>
 * </pre>
 * 
 * </p>
 * <p>
 * By default any <code>Iterator</code>s created by <code>PersistitMap</code>'s
 * collection views implement <i>fail-fast</i> behavior, meaning that methods of
 * the the <code>Iterator</code> will throw a
 * <code>ConcurrentModificationException</code> if the backing store has changed
 * after the <code>Iterator</code> was created. However, in a large persistent
 * database designed for concurrent access, it may be preferable to allow the
 * <code>Iterator</code> to function continuously even when the underlying
 * database is changing. The {@link #setAllowConcurrentModification} method
 * provides control over this behavior.
 * </p>
 * <p>
 * PersistitMap departs from the general contract for <code>SortedMap</code>
 * with respect to the {@link Comparable} interface. The ordering of items
 * within the map is determined by the encoding of key values into their
 * underlying serialized byte array representation (see {@link Key Key Ordering}
 * ). Generally this ordering corresponds to the default <code>Comparable</code>
 * implementation for each supported type. However, <code>PersistitMap</code>
 * permits storage of key values that may not permit comparison under their
 * <code>compareTo</code> implementations, does not require key values to
 * implement <code>Comparable</code>, and ignores the ordering implemented by
 * <code>Comparable</code>. Similarly, <code>PersistitMap</code> does not allow
 * installation of a custom {@link java.util.Comparator Comparator}. To
 * customize the ordering of key values in a <code>PersistitMap</code>,
 * implement and register a custom {@link com.persistit.encoding.KeyCoder
 * KeyCoder}.
 * </p>
 * <p>
 * Unlike other <code>Map</code> implementations, the methods of this class
 * <i>are</i> thread-safe. Each of the <code>Map</code> methods is synchronized
 * on this <code>PersistitMap</code> instance, and each of the methods of any
 * collection view <code>Iterator</code> created by this Map is synchronized on
 * that iterator. Thus this class enforces serialized access to its internal
 * memory structures. However, for maximum concurrency an application that
 * shares data among multiple threads should create a separate instance of
 * PersistitMap for each thread backed by the same tree rather than sharing a
 * single PersistitMap object. These <code>PersistitMap</code> instances can
 * concurrently modify and query the same collection of data.
 * </p>
 * <p>
 * Unlike an in-memory implementation of <code>SortedMap</code>, this
 * implementation serializes and deserializes object values into and from byte
 * arrays stored as Persistit key/value pairs. Thus the {@link #put} and
 * {@link #remove} methods not only read the former value from the database, but
 * also incur the overhead of deserializing it. In many cases applications do
 * not use the former value, and therefore this computation is often
 * unnecessary. For maximum performance an application can use the
 * {@link #putFast} and {@link #removeFast} methods. These methods are analogous
 * to {@link #put} and {@link #remove} but do not deserialize or return the
 * former object value.
 * </p>
 * <p>
 * The <code>Iterator</code> implementations returned by the {@link #keySet},
 * {@link #values()} and {@link #entrySet()} provide methods to set and access a
 * {@link KeyFilter} for the iterator's traversal of keys in the map. With a
 * filter in place, the iterator only returns keys, values or entries for
 * records whose keys are selected by the filter.
 * </p>
 * <p>
 * Note on generic types: compile-time type verification cannot check the
 * contents of the data stored in the backing B-Tree. Therefore the types of Key
 * and Value objects stored in the B-Tree are not guaranteed to match the
 * expected types. For example, the following will compile correctly but throw a
 * ClassCastException at runtime:
 * 
 * <pre>
 * <code>
 * Exchange ex = new Exchange(&quot;demo&quot;, &quot;composers&quot;, true);
 * PersistitMap&lt;String, Integer&gt; map1 = new PersistitMap&lt;String, Integer&gt;();
 * map1.put(&quot;Adams&quot;, Integer.valueOf(1));
 * PersistitMap&lt;String, String&gt; map2 = new PersistitMap&lt;String, String&gt;();
 * String lastName = map2.get(&quot;Adams&quot;);
 * </code>
 * </pre>
 * 
 * <p>
 * Note that any {@link PersistitException} that may occur during execution of
 * one of the <code>SortedMap</code> methods is thrown within the unchecked
 * wrapper class {@link PersistitMapException}. Applications using
 * <code>PersistitMap</code> should catch and handled
 * <code>PersistitMapException</code>.
 * </p>
 * 
 * @version 1.0
 */
public class PersistitMap<K, V> extends AbstractMap<K, V> implements SortedMap<K, V> {

    private final Exchange _ex;
    private long _sizeGeneration;
    private int _size;
    private boolean _allowConcurrentModification = false;
    //
    // These fields are used for subMaps.
    //
    private Key _fromKey;
    private Key _toKey;
    /**
     * Each of these fields are initialized to contain an instance of the
     * appropriate view the first time this view is requested. The views are
     * stateless, so there's no reason to create more than one of each.
     */
    private transient volatile Set<K> _keySet = null;
    private transient volatile Collection<V> _values = null;
    private transient volatile Set<Map.Entry<K, V>> _entrySet;

    /**
     * Construct a PersistitMap over a particular Exchange.
     * 
     * @param ex
     *            A <code>Exchange</code> that serves as the parent of the Map's
     *            keys. This constructor makes a copy of the
     *            <code>Exchange</code>. The original Exchange is unchanged, and
     *            may be reused by the caller.
     */
    public PersistitMap(final Exchange ex) {
        _ex = new Exchange(ex);

        _ex.append(Key.BEFORE);
        _sizeGeneration = -1; // Unknown
    }

    /**
     * Construct a PersistitMap over the range of keys from fromKey (inclusive)
     * to toKey (exclusive)
     * 
     * @param pm
     * @param useFrom
     * @param fromKey
     * @param useTo
     * @param toKey
     * 
     * @throws IllegalArgumentException
     *             if fromKey is after toKey or if either fromKey or toKey is
     *             outside the range of the supplied base PersistitMap
     */
    private PersistitMap(final PersistitMap<K, V> pm, final boolean useFrom, final Object fromKey, final boolean useTo,
            final Object toKey) {
        _ex = new Exchange(pm._ex);

        _sizeGeneration = -1; // Unknown
        if (useFrom) {
            final Key key = new Key(_ex.getKey());
            try {
                key.to(fromKey);
            } catch (final UnsupportedOperationException use) {
                throw new ClassCastException(fromKey != null ? fromKey.getClass().getName() : null);
            }
            if (pm._fromKey != null && pm._fromKey.compareTo(key) > 0 || pm._toKey != null
                    && pm._toKey.compareTo(key) < 0) {
                throw new IllegalArgumentException("Key " + fromKey + " is outside submap range");
            }
            _fromKey = key;
        } else {
            _fromKey = pm._fromKey;
        }
        if (useTo) {
            final Key key = new Key(_ex.getKey());
            try {
                key.to(toKey);
            } catch (final UnsupportedOperationException use) {
                throw new ClassCastException(toKey != null ? toKey.getClass().getName() : null);
            }
            if (pm._fromKey != null && pm._fromKey.compareTo(key) > 0 || pm._toKey != null
                    && pm._toKey.compareTo(key) < 0) {
                throw new IllegalArgumentException("Key " + toKey + " is outside submap range");
            }
            _toKey = key;
        } else {
            _toKey = pm._toKey;
        }
    }

    /**
     * Indicate whether a new <code>Iterator</code>s should throw a
     * <code>ConcurrentModificationException</code> in the event the Persistit
     * Tree backing this Map changes.
     * 
     * @return <code>true</code> if concurrent modifications are currently
     *         allowed, otherwise <code>false</code>.
     */
    public boolean isAllowConcurrentModification() {
        return _allowConcurrentModification;
    }

    /**
     * Control whether iterators of the collection views of this Map will throw
     * <code>ConcurrentModificationException</code> in the event the underlying
     * physical database changes. This property is used in constructing an new
     * <code>Iterator</code> on a collection view. Changing this property after
     * an <code>Iterator</code> has been constructed does not change that
     * <code>Iterator</code>'s behavior.
     * 
     * @param allow
     *            Specify <i>true</i> to allow changes in the backing Tree.
     *            Specify <i>false</i> to cause newly constructed
     *            <code>Iterator</code>s to throw a
     *            <code>ConcurrentModificationException</code> if the Persistit
     *            Tree backing this Map changes.
     */
    public void setAllowConcurrentModification(final boolean allow) {
        _allowConcurrentModification = allow;
    }

    private void toLeftEdge() {
        if (_fromKey == null) {
            _ex.to(Key.BEFORE);
        } else {
            _fromKey.copyTo(_ex.getKey());
        }
    }

    private void toRightEdge() {
        if (_toKey == null) {
            _ex.to(Key.AFTER);
        } else {
            _toKey.copyTo(_ex.getKey());
        }
    }

    private boolean toKey(final Object key) {
        try {
            _ex.to(key);
            if (_fromKey != null && _fromKey.compareTo(_ex.getKey()) > 0)
                return false;
            if (_toKey != null && _toKey.compareTo(_ex.getKey()) < 0)
                return false;
            return true;
        } catch (final UnsupportedOperationException uoe) {
            throw new ClassCastException(key != null ? key.getClass().getName() : null);
        }
    }

    // Query Operations

    /**
     * Return the number of key-value mappings in this map. In the unlikely
     * event the map contains more than <code>Integer.MAX_VALUE</code> elements,
     * the value returned is <code>Integer.MAX_VALUE</code>.
     * <p>
     * This implementation enumerates all the members of the Map, which for a
     * large database could be time-consuming.
     * 
     * @return the number of key-value mappings in this map.
     */
    @Override
    public synchronized int size() {
        if (_ex.getChangeCount() == _sizeGeneration) {
            return _size;
        }
        int size = 0;
        try {
            toLeftEdge();
            while (_ex.traverse(size == 0 ? Key.GTEQ : Key.GT, false, 0)) {
                if (_toKey != null && _ex.getKey().compareTo(_toKey) >= 0) {
                    break;
                }

                //
                // The following code fails (!) in HotSpot if you remove the -1.
                // If you remove the -1, then the value of size does not get
                // incremented, and the iteration through traverse above
                // uses GTEQ rather than GT in an infinite loop.
                //
                if (size < Integer.MAX_VALUE - 1) {
                    size++;
                }
            }
            _size = size;
            _sizeGeneration = _ex.getChangeCount();
            return _size;
        } catch (final PersistitException de) {
            throw new PersistitMapException(de);
        }
    }

    /**
     * Return <code>true</code> if this map contains no key-value mappings.
     * <p>
     * This implementation is relatively efficient because it enumerates at most
     * one child node of the Exchange to determine whether there are any
     * children.
     * 
     * @return <code>true</code> if this map contains no key-value mappings.
     */
    @Override
    public synchronized boolean isEmpty() {
        if (_ex.getChangeCount() == _sizeGeneration) {
            return _size == 0;
        }
        try {
            toLeftEdge();
            if (_ex.traverse(Key.GTEQ, false)) {
                if (_toKey != null && _ex.getKey().compareTo(_toKey) >= 0) {
                    _size = 0;
                    _sizeGeneration = _ex.getChangeCount();
                    return true;
                } else {
                    return false;
                }
            } else {
                return true;
            }
        } catch (final PersistitException de) {
            throw new PersistitMapException(de);
        }
    }

    /**
     * Determine whether this map maps one or more keys to this value. More
     * formally, returns <code>true</code> if and only if this map contains at
     * least one mapping to a value <code>v</code> such that
     * 
     * <pre>
     * <code>(value==null ? v==null : value.equals(v))</code>
     * </pre>
     * 
     * This implementation implements a linear search across the child nodes of
     * the Exchange, and can therefore be extremely time-consuming and
     * resource-intensive for large databases.
     * 
     * @param value
     *            value whose presence in this map is to be tested.
     * 
     * @return <code>true</code> if this map maps one or more keys to this
     *         value.
     */
    @Override
    public synchronized boolean containsValue(final Object value) {
        final Value lookupValue = new Value(_ex.getPersistitInstance());
        try {
            lookupValue.put(value);
            toLeftEdge();
            while (_ex.next()) {
                if (_toKey != null && _ex.getKey().compareTo(_toKey) >= 0) {
                    return false;
                } else if (lookupValue.equals(_ex.getValue())) {
                    return true;
                }
            }
            return false;
        } catch (final Exception exception) {
            throw new PersistitMapException(exception);
        }
    }

    /**
     * Determine whether this map contains a mapping for the specified key. This
     * implementation performs a direct B-Tree lookup and is designed to be
     * fast.
     * 
     * @param key
     *            key whose presence in this map is to be tested.
     * @return <code>true</code> if this map contains a mapping for the
     *         specified key.
     * 
     * @throws NullPointerException
     *             key is <code>null</code> and this map does not not permit
     *             <code>null</code> keys.
     */
    @Override
    public synchronized boolean containsKey(final Object key) {
        try {
            if (!toKey(key)) {
                return false;
            }
            return _ex.isValueDefined();
        } catch (final PersistitException de) {
            throw new PersistitMapException(de);
        }
    }

    /**
     * <p>
     * Return the value to which this map maps the specified key. Returns
     * <code>null</code> if the map contains no mapping for this key. A return
     * value of <code>null</code> does not <i>necessarily</i> indicate that the
     * map contains no mapping for the key; it's also possible that the map
     * explicitly maps the key to <code>null</code>. The containsKey operation
     * may be used to distinguish these two cases.
     * </p>
     * <p>
     * This implementation iterates over <code>entrySet()</code> searching for
     * an entry with the specified key. If such an entry is found, the entry's
     * value is returned. If the iteration terminates without finding such an
     * entry, <code>null</code> is returned. Note that this implementation
     * requires linear time in the size of the map; many implementations will
     * override this method.
     * </p>
     * 
     * @param key
     *            key whose associated value is to be returned.
     * @return the value to which this map maps the specified key.
     * 
     * @throws NullPointerException
     *             if the key is <code>null</code> and this map does not not
     *             permit <code>null</code> keys.
     * 
     * @see #containsKey(Object)
     */
    @Override
    public synchronized V get(final Object key) {
        try {
            if (!toKey(key))
                return null;
            _ex.fetch();
            if (!_ex.getValue().isDefined()) {
                return null;
            }
            @SuppressWarnings("unchecked")
            final V value = (V) _ex.getValue().get();
            return value;
        } catch (final PersistitException de) {
            throw new PersistitMapException(de);
        }
    }

    // Modification Operations

    /**
     * <p>
     * Stores the specified value with the specified key in this map. If the map
     * previously contained a mapping for this key, the old value is replaced.
     * This method returns the old value, if present, or <code>null</code> if
     * there was no former value. An application that does not require the
     * former value can use {@link #putFast} to avoid incurring the cost of
     * deserializing that value.
     * </p>
     * 
     * @param key
     *            key with which the specified value is to be associated.
     * @param value
     *            value to be associated with the specified key.
     * 
     * @return previous value associated with specified key, or
     *         <code>null</code> if there was no mapping for key. (A
     *         <code>null</code> return can also indicate that the map
     *         previously associated <code>null</code> with the specified key,
     *         if the implementation supports <code>null</code> values.)
     * 
     * @throws ClassCastException
     *             if the class of the specified key or value prevents it from
     *             being stored in this map.
     * 
     * @throws IllegalArgumentException
     *             if some aspect of this key or value * prevents it from being
     *             stored in this map.
     * 
     */
    @Override
    @SuppressWarnings("unchecked")
    public synchronized V put(final K key, final V value) {
        try {
            if (!toKey(key)) {
                throw new IllegalArgumentException("Key " + key + " is out of submap range");
            }

            final long changeCount = _sizeGeneration;
            Object result = null;
            _ex.getValue().put(value);
            _ex.fetchAndStore();
            if (_ex.getValue().isDefined()) {
                result = _ex.getValue().get();
            } else {
                adjustSize(changeCount, 1);
            }

            return (V) result;
        } catch (final PersistitException de) {
            throw new PersistitMapException(de);
        }
    }

    /**
     * <p>
     * Associates the specified value with the specified key in this map. If the
     * map previously contained a mapping for this key, the old value is
     * replaced.
     * </p>
     * <p>
     * This method differs from {@link #put} by not returning the previous
     * value. In order to return the previous value, <code>put</code> must
     * deserialize it, which can be costly. Applications that don't need that
     * value can benefit from calling <code>putFast</code> instead of
     * <code>put</code>. </pp>
     * 
     * @param key
     *            key with which the specified value is to be associated.
     * 
     * @param value
     *            value to be associated with the specified key.
     * 
     * @throws ClassCastException
     *             if the class of the specified key or value prevents it from
     *             being stored in this map.
     * 
     * @throws IllegalArgumentException
     *             if some aspect of this key or value * prevents it from being
     *             stored in this map.
     * 
     */
    public synchronized void putFast(final Object key, final Object value) {
        try {
            if (!toKey(key)) {
                throw new IllegalArgumentException("Key " + key + " is out of submap range");
            }

            _ex.getValue().put(value);
            _ex.store();
            _sizeGeneration = -1;
        } catch (final PersistitException de) {
            throw new PersistitMapException(de);
        }
    }

    /**
     * <p>
     * Removes the mapping for this key from this map if present, and returns
     * the former value. Applications that do not require the former value can
     * use {@link #removeFast} to improve performance.
     * </p>
     * 
     * @param key
     *            key whose mapping is to be removed from the map.
     * 
     * @return previous value associated with specified key, or
     *         <code>null</code> if there was no entry for key. (A
     *         <code>null</code> return can also indicate that the map
     *         previously associated <code>null</code> with the specified key.)
     * 
     */
    @Override
    @SuppressWarnings("unchecked")
    public synchronized V remove(final Object key) {
        try {
            if (!toKey(key)) {
                throw new IllegalArgumentException("Key " + key + " is out of submap range");
            }
            final long changeCount = _sizeGeneration;
            final boolean removed = _ex.fetchAndRemove();
            Object result = null;
            if (removed) {
                adjustSize(changeCount, -1);
                if (_ex.getValue().isDefined()) {
                    result = _ex.getValue().get();
                }
            }
            return (V) result;
        } catch (final PersistitException de) {
            throw new PersistitMapException(de);
        }
    }

    /**
     * <p>
     * Removes the mapping for this key from this map if present. Unlike
     * {@link #remove}, this method does not return the former value, and
     * therefore does not incur the cost of deserializing it.
     * </p>
     * 
     * @param key
     *            key whose mapping is to be removed from the map.
     * 
     */
    public synchronized void removeFast(final Object key) {
        try {
            if (!toKey(key)) {
                throw new IllegalArgumentException("Key " + key + " is out of submap range");
            }
            final long changeCount = _sizeGeneration;
            final boolean removed = _ex.remove();
            if (removed) {
                adjustSize(changeCount, -1);
            }
        } catch (final PersistitException de) {
            throw new PersistitMapException(de);
        }
    }

    /**
     * Called by operations that add or remove keys. This method attempts to
     * track and update the current Map size, but if there are untracked
     * modifications, then resets _sizeGeneration to -1, meaning that on the
     * next call to size(), the entire collection will need to be counted.
     * 
     * @param changeCount
     *            The changeCount of the backing tree prior to the operation
     *            that added or removed a key
     * @param delta
     *            1 if a key was added, -1 if removed.
     */
    private void adjustSize(final long changeCount, final int delta) {
        if (_sizeGeneration >= 0) {
            if (_ex.getChangeCount() == changeCount + 1) {
                _size += delta;
                _sizeGeneration = _ex.getChangeCount();
            } else {
                _sizeGeneration = -1;
            }
        }
    }

    // Bulk Operations

    /**
     * Copies all of the mappings from the specified map to this map. These
     * mappings will replace any mappings that this map had for any of the keys
     * currently in the specified map.
     * <p>
     * 
     * This implementation iterates over the specified map's
     * <code>entrySet()</code> collection, and calls this map's <code>put</code>
     * operation once for each entry returned by the iteration.
     * <p>
     * 
     * @param t
     *            mappings to be stored in this map.
     * 
     * @throws ClassCastException
     *             if the class of a key or value in the specified map prevents
     *             it from being stored in this map.
     * 
     * @throws IllegalArgumentException
     *             if some aspect of a key or value in the specified map
     *             prevents it from being stored in this map.
     * 
     * @throws NullPointerException
     *             the specified map is <code>null</code>.
     */
    @Override
    public synchronized void putAll(final Map t) {
        for (final Iterator iterator = t.entrySet().iterator(); iterator.hasNext();) {
            final Map.Entry entry = (Map.Entry) iterator.next();
            putFast(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Removes all mappings from this map.
     * <p>
     * 
     * This implementation calls <code>entrySet().clear()</code>.
     */
    @Override
    public synchronized void clear() {
        try {
            toLeftEdge();
            final Key key = new Key(_ex.getKey());
            if (_toKey == null) {
                key.to(Key.AFTER);
            } else {
                _toKey.copyTo(key);
            }
            _ex.removeKeyRange(_ex.getKey(), key);
            _size = 0;
            _sizeGeneration = _ex.getChangeCount();
        } catch (final PersistitException de) {
            throw new PersistitMapException(de);
        }
    }

    /**
     * Unchecked wrapper for {@link PersistitException}s that may be thrown by
     * methods of {@link SortedMap}.
     */
    public static class PersistitMapException extends RuntimeException {
        private static final long serialVersionUID = 7257161738800744724L;

        Exception _exception;

        PersistitMapException(final Exception ee) {
            _exception = ee;
        }

        @Override
        public Throwable getCause() {
            return _exception;
        }
    }

    /**
     * Implement <code>java.util.Map.Entry</code> using a backing Persistit
     * tree.
     */
    public class ExchangeEntry<K, V> implements Map.Entry<K, V> {
        private final K _key;
        private final V _value;
        private final ExchangeIterator<Map.Entry<K, V>> _iterator;

        private ExchangeEntry(final K key, final V value, final ExchangeIterator<Map.Entry<K, V>> iterator) {
            _key = key;
            _value = value;
            _iterator = iterator;
        }

        /**
         * Returns the key field of this <code>ExchangeEntry</code>.
         * 
         * @return The key
         */
        @Override
        public K getKey() {
            return _key;
        }

        /**
         * Returns the value field of this <code>ExchangeEntry</code>. This is
         * the value that was present in the database at the time an
         * <code>ExchangeIterator</code> created this entry; because other
         * threads may concurrently modify the database, the stored value may
         * have changed.
         * 
         * @return The value associated with this <code>ExchangeEntry</code>
         */
        @Override
        public V getValue() {
            return _value;
        }

        /**
         * Modifies the value field of this Entry and of the underlying
         * Persistit database record associated with the key for this Entry.
         * Returns the former value associated with this entry. This is the
         * value that was present in the database at the time an
         * <code>ExchangeIterator</code> created this entry; because other
         * threads may concurrently modify the database, the stored value may
         * have changed.
         * 
         * @param value
         *            The new value.
         * 
         * @return The value formerly associated with this
         *         <code>ExchangeEntry</code>.
         */
        @Override
        @SuppressWarnings("unchecked")
        public synchronized V setValue(final V value) {
            Object result = null;
            try {
                _ex.to(_key);
                _ex.getValue().put(value);
                long changeCount = _ex.getChangeCount();
                _ex.fetchAndStore();
                if (!_ex.getValue().isDefined())
                    changeCount++;
                _iterator._changeCount = changeCount;
                result = _value;
            } catch (final PersistitException pe) {
                throw new PersistitMapException(pe);
            }
            return (V) result;
        }

        /**
         * Implements the contract for <code>equals</code>. Two
         * <code>ExchangeEntr</code> instances are equal if their key and value
         * fields are equal.
         */
        @Override
        public boolean equals(final Object o) {
            if (o instanceof Map.Entry) {
                final Map.Entry entry = (Map.Entry) o;
                return (entry.getKey() == null ? _key == null : entry.getKey().equals(_key))
                        && (entry.getValue() == null ? _value == null : entry.getValue().equals(_value));
            } else
                return false;
        }

        /**
         * Implements the contract for <code>hashCode</code>. The hash code is
         * the XOR of the hashCodes of the key and value fields.
         */
        @Override
        public int hashCode() {
            return (_key == null ? 0 : _key.hashCode()) ^ (_value == null ? 0 : _value.hashCode());
        }
    }

    private final class ExchangeValueIterator extends ExchangeIterator<V> {

        protected ExchangeValueIterator(final PersistitMap<K, V> pm, final boolean allowConcurrentModification) {
            super(pm, allowConcurrentModification);

        }

        @Override
        public V next() {
            nextEntry();
            return (V) _iteratorExchange.getValue().get();
        }
    }

    private final class ExchangeKeyIterator extends ExchangeIterator<K> {

        protected ExchangeKeyIterator(final PersistitMap<K, V> pm, final boolean allowConcurrentModification) {
            super(pm, allowConcurrentModification);

        }

        @Override
        public K next() {
            nextEntry();
            _iteratorExchange.getKey().indexTo(-1);
            return (K) _iteratorExchange.getKey().decode();
        }
    }

    private final class ExchangeEntryIterator extends ExchangeIterator<Map.Entry<K, V>> {

        protected ExchangeEntryIterator(final PersistitMap<K, V> pm, final boolean allowConcurrentModification) {
            super(pm, allowConcurrentModification);

        }

        @Override
        public Map.Entry<K, V> next() {
            nextEntry();
            _iteratorExchange.getKey().indexTo(-1);
            return (new ExchangeEntry(_iteratorExchange.getKey().decode(), _iteratorExchange.getValue().get(), this));
        }
    }

    /**
     * Implements <code>java.util.Iterator</code> using an underlying Persistit
     * tree as the source of keys and values to be traversed. In addition to the
     * inherited methods of <code>java.util.Iterator</code>, this class also
     * implements a facility to apply a {@link KeyFilter} to restrict the set of
     * keys over it will iterate. See
     * {@link PersistitMap.ExchangeIterator#setFilterTerm(KeyFilter.Term)} and
     * {@link PersistitMap.ExchangeIterator#getKeyFilter()} for details.
     */
    public abstract class ExchangeIterator<E> implements Iterator<E> {
        PersistitMap<K, V> _pm;
        Exchange _iteratorExchange;
        boolean _allowCM;
        Key.Direction _direction = Key.GT;
        long _changeCount;
        boolean _okToRemove;
        KeyFilter _keyFilter;
        boolean _traversed;
        boolean _hasNext;
        Key _trailingKey;

        ExchangeIterator(final PersistitMap<K, V> pm, final boolean allowConcurrentModification) {

            _pm = pm;
            _iteratorExchange = new Exchange(_ex);
            _iteratorExchange.to(Key.BEFORE);
            _trailingKey = new Key(_iteratorExchange.getKey());
            _traversed = false;
            if (_fromKey != null) {
                _fromKey.copyTo(_iteratorExchange.getKey());
            }
            _changeCount = _iteratorExchange.getChangeCount();
            _allowCM = allowConcurrentModification;
        }

        /**
         * Set the desired traversal direction of this Iterator. Values
         * {@link com.persistit.Key#GT} (ascending) and
         * {@link com.persistit.Key#LT} (descending) are allowed.
         * 
         * @param direction
         *            The direction
         */
        public void setDirection(final Key.Direction direction) {
            if (direction == Key.GT || direction == Key.LT) {
                _direction = direction;
            } else {
                throw new IllegalArgumentException("Must be GT or LT");
            }
        }

        /**
         * @return The traversal direction (GT for increasing keys, LT for
         *         decreasing keys) of this Iterator.
         */
        public Key.Direction getDirection() {
            return _direction;
        }

        /**
         * Returns the <code>KeyFilter</code> for this iterator, or
         * <code>null</code> if there is none.
         * 
         * @return A copy of the current <code>KeyFilter</code>, or
         *         <code>null</code> if there is none.
         */
        public KeyFilter getKeyFilter() {
            return _keyFilter;
        }

        /**
         * <p>
         * Set up a <code>KeyFilter</code> for this <code>Iterator</code>. When
         * set, the {@link #hasNext} and {@link #next} operations return only
         * those keys, values or entries for records having keys selected by the
         * supplied {@link KeyFilter.Term}. Note that setting the filter affects
         * subsequent <code>hasNext</code> or <code>next</code> operations, but
         * does not modify the current key, nor does it affect the behavior of
         * {@link #remove} or
         * {@link com.persistit.PersistitMap.ExchangeEntry#setValue} for the
         * current entry.
         * </p>
         * 
         * @param term
         *            If <code>null</code>, clears the <code>KeyFilter</code>
         *            for this iterator. Otherwise, sets up a filter based on
         *            the supplied <code>Term</code>.
         */
        public void setFilterTerm(final KeyFilter.Term term) {
            if (term == null) {
                _keyFilter = null;
            } else {
                final Key key = _iteratorExchange.getKey();
                final Key spareKey = _iteratorExchange.getAuxiliaryKey1();
                key.copyTo(spareKey);
                final int depth = spareKey.getDepth();
                spareKey.cut();
                _keyFilter = new KeyFilter(spareKey).append(term).limit(depth, depth);
            }
        }

        @Override
        public synchronized boolean hasNext() {
            if (!_traversed) {
                _hasNext = traverse();
                _traversed = true;
            }
            return _hasNext;
        }

        protected synchronized void nextEntry() {
            if (!_traversed && !traverse() || _traversed && !_hasNext) {
                throw new NoSuchElementException();
            }
            if (!_allowCM && _iteratorExchange.getChangeCount() != _changeCount) {
                _changeCount = _iteratorExchange.getChangeCount();
                throw new ConcurrentModificationException();
            }
            _traversed = false;
            _okToRemove = true;

        }

        @Override
        public synchronized void remove() {
            if (!_okToRemove) {
                throw new IllegalStateException();
            }

            if (!_allowCM && _iteratorExchange.getChangeCount() != _changeCount) {
                _changeCount = _iteratorExchange.getChangeCount();
                throw new ConcurrentModificationException();
            }

            boolean removed = false;
            final long changeCount = _iteratorExchange.getChangeCount();
            if (_traversed) {
                _trailingKey.copyTo(_iteratorExchange.getKey());
                _traversed = false;
            }
            try {
                removed = _iteratorExchange.remove();
            } catch (final PersistitException e) {
                throw new PersistitMapException(e);
            }
            if (!removed) {
                throw new NoSuchElementException();
            } else {
                _pm.adjustSize(changeCount, -1);

                if (!_allowCM && _iteratorExchange.getChangeCount() != changeCount + 1) {
                    _changeCount = _iteratorExchange.getChangeCount();
                    throw new ConcurrentModificationException();
                }
            }
            _changeCount = _iteratorExchange.getChangeCount();
            _okToRemove = false;
        }

        private boolean traverse() {
            try {
                // Save the current Key before traversing so that in case
                // user calls next(), hasNext(), remove() in that sequence,
                // the original key to be removed is still available.
                _iteratorExchange.getKey().copyTo(_trailingKey);
                final boolean result = _iteratorExchange.traverse(_direction, _keyFilter, Integer.MAX_VALUE);
                if (!result || _toKey == null) {
                    return result;
                } else {
                    return _toKey.compareTo(_iteratorExchange.getKey()) <= 0;
                }
            } catch (final PersistitException de) {
                throw new PersistitMapException(de);
            }
        }
    }

    // Views

    /**
     * Returns a Set view of the keys contained in this map. The Set is backed
     * by the map, so changes to the map are reflected in the Set, and
     * vice-versa. (If the map is modified while an iteration over the Set is in
     * progress, the results of the iteration are undefined.) The Set supports
     * element removal, which removes the corresponding entry from the map, via
     * the Iterator.remove, Set.remove, removeAll retainAll, and clear
     * operations. It does not support the add or addAll operations.
     * <p>
     * 
     * This implementation returns a Set that subclasses AbstractSet. The
     * subclass's iterator method returns a "wrapper object" over this map's
     * entrySet() iterator. The size method delegates to this map's size method
     * and the contains method delegates to this map's containsKey method.
     * <p>
     * 
     * The Set is created the first time this method is called, and returned in
     * response to all subsequent calls. No synchronization is performed, so
     * there is a slight chance that multiple calls to this method will not all
     * return the same Set.
     * 
     * @return a Set view of the keys contained in this map.
     */
    @Override
    public Set<K> keySet() {
        if (_keySet == null) {
            _keySet = new AbstractSet<K>() {
                @Override
                public Iterator<K> iterator() {
                    return new ExchangeKeyIterator(PersistitMap.this, _allowConcurrentModification);
                }

                @Override
                public int size() {
                    return PersistitMap.this.size();
                }

                @Override
                public boolean contains(final Object k) {
                    return PersistitMap.this.containsKey(k);
                }
            };
        }
        return _keySet;
    }

    /**
     * Returns a collection view of the values contained in this map. The
     * collection is backed by the map, so changes to the map are reflected in
     * the collection, and vice-versa. (If the map is modified while an
     * iteration over the collection is in progress, the results of the
     * iteration are undefined.) The collection supports element removal, which
     * removes the corresponding entry from the map, via the
     * <code>Iterator.remove</code>, <code>Collection.remove</code>,
     * <code>removeAll</code>, <code>retainAll</code> and <code>clear</code>
     * operations. It does not support the <code>add</code> or
     * <code>addAll</code> operations.
     * <p>
     * 
     * This implementation returns a collection that subclasses abstract
     * collection. The subclass's iterator method returns a "wrapper object"
     * over this map's <code>entrySet()</code> iterator. The size method
     * delegates to this map's size method and the contains method delegates to
     * this map's containsValue method.
     * <p>
     * 
     * The collection is created the first time this method is called, and
     * returned in response to all subsequent calls. No synchronization is
     * performed, so there is a slight chance that multiple calls to this method
     * will not all return the same Collection.
     * 
     * @return a collection view of the values contained in this map.
     */
    @Override
    public Collection<V> values() {
        if (_values == null) {
            _values = new AbstractSet<V>() {
                @Override
                public Iterator<V> iterator() {
                    return new ExchangeValueIterator(PersistitMap.this, _allowConcurrentModification);
                }

                @Override
                public int size() {
                    return PersistitMap.this.size();
                }

                @Override
                public boolean contains(final Object v) {
                    return PersistitMap.this.containsValue(v);
                }
            };
        }
        return _values;
    }

    /**
     * Returns a Set of Map.Entry values corresponding with key-value pairs in
     * the backing B-Tree.
     * 
     * @return a set view of the mappings contained in this map.
     */
    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        if (_entrySet == null) {
            _entrySet = new AbstractSet<Map.Entry<K, V>>() {
                @Override
                public Iterator<Map.Entry<K, V>> iterator() {
                    return new ExchangeEntryIterator(PersistitMap.this, _allowConcurrentModification);
                }

                @Override
                public int size() {
                    return PersistitMap.this.size();
                }

                @Override
                public boolean contains(final Object v) {
                    return PersistitMap.this.containsValue(v);
                }
            };
        }
        return _entrySet;
    }

    /**
     * Returns <i>null</i> because PersistitMap always uses Persistit's natural
     * ordering and no other Comparator is possible.
     * 
     * @return <i>null</i>
     */
    @Override
    public Comparator<K> comparator() {
        return null;
    }

    /**
     * Returns a view of the portion of this <code>PersistitMap</code> whose
     * keys range from <code>fromKey</code>, inclusive, to <code>toKey</code>,
     * exclusive. The view is another <code>PersistitMap</code> instance backed
     * by this one; any change to the collection made by either by either
     * instance is reflected in the other.
     * 
     * @param fromKey
     *            low endpoint (inclusive) of the subMap.
     * 
     * @param toKey
     *            high endpoint (exclusive) of the subMap.
     * 
     * @return a view of the specified range within this sorted map.
     */
    @Override
    public SortedMap<K, V> subMap(final Object fromKey, final Object toKey) {
        return new PersistitMap<K, V>(this, true, fromKey, true, toKey);
    }

    /**
     * Returns a view of the portion of this <code>PersistitMap</code> whose
     * keys are strictly less than <code>toKey</code>. The view is another
     * <code>PersistitMap</code> instance backed by this one; any change to the
     * collection made by either by either instance is reflected in the other.
     * 
     * @param toKey
     *            high endpoint (exclusive) of the subMap.
     * 
     * @return a view of the specified range within this sorted map.
     */
    @Override
    public SortedMap<K, V> headMap(final Object toKey) {
        return new PersistitMap<K, V>(this, false, null, true, toKey);
    }

    /**
     * Returns a view of the portion of this <code>PersistitMap</code> whose
     * keys are greater than or equal to than <code>fromKey</code>. The view is
     * another <code>PersistitMap</code> instance backed by this one; any change
     * to the collection made by either by either instance is reflected in the
     * other.
     * 
     * @param fromKey
     *            low endpoint (inclusive) of the subMap.
     * 
     * @return a view of the specified range within this sorted map.
     */
    @Override
    public SortedMap<K, V> tailMap(final Object fromKey) {
        return new PersistitMap<K, V>(this, true, fromKey, false, null);
    }

    /**
     * Returns the first (lowest) key currently in this
     * <code>PersistitMap</code> determined by the <a
     * href="Key.html#_keyOrdering">key ordering specification</a>.
     * 
     * @return the first (lowest) key currently in the backing store for this
     *         <code>PersistitMap</code>.
     * 
     * @throws NoSuchElementException
     *             if this map is empty.
     */
    @Override
    public K firstKey() {
        toLeftEdge();
        try {
            if (_ex.traverse(_fromKey == null ? Key.GT : Key.GTEQ, false, -1)
                    && (_toKey == null || _toKey.compareTo(_ex.getKey()) > 0)) {
                return (K) _ex.getKey().decode();
            } else
                throw new NoSuchElementException();
        } catch (final PersistitException de) {
            throw new PersistitMapException(de);
        }
    }

    /**
     * Returns the last (highest) key currently in this
     * <code>PersistitMap</code> as determined by the <a
     * href="Key.html#_keyOrdering">key ordering specification</a>.
     * 
     * @return the last (highest) key currently in the backing store for this
     *         <code>PersistitMap</code>.
     * 
     * @throws NoSuchElementException
     *             if this map is empty.
     */
    @Override
    public K lastKey() {
        toRightEdge();
        try {
            if (_ex.traverse(Key.LT, false, -1) && (_fromKey == null || _fromKey.compareTo(_ex.getKey()) <= 0)) {
                return (K) _ex.getKey().decode();
            } else
                throw new NoSuchElementException();
        } catch (final PersistitException de) {
            throw new PersistitMapException(de);
        }
    }

    // Comparison and hashing

    /**
     * If the supplied object is also a Map, then this method determines whether
     * the supplied map and this map contain the same entries. This is
     * implemented by testing set equality between the entrySets of the two
     * maps. Generally this operation is not useful on a backing database that
     * is large and/or changing is discouraged in most cases.
     * 
     * @param o
     *            object to be compared for equality with this map.
     * @return <code>true</code> if the specified object is equal to this map.
     */
    @Override
    public boolean equals(final Object o) {
        return super.equals(o);
    }

    /**
     * Returns the hash code value for this map. The hash code is the sum of the
     * hash codes of each Map.Entry value in the entry set. The hash code value
     * is generally not useful on a backing database that is large and/or
     * changing, and therefore use of this method is discouraged in most cases.
     * 
     * @return the hash code value for this map.
     * @see java.util.Map.Entry#hashCode()
     * @see Object#hashCode()
     * @see Object#equals(Object)
     * @see Set#equals(Object)
     */
    @Override
    public int hashCode() {
        return super.hashCode();
    }

    /**
     * Returns a string representation of this map. The string representation
     * consists of a list of key-value mappings in the order returned by the
     * map's <code>entrySet</code> view's iterator, enclosed in braces (
     * <code>"{}"</code>). Adjacent mappings are separated by the characters
     * <code>", "</code> (comma and space). Each key-value mapping is rendered
     * as the key followed by an equals sign (<code>"="</code>) followed by the
     * associated value. Keys and values are converted to strings as by
     * <code>String.valueOf(Object)</code>.
     * <p>
     * 
     * @return a String representation of this map.
     */
    @Override
    public String toString() {
        return "PersistitMap(" + _ex + ")";
    }

    /**
     * Returns a shallow copy of this <code>AbstractMap</code> instance: the
     * keys and values themselves are not cloned.
     * 
     * @return a shallow copy of this map.
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }

}
