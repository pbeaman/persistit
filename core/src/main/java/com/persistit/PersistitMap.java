/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.persistit;

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
 * Implements a persistent <code>java.util.SortedMap</code> over a Persistit
 * database. Keys and values inserted into this map are serialized and stored
 * within Persistit using the encoding methods of {@link Key} and {@link Value}.
 * </p>
 * <p>
 * To construct a <code>PersistitMap</code> you supply a {@link Exchange}. The
 * keys inserted into this map are appended to the key within the {@link Tree}
 * originally supplied by the <code>Exchange</code>. For example, the code
 * <blockquote>
 * 
 * <pre>
 *      Exchange ex = new Exchange("demo", "composers", true);
 *      ex.append("My Favorite Johns");
 *      Map map = new PersistitMap(ex);
 *      map.put("Brahms", "Johannes");
 *      map.put("Bach", "Johann");
 * </blockquote>
 * </pre>
 * 
 * is equivalent to <blockquote>
 * 
 * <pre>
 *      Exchange ex = new Exchange("demo", "composers", true);
 *      ex.getValue().put("Johannes");
 *      ex.clear().append("My Favorite Johns","Brahms").store();
 *      ex.getValue().put("Johann");
 *      ex.clear().append("My Favorite Johns","Bach").store();
 * </blockquote>
 * </pre>
 * 
 * </p>
 * <p>
 * By default any <code>Iterator</code>s created by <code>PersistitMap</code>'s
 * collection views implement <i>fail-fast</i> behavior, meaning that methods of
 * the the <code>Iterator</code> will throw a
 * <code>ConcurrentModificationException</code> if the backing store has changed
 * after the <code>Iterator</code> was created. However, in a large persistent
 * database designed for concurrent access, it is often preferrable to allow the
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
 * KeyCoder} or {@link com.persistit.encoding.KeyStringCoder KeyStringCoder}.
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
 * unnecessary. For maximum performance an application can invoke the
 * {@link #putFast} and {@link #removeFast} methods. These methods are analogous
 * to {@link #put} and {@link #remove} but do not deserialize or return the
 * former object value.
 * </p>
 * <p>
 * The <code>Iterator</code> implementation returned by the {@link #keySet},
 * {@link #values()} and {@link #entrySet()} provides methods to set and access
 * a {@link KeyFilter} for the iterator's traversal of keys in the map. With a
 * filter in place, the iterator only returns keys, values or entries for
 * records whose keys are selected by the filter.
 * </p>
 * 
 * @version 1.0
 */
public class PersistitMap implements SortedMap {
    private final static int KEY = 1;
    private final static int VALUE = 2;
    private final static int ENTRY = 3;

    private Exchange _ex;
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
    private transient volatile Set _keySet = null;
    private transient volatile Collection _values = null;
    private transient volatile Set _entrySet;

    /**
     * Constructs a PersistitMap over a particular Exchange.
     * 
     * @param ex
     *            A <code>Exchange</code> that serves as the parent of the Map's
     *            keys. This constructor makes a copy of the
     *            <code>Exchange</code>. The original Exchange is unchanged, and
     *            may be reused by the caller.
     */
    public PersistitMap(Exchange ex) {
        _ex = new Exchange(ex);

        _ex.append(Key.BEFORE);
        _sizeGeneration = -1; // Unknown
    }

    /**
     * Constructs a PersistitMap over the range of keys from fromKey (inclusive)
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
    private PersistitMap(PersistitMap pm, boolean useFrom, Object fromKey,
            boolean useTo, Object toKey) {
        _ex = new Exchange(pm._ex);

        _sizeGeneration = -1; // Unknown
        if (useFrom) {
            Key key = new Key(_ex.getKey());
            try {
                key.to(fromKey);
            } catch (UnsupportedOperationException use) {
                throw new ClassCastException(fromKey != null ? fromKey
                        .getClass().getName() : null);
            }
            if (pm._fromKey != null && pm._fromKey.compareTo(key) > 0
                    || pm._toKey != null && pm._toKey.compareTo(key) < 0) {
                throw new IllegalArgumentException("Key " + fromKey
                        + " is outside submap range");
            }
            _fromKey = key;
        } else {
            _fromKey = pm._fromKey;
        }
        if (useTo) {
            Key key = new Key(_ex.getKey());
            try {
                key.to(toKey);
            } catch (UnsupportedOperationException use) {
                throw new ClassCastException(toKey != null ? toKey.getClass()
                        .getName() : null);
            }
            if (pm._fromKey != null && pm._fromKey.compareTo(key) > 0
                    || pm._toKey != null && pm._toKey.compareTo(key) < 0) {
                throw new IllegalArgumentException("Key " + toKey
                        + " is outside submap range");
            }
            _toKey = key;
        } else {
            _toKey = pm._toKey;
        }
    }

    /**
     * Indicates whether a new <code>Iterator</code>s should throw a
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
     * Controls whether iterators of the collection views of this Map will throw
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
    public void setAllowConcurrentModification(boolean allow) {
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

    private boolean toKey(Object key) {
        try {
            _ex.to(key);
            if (_fromKey != null && _fromKey.compareTo(_ex.getKey()) > 0)
                return false;
            if (_toKey != null && _toKey.compareTo(_ex.getKey()) < 0)
                return false;
            return true;
        } catch (UnsupportedOperationException uoe) {
            throw new ClassCastException(key != null ? key.getClass().getName()
                    : null);
        }
    }

    // Query Operations

    /**
     * Returns the number of key-value mappings in this map. If the map contains
     * more than <code>Integer.MAX_VALUE</code> elements, returns
     * <code>Integer.MAX_VALUE</code>.
     * <p>
     * This implementation enumerates all the members of the Map, which for a
     * large database could be time-consuming.
     * 
     * @return the number of key-value mappings in this map.
     */
    public synchronized int size() {
        if (_ex.getChangeCount() == _sizeGeneration) {
            return _size;
        }
        int size = 0;
        try {
            toLeftEdge();
            while (_ex.traverse(size == 0 ? Key.GTEQ : Key.GT, false, 0)) {
                if (_toKey != null && _ex.getKey().compareTo(_toKey) >= 0)
                    break;
                size++;
            }
            _size = size;
            _sizeGeneration = _ex.getChangeCount();
            return _size;
        } catch (PersistitException de) {
            throw new PersistitMapException(de);
        }
    }

    /**
     * Returns <code>true</code> if this map contains no key-value mappings.
     * <p>
     * This implementation is relatively efficient because it enumerates at most
     * one child node of the Exchange to determine whether there are any
     * children.
     * 
     * @return <code>true</code> if this map contains no key-value mappings.
     */
    public synchronized boolean isEmpty() {
        if (_ex.getChangeCount() == _sizeGeneration) {
            return _size == 0;
        }
        try {
            toLeftEdge();
            if (_ex.traverse(Key.GTEQ, false)) {
                if (_toKey != null && _ex.getKey().compareTo(_toKey) >= 0) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return true;
            }
        } catch (PersistitException de) {
            throw new PersistitMapException(de);
        }
    }

    /**
     * Returns <code>true</code> if this map maps one or more keys to this
     * value. More formally, returns <code>true</code> if and only if this map
     * contains at least one mapping to a value <code>v</code> such that
     * <code>(value==null ?
     * v==null : value.equals(v))</code>. This operation will probably require
     * time linear in the map size for most implementations of map.
     * <p>
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
    public synchronized boolean containsValue(Object value) {
        Value lookupValue = new Value(_ex.getPersistitInstance());
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
        } catch (Exception exception) {
            throw new PersistitMapException(exception);
        }
    }

    /**
     * Returns <code>true</code> if this map contains a mapping for the
     * specified key.
     * <p>
     * This implementation performs a direct B-Tree lookup and is designed to be
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
    public synchronized boolean containsKey(Object key) {
        try {
            if (!toKey(key))
                return false;
            _ex.fetch();
            return _ex.getValue().isDefined();
        } catch (PersistitException de) {
            throw new PersistitMapException(de);
        }
    }

    /**
     * Returns the value to which this map maps the specified key. Returns
     * <code>null</code> if the map contains no mapping for this key. A return
     * value of <code>null</code> does not <i>necessarily</i> indicate that the
     * map contains no mapping for the key; it's also possible that the map
     * explicitly maps the key to <code>null</code>. The containsKey operation
     * may be used to distinguish these two cases.
     * <p>
     * 
     * This implementation iterates over <code>entrySet()</code> searching for
     * an entry with the specified key. If such an entry is found, the entry's
     * value is returned. If the iteration terminates without finding such an
     * entry, <code>null</code> is returned. Note that this implementation
     * requires linear time in the size of the map; many implementations will
     * override this method.
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
    public synchronized Object get(Object key) {
        try {
            if (!toKey(key))
                return null;
            _ex.fetch();
            if (!_ex.getValue().isDefined())
                return null;
            return _ex.getValue().get();
        } catch (PersistitException de) {
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
    public synchronized Object put(Object key, Object value) {
        try {
            if (!toKey(key)) {
                throw new IllegalArgumentException("Key " + key
                        + " is out of submap range");
            }

            long changeCount = _sizeGeneration;
            Object result = null;
            _ex.getValue().put(value);
            _ex.fetchAndStore();
            if (_ex.getValue().isDefined()) {
                result = _ex.getValue().get();
            } else {
                adjustSize(changeCount, 1);
            }

            return result;
        } catch (PersistitException de) {
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
    public synchronized void putFast(Object key, Object value) {
        try {
            if (!toKey(key)) {
                throw new IllegalArgumentException("Key " + key
                        + " is out of submap range");
            }

            _ex.getValue().put(value);
            _ex.store();
            _sizeGeneration = -1;
        } catch (PersistitException de) {
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
    public synchronized Object remove(Object key) {
        try {
            if (!toKey(key)) {
                throw new IllegalArgumentException("Key " + key
                        + " is out of submap range");
            }
            long changeCount = _sizeGeneration;
            boolean removed = _ex.fetchAndRemove();
            Object result = null;
            if (removed) {
                adjustSize(changeCount, -1);
                if (_ex.getValue().isDefined()) {
                    result = _ex.getValue().get();
                }
            }
            return result;
        } catch (PersistitException de) {
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
    public synchronized void removeFast(Object key) {
        try {
            if (!toKey(key)) {
                throw new IllegalArgumentException("Key " + key
                        + " is out of submap range");
            }
            long changeCount = _sizeGeneration;
            boolean removed = _ex.remove();
            if (removed) {
                adjustSize(changeCount, -1);
            }
        } catch (PersistitException de) {
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
    private void adjustSize(long changeCount, int delta) {
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
    public synchronized void putAll(Map t) {
        for (Iterator iterator = t.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry entry = (Map.Entry) iterator.next();
            putFast(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Removes all mappings from this map.
     * <p>
     * 
     * This implementation calls <code>entrySet().clear()</code>.
     */
    public synchronized void clear() {
        try {
            toLeftEdge();
            Key key = new Key(_ex.getKey());
            if (_toKey == null) {
                key.to(Key.AFTER);
            } else {
                _toKey.copyTo(key);
            }
            _ex.removeKeyRange(_ex.getKey(), key);
            _size = 0;
            _sizeGeneration = _ex.getChangeCount();
        } catch (PersistitException de) {
            throw new PersistitMapException(de);
        }
    }

    public class PersistitMapException extends RuntimeException {
        private static final long serialVersionUID = 7257161738800744724L;

        Exception _exception;

        PersistitMapException(Exception ee) {
            _exception = ee;
        }

        @Override
        public Throwable getCause() {
            return _exception;
        }
    }

    /**
     * Implementats <code>java.util.Map.Entry</code> using a backing Persistit
     * tree.
     */
    public class ExchangeEntry implements Map.Entry {
        private Object _key;
        private Object _value;
        private ExchangeIterator _iterator;

        private ExchangeEntry(Object key, Object value,
                ExchangeIterator iterator) {
            _key = key;
            _value = value;
            _iterator = iterator;
        }

        /**
         * Returns the key field of this <code>ExchangeEntry</code>.
         * 
         * @return The key
         */
        public Object getKey() {
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
        public Object getValue() {
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
        public synchronized Object setValue(Object value) {
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
            } catch (PersistitException pe) {
                throw new PersistitMapException(pe);
            }
            return result;
        }

        /**
         * Implements the contract for <code>equals</code>. Two
         * <code>ExchangeEntr</code> instances are equal if their key and value
         * fields are equal.
         */
        @Override
        public boolean equals(Object o) {
            if (o instanceof Map.Entry) {
                Map.Entry entry = (Map.Entry) o;
                return (entry.getKey() == null ? _key == null : entry.getKey()
                        .equals(_key))
                        && (entry.getValue() == null ? _value == null : entry
                                .getValue().equals(_value));
            } else
                return false;
        }

        /**
         * Implements the contract for <code>hashCode</code>. The hash code is
         * the XOR of the hashCodes of the key and value fields.
         */
        @Override
        public int hashCode() {
            return (_key == null ? 0 : _key.hashCode())
                    ^ (_value == null ? 0 : _value.hashCode());
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
    public class ExchangeIterator implements Iterator {
        PersistitMap _pm;
        Exchange _iteratorExchange;
        boolean _allowCM;
        long _changeCount;
        int _type;
        boolean _okToRemove;
        KeyFilter _keyFilter;

        ExchangeIterator(PersistitMap pm, int type,
                boolean allowConcurrentModification) {
            _type = type;
            _pm = pm;
            _iteratorExchange = new Exchange(_ex);
            _iteratorExchange.to(Key.BEFORE);
            if (_fromKey != null) {
                _fromKey.copyTo(_iteratorExchange.getKey());
            }
            _changeCount = _iteratorExchange.getChangeCount();
            _allowCM = allowConcurrentModification;
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
         * Sets up a <code>KeyFilter</code> for this <code>Iterator</code>. When
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
        public void setFilterTerm(KeyFilter.Term term) {
            if (term == null) {
                _keyFilter = null;
            } else {
                Key key = _iteratorExchange.getKey();
                Key spareKey = _iteratorExchange.getAuxiliaryKey1();
                key.copyTo(spareKey);
                int depth = spareKey.getDepth();
                spareKey.cut();
                _keyFilter = new KeyFilter(spareKey).append(term).limit(depth,
                        depth);
            }
        }

        public synchronized boolean hasNext() {
            ;
            try {
                boolean result = _iteratorExchange.hasNext(_keyFilter);
                if (!result || _toKey == null) {
                    return result;
                } else {
                    return _toKey.compareTo(_iteratorExchange.getKey()) <= 0;
                }
            } catch (PersistitException de) {
                throw new PersistitMapException(de);
            }
        }

        public synchronized Object next() {
            ;
            try {
                if (!_iteratorExchange.next(_keyFilter) || _toKey != null
                        && _toKey.compareTo(_iteratorExchange.getKey()) <= 0) {
                    throw new NoSuchElementException();
                }
                if (!_allowCM
                        && _iteratorExchange.getChangeCount() != _changeCount) {
                    _changeCount = _iteratorExchange.getChangeCount();
                    throw new ConcurrentModificationException();
                }
                Object result = null;
                switch (_type) {
                case VALUE:
                    result = _iteratorExchange.getValue().get();
                    break;

                case KEY:
                    _iteratorExchange.getKey().indexTo(-1);
                    result = _iteratorExchange.getKey().decode();
                    break;

                default:
                    _iteratorExchange.getKey().indexTo(-1);
                    result = new ExchangeEntry(_iteratorExchange.getKey()
                            .decode(), _iteratorExchange.getValue().get(), this);
                    break;
                }
                _okToRemove = true;
                return result;
            } catch (PersistitException e) {
                throw new PersistitMapException(e);
            }
        }

        public synchronized void remove() {
            ;
            if (!_okToRemove) {
                throw new IllegalStateException();
            }

            if (!_allowCM && _iteratorExchange.getChangeCount() != _changeCount) {
                _changeCount = _iteratorExchange.getChangeCount();
                throw new ConcurrentModificationException();
            }

            boolean removed = false;
            long changeCount = _iteratorExchange.getChangeCount();
            try {
                removed = _iteratorExchange.remove();
            } catch (PersistitException e) {
                throw new PersistitMapException(e);
            }
            if (!removed) {
                throw new NoSuchElementException();
            } else {
                _pm.adjustSize(changeCount, -1);

                if (!_allowCM
                        && _iteratorExchange.getChangeCount() != changeCount + 1) {
                    _changeCount = _iteratorExchange.getChangeCount();
                    throw new ConcurrentModificationException();
                }
            }
            _changeCount = _iteratorExchange.getChangeCount();
            _okToRemove = false;
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
    public Set keySet() {
        if (_keySet == null) {
            _keySet = new AbstractSet() {
                @Override
                public Iterator iterator() {
                    return new ExchangeIterator(PersistitMap.this, KEY,
                            _allowConcurrentModification);
                }

                @Override
                public int size() {
                    return PersistitMap.this.size();
                }

                @Override
                public boolean contains(Object k) {
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
    public Collection values() {
        if (_values == null) {
            _values = new AbstractSet() {
                @Override
                public Iterator iterator() {
                    return new ExchangeIterator(PersistitMap.this, VALUE,
                            _allowConcurrentModification);
                }

                @Override
                public int size() {
                    return PersistitMap.this.size();
                }

                @Override
                public boolean contains(Object v) {
                    return PersistitMap.this.containsValue(v);
                }
            };
        }
        return _values;
    }

    /**
     * Returns a set view of the mappings contained in this map. Each element in
     * this set is a Map.Entry. The set is backed by the map, so changes to the
     * map are reflected in the set, and vice-versa. (If the map is modified
     * while an iteration over the set is in progress, the results of the
     * iteration are undefined.) The set supports element removal, which removes
     * the corresponding entry from the map, via the
     * <code>Iterator.remove</code>, <code>Set.remove</code>,
     * <code>removeAll</code>, <code>retainAll</code> and <code>clear</code>
     * operations. It does not support the <code>add</code> or
     * <code>addAll</code> operations.
     * 
     * @return a set view of the mappings contained in this map.
     */
    public Set entrySet() {
        if (_entrySet == null) {
            _entrySet = new AbstractSet() {
                @Override
                public Iterator iterator() {
                    return new ExchangeIterator(PersistitMap.this, ENTRY,
                            _allowConcurrentModification);
                }

                @Override
                public int size() {
                    return PersistitMap.this.size();
                }

                @Override
                public boolean contains(Object v) {
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
    public Comparator comparator() {
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
    public SortedMap subMap(Object fromKey, Object toKey) {
        return new PersistitMap(this, true, fromKey, true, toKey);
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
    public SortedMap headMap(Object toKey) {
        return new PersistitMap(this, false, null, true, toKey);
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
    public SortedMap tailMap(Object fromKey) {
        return new PersistitMap(this, true, fromKey, false, null);
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
    public Object firstKey() {
        toLeftEdge();
        try {
            if (_ex.traverse(_fromKey == null ? Key.GT : Key.GTEQ, false, -1)
                    && (_toKey == null || _toKey.compareTo(_ex.getKey()) > 0)) {
                return _ex.getKey().decode();
            } else
                throw new NoSuchElementException();
        } catch (PersistitException de) {
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
    public Object lastKey() {
        toRightEdge();
        try {
            if (_ex.traverse(Key.LT, false, -1)
                    && (_fromKey == null || _fromKey.compareTo(_ex.getKey()) <= 0)) {
                return _ex.getKey().decode();
            } else
                throw new NoSuchElementException();
        } catch (PersistitException de) {
            throw new PersistitMapException(de);
        }
    }

    // Comparison and hashing

    /**
     * Compares the specified object with this map for equality. Returns
     * <code>true</code> if the given object is also a map and the two maps
     * represent the same mappings. More formally, two maps <code>t1</code> and
     * <code>t2</code> represent the same mappings if
     * <code>t1.keySet().equals(t2.keySet())</code> and for every key
     * <code>k</code> in <code>t1.keySet()</code>,
     * <code> (t1.get(k)==null ? t2.get(k)==null :
     * t1.get(k).equals(t2.get(k))) </code>. This ensures that the
     * <code>equals</code> method works properly across different
     * implementations of the map interface.
     * <p>
     * 
     * @param o
     *            object to be compared for equality with this map.
     * @return <code>true</code> if the specified object is equal to this map.
     */
    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Map))
            return false;

        Map t = (Map) o;

        if (t.size() != size())
            return false;

        try {
            Iterator i = entrySet().iterator();
            while (i.hasNext()) {
                Entry e = (Entry) i.next();
                Object key = e.getKey();
                Object value = e.getValue();
                if (value == null) {
                    if (!(t.get(key) == null && t.containsKey(key))) {
                        return false;
                    }
                } else {
                    if (!value.equals(t.get(key)))
                        return false;
                }
            }
        } catch (ClassCastException unused) {
            return false;
        } catch (NullPointerException unused) {
            return false;
        }
        return true;
    }

    /**
     * Returns the hash code value for this map. The hash code of a map is
     * defined to be the sum of the hash codes of each entry in the map's
     * <code>entrySet()</code> view. This ensures that
     * <code>t1.equals(t2)</code> implies that
     * <code>t1.hashCode()==t2.hashCode()</code> for any two maps
     * <code>t1</code> and <code>t2</code>, as required by the general contract
     * of Object.hashCode.
     * <p>
     * 
     * This implementation iterates over <code>entrySet()</code>, calling
     * <code>hashCode</code> on each element (entry) in the Collection, and
     * adding up the results.
     * 
     * @return the hash code value for this map.
     * @see java.util.Map.Entry#hashCode()
     * @see Object#hashCode()
     * @see Object#equals(Object)
     * @see Set#equals(Object)
     */
    @Override
    public int hashCode() {
        int h = 0;
        Iterator i = entrySet().iterator();
        while (i.hasNext()) {
            h += i.next().hashCode();
        }
        return h;
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
