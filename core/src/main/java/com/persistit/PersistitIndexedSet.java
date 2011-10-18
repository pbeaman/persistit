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

import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.persistit.exception.TransactionFailedException;

/**
 * <p>
 * Manages objects in a canonical indexed object collection. A
 * <code>PersistitIndexedSet</code> is based on an {@link Exchange}, supplied to
 * the constructor, that denotes the <code>Volume</code>, <code>Tree</code> and
 * parent logical key under which all of its data will be stored. An index
 * stores objects under a key containing a unique OID value. The OID is a Java
 * long integer (64 bits). The OID serves as the object's internal, absolute,
 * permanent identifier within the <code>PersistitIndexedSet</code>.
 * </p>
 * Objects stored in a PersistitIndex have the following key structure:
 * <blockquote>
 * 
 * <pre>
 * {...,"id"}            next unused permanent OID value
 * {...,"id",id}         the object
 * {...,"key",object}    the OID
 * </blockquote>
 * </pre>
 * 
 * </p>
 * <p>
 * Persistit uses the object itself as a secondary key by which to look up the
 * OID. This means that an object stored in a <code>PersistitIndex</code> must
 * either have a registered {@link com.persistit.encoding.KeyCoder} or it must
 * be one of the types having built-in encoding. See {@link Key} for information
 * on how objects are translated to persistent key values.
 * </p>
 * <p>
 * The methods of this class are thread safe. All updates are performed within
 * transactions to ensure that the structure described above always remains
 * consistent.
 * </p>
 * 
 * @version 1.0
 */
public class PersistitIndexedSet {
    protected final Tree _tree;
    protected final KeyState _keyState;
    protected final Persistit _persistit;

    /**
     * Constructs a <code>PersistitIndexedSet</code> over the specified
     * exchange. The <code>Exchange</code> may be changed by the caller after
     * being used in this constructor.
     * 
     * @param exchange
     */
    public PersistitIndexedSet(Exchange exchange) {
        _tree = exchange.getTree();
        _keyState = new KeyState(exchange.getKey());
        _persistit = exchange.getPersistitInstance();
    }

    /**
     * Allocates an <code>Exchange</code> from the Exchange pool. Be sure to
     * release this for reuse.
     * 
     * @return An <code>Exchange</code> set up for this thread's use
     * 
     * @throws PersistitException
     */
    private Exchange allocateExchange() throws PersistitException {
        Exchange exchange = _persistit.getExchange(_tree.getVolume(), _tree.getName(), false);
        _keyState.copyTo(exchange.getKey());
        return exchange;
    }

    /**
     * Removes all objects from the backing store for this indexed set.
     * 
     * @throws PersistitException
     */
    public synchronized void removeAll() throws PersistitException {
        Exchange exchange = allocateExchange();
        exchange.append("key").remove(Key.GTEQ);
        exchange.to("id").remove(Key.GTEQ);
        _persistit.releaseExchange(exchange);
    }

    /**
     * Indicates whether there is an object value associated with this OID,
     * without fully deserializing the object.
     * 
     * @param id
     *            The OID
     * 
     * @return <code>true</code> if there is an object associated with this ID;
     *         otherwise <code>false</code>.
     * 
     * @throws PersistitException
     */
    public boolean isDefined(long id) throws PersistitException {
        Exchange exchange = allocateExchange();
        exchange.append("id").append(id).fetch();
        boolean result = exchange.getValue().isDefined();
        _persistit.releaseExchange(exchange);
        return result;
    }

    /**
     * Indicates whether an equivalent object value is already a member of the
     * set. The equivalence of stored objects is determined by whether the
     * {@link Key} values they generate are the same.
     * 
     * @param object
     *            The object
     * 
     * @return <code>true</code> if there is an equivalent object; otherwise
     *         <code>false</code>.
     * 
     * @throws PersistitException
     */
    public boolean isDefined(Object object) throws PersistitException {
        Exchange exchange = allocateExchange();
        exchange.append("key").append(object).fetch();
        boolean result = exchange.getValue().isDefined();
        _persistit.releaseExchange(exchange);
        return result;
    }

    /**
     * Looks up an Object by its OID.
     * 
     * @param id
     *            The OID
     * 
     * @return The associated Object, or <code>null</code> if there is none.
     * @throws PersistitException
     */
    public Object lookup(long id) throws PersistitException {
        Exchange exchange = allocateExchange();
        exchange.append("id").append(id).fetch();
        Object value;
        if (exchange.getValue().isDefined()) {
            value = exchange.getValue().get();
        } else {
            value = null;
        }
        _persistit.releaseExchange(exchange);
        return value;
    }

    /**
     * Looks up an object ID for an object.
     * 
     * @param object
     *            The Object
     * @return The object ID, or -1 if the Object is currently not represented
     *         in the index.
     * @throws PersistitException
     */
    public long lookup(Object object) throws PersistitException {
        Exchange exchange = allocateExchange();
        exchange.append("key").append(object).fetch();
        long id;
        if (exchange.getValue().isDefined()) {
            id = exchange.getValue().getLong();
        } else {
            id = -1;
        }
        _persistit.releaseExchange(exchange);
        return id;
    }

    /**
     * Puts an <code>Object</code> into the index. If there already is an
     * equivalent object, the stored copy is updated to reflect the state of the
     * supplied object. Otherwise the object is associated with a newly
     * allocated object ID. The equivalence of stored objects is determined by
     * whether the {@link Key} values they generate are the same.
     * 
     * @param object
     *            The <code>Object</code> to store or update
     * 
     * @return The OID
     * 
     * @throws PersistitException
     */
    public synchronized long put(Object object) throws PersistitException {
        Exchange exchange = allocateExchange();
        Transaction txn = exchange.getTransaction();
        for (int attempts = 0; attempts < 10; attempts++) {
            try {
                txn.begin();
                exchange.append("key").append(object).fetch();
                exchange.cut(2);

                boolean insertKey = false;
                long id;

                if (exchange.getValue().isDefined()) {
                    id = exchange.getValue().getLong();
                } else {
                    id = exchange.append("id").incrementValue();
                    exchange.cut(1);
                    insertKey = true;
                }
                exchange.getValue().put(object);
                exchange.append("id").append(id).store();
                exchange.cut(2);

                if (insertKey) {
                    exchange.getValue().put(id);
                    exchange.append("key").append(object).store();
                    exchange.cut(2);
                }
                txn.commit();
                _persistit.releaseExchange(exchange);
                return id; // Normal exit
            } catch (RollbackException re) {
                // It's okay, just repeat the loop.
            } finally {
                txn.end();
            }
        }
        _persistit.releaseExchange(exchange);
        throw new TransactionFailedException();
    }

    /**
     * Replaces the object value associated with an OID. The OID must already
     * exist in the indexed set. This method permits changing an object in such
     * a way its encoded {@link Key} value changes. If the new object value
     * results in a different <code>Key</code> value, this method removes the
     * prior entry and inserts the new one.
     * 
     * @param id
     *            The OID
     * 
     * @param newValue
     *            The new value to be associated with this OID
     * 
     * @return <code>true</code> if the new object is indexed under a different
     *         key value then the old one; otherwise <code>false</code>
     * 
     * @throws PersistitException
     */
    public synchronized boolean replace(long id, Object newValue) throws PersistitException {
        Exchange exchange = allocateExchange();
        Transaction txn = exchange.getTransaction();
        for (int attempts = 0; attempts < 10; attempts++) {
            try {
                txn.begin();
                exchange.append("id").append(id).fetch();
                exchange.cut(2);

                if (!exchange.getValue().isDefined()) {
                    throw new IllegalArgumentException("Undefined object ID");
                } else {
                    KeyState oldKey;
                    KeyState newKey = null;
                    Object oldValue = exchange.getValue().get();

                    exchange.append("key").append(oldValue);
                    oldKey = new KeyState(exchange.getKey());
                    exchange.cut(2);
                    exchange.append("key").append(newValue);

                    boolean differentKeys = !oldKey.equals(exchange.getKey());

                    if (differentKeys) {
                        newKey = new KeyState(exchange.getKey());
                        oldKey.copyTo(exchange.getKey());
                        exchange.remove();
                    }

                    exchange.cut(2);
                    exchange.getValue().put(newValue);
                    exchange.append("id").append(id).store();

                    if (differentKeys) {
                        newKey.copyTo(exchange.getKey());
                        exchange.getValue().put(id);
                        exchange.store();
                    }
                    txn.commit();
                    _persistit.releaseExchange(exchange);
                    return differentKeys;
                }
            } catch (RollbackException re) {
                // It's okay, just repeat the loop.
            } finally {
                txn.end();
            }
        }
        _persistit.releaseExchange(exchange);
        throw new TransactionFailedException();
    }
}
