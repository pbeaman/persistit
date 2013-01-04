/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit;

import com.persistit.exception.PersistitException;

/**
 * Methods of the {@link Exchange} class that are safe to use within a
 * {@link Exchange.TraverseVisitor}.
 * 
 * @author peter
 */
public interface ReadOnlyExchange {

    /**
     * Return the {@link Key} associated with this <code>Exchange</code>.
     * 
     * @return This <code>Key</code>.
     */
    public abstract Key getKey();

    /**
     * Return the {@link Value} associated with this <code>Exchange</code>.
     * 
     * @return The <code>Value</code>.
     */
    public abstract Value getValue();

    /**
     * Return the {@link Volume} containing the data accessed by this
     * <code>Exchange</code>.
     * 
     * @return The <code>Volume</code>.
     */
    public abstract Volume getVolume();

    /**
     * Return the {@link Tree} on which this <code>Exchange</code> operates.
     * 
     * @return The <code>Tree</code>
     */
    public abstract Tree getTree();

    /**
     * Return the Persistit instance from which this Exchange was created.
     * 
     * @return The <code>Persistit</code> instance.
     */
    public abstract Persistit getPersistitInstance();

    /**
     * Return the count of structural changes committed to the {@link Tree} on
     * which this <code>Exchange</code> operates. This count includes changes
     * committed by all Threads, including the current one. A structural change
     * is one in which at least one key is inserted or deleted. Replacement of
     * an existing value is not counted.
     * 
     * @return The change count
     */
    public abstract long getChangeCount();

    /**
     * Determines whether the current key has a logical sibling successor,
     * without changing the state of <code>Key</code> or <code>Value</code>.
     * This method is equivalent to {@link Exchange#next()} except that no state
     * is changed.
     * 
     * @return <code>true</code> if the key has a successor
     * 
     * @throws PersistitException
     */
    public abstract boolean hasNext() throws PersistitException;

    /**
     * Determines whether the current key has a successor within the subset of
     * all keys defined by a <code>KeyFilter</code>. This method does not change
     * the state of <code>Key</code> or <code>Value</code>.
     * 
     * @return <code>true</code> if the key has a successor
     * 
     * @throws PersistitException
     */
    public abstract boolean hasNext(KeyFilter filter) throws PersistitException;

    /**
     * Determines whether the current key has a logical sibling successor,
     * without changing the state of <code>Key</code> or <code>Value</code>.
     * This method is equivalent to {@link Exchange#next(boolean)} except that
     * no state is changed.
     * 
     * @param deep
     *            Determines whether the predecessor may be of any logical depth
     *            (<code>true</code>, or must be a restricted logical siblings (
     *            <code>false</code>) of the current key. (See <a
     *            href="Key.html#_keyChildren">Logical Key Children and
     *            Siblings</a>).
     * 
     * @return <code>true</code> if the key has a successor
     * 
     * @throws PersistitException
     */
    public abstract boolean hasNext(boolean deep) throws PersistitException;

    /**
     * Determines whether the current key has a logical sibling predecessor,
     * without changing the state of <code>Key</code> or <code>Value</code>.
     * This method is equivalent to {@link Exchange#previous()} except that no
     * state is changed.
     * 
     * @return <code>true</code> if the key has a predecessor
     * @throws PersistitException
     */
    public abstract boolean hasPrevious() throws PersistitException;

    /**
     * Determines whether the current key has a logical sibling predecessor,
     * without changing the state of <code>Key</code> or <code>Value</code>.
     * This method is equivalent to {@link Exchange#previous(boolean)} except
     * that no state is changed.
     * 
     * @param deep
     *            Determines whether the predecessor may be of any logical depth
     *            (<code>true</code>, or must be a restricted logical siblings (
     *            <code>false</code>) of the current key. (See <a
     *            href="Key.html#_keyChildren">Logical Key Children and
     *            Siblings</a>).
     * 
     * @return <code>true</code> if the key has a predecessor
     * 
     * @throws PersistitException
     */
    public abstract boolean hasPrevious(boolean deep) throws PersistitException;

    /**
     * Determines whether the current key has a predecessor within the subset of
     * all keys defined by a <code>KeyFilter</code>. This method does not change
     * the state of <code>Key</code> or <code>Value</code>.
     * 
     * @return <code>true</code> if the key has a successor
     * 
     * @throws PersistitException
     */
    public abstract boolean hasPrevious(KeyFilter filter) throws PersistitException;

    /**
     * Determines whether the current key has an associated value - that is,
     * whether a {@link Exchange#fetch} operation would return a defined value -
     * without actually changing the state of either the <code>Key</code> or the
     * <code>Value</code>.
     * 
     * @return <code>true</code> if the key has an associated value
     * 
     * @throws PersistitException
     */
    public abstract boolean isValueDefined() throws PersistitException;

    /**
     * Return true if there is at least one key stored in this
     * <code>Exchange</code> 's <code>Tree</code> that is a logical child of the
     * current <code>Key</code>. A logical child is a key that can be formed by
     * appending a value to the parent. (See <a
     * href="Key.html#_keyChildren">Logical Key Children and Siblings</a>).
     * 
     * @return <code>true</code> if the current <code>Key</code> has logical
     *         children
     * @throws PersistitException
     */
    public abstract boolean hasChildren() throws PersistitException;

    /**
     * The transaction context for this Exchange. By default, this is the
     * transaction context of the current thread, and by default, all
     * <code>Exchange</code>s created by a thread share the same transaction
     * context.
     * 
     * @return The <code>Transaction</code> context for this thread.
     */
    public abstract Transaction getTransaction();

}