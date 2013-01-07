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
    public Key getKey();

    /**
     * Return the {@link Value} associated with this <code>Exchange</code>.
     * 
     * @return The <code>Value</code>.
     */
    public Value getValue();

    /**
     * Return the {@link Volume} containing the data accessed by this
     * <code>Exchange</code>.
     * 
     * @return The <code>Volume</code>.
     */
    public Volume getVolume();

    /**
     * Return the {@link Tree} on which this <code>Exchange</code> operates.
     * 
     * @return The <code>Tree</code>
     */
    public Tree getTree();

    /**
     * Return the Persistit instance from which this Exchange was created.
     * 
     * @return The <code>Persistit</code> instance.
     */
    public Persistit getPersistitInstance();

    /**
     * Return the count of structural changes committed to the {@link Tree} on
     * which this <code>Exchange</code> operates. This count includes changes
     * committed by all Threads, including the current one. A structural change
     * is one in which at least one key is inserted or deleted. Replacement of
     * an existing value is not counted.
     * 
     * @return The change count
     */
    public long getChangeCount();

    /**
     * The transaction context for this Exchange. By default, this is the
     * transaction context of the current thread, and by default, all
     * <code>Exchange</code>s created by a thread share the same transaction
     * context.
     * 
     * @return The <code>Transaction</code> context for this thread.
     */
    public Transaction getTransaction();
}
