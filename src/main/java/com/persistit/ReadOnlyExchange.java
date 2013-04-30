/**
 * Copyright 2012 Akiban Technologies, Inc.
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
