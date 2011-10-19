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

import java.util.concurrent.atomic.AtomicInteger;

public class TransactionStatus {

    final static long ABORTED = Long.MIN_VALUE;
    final static long UNCOMMITTED = Long.MAX_VALUE;
    
    /**
     * Start timestamp. This value may only be assigned by
     * {@link TransactionIndex#registerTransaction(Transaction)}.
     */
    private volatile long _ts;

    /**
     * Commit timestamp and status. Value is one of
     * <dl>
     * <dt>Long.MIN_VALUE</dt>
     * <dd>the transaction aborted</dd>
     * <dt>Long.MAX_VALUE</dt>
     * <dd>The transaction is running and has not yet asked to commit.</dd>
     * <dt>v &lt; 0</dt>
     * <dd>Any negative value other than Long.MIN_VALUE indicates the timestamp
     * value at which the transaction started to commit; the commit process is
     * incomplete.</dd>
     * <dt>v &gt; 0</dt>
     * <dd>Any positive value other than Long.MAX_VALUE indicates the commit
     * timestamp; the transaction commit is complete.</dd>
     * </dl>
     * Long.MAX_VALUE:
     */
    private volatile long _tc;
    
    private AtomicInteger _mvvCount = new AtomicInteger();
    /**
     * Pointer to next member of singly-linked list.
     */
    private TransactionStatus _next;
    
    TransactionStatus getNext() {
        return _next;
    }
    
    void setNext(final TransactionStatus next) {
        _next = next;
    }
    
    long getTs() {
        return _ts;
    }
    
    void initialize(final long ts) {
        _ts = ts;
        _tc = UNCOMMITTED;
        _next = null;
        _mvvCount.set(0);
    }
}
