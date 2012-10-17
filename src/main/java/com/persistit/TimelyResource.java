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

import static com.persistit.TransactionIndex.vh2ts;
import static com.persistit.TransactionStatus.UNCOMMITTED;

import com.persistit.exception.CorruptValueException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.exception.RollbackException;
import com.persistit.exception.TimeoutException;
import com.persistit.exception.WWRetryException;

/**
 * <p>
 * Base class for objects that cache objects within the transaction mechanism. A
 * TimelyResource instance has an identity, such as a TreeName or an Accumulator
 * identifier. The collection of all TimelyResources is managed by the host
 * Persistit instance.
 * </p>
 * <p>
 * A TimelyResource has start and commit timestamps. It is invisible to any
 * transactions that started before start timestamp, in write-write conflict
 * with any concurrent transaction, and available to any transaction that starts
 * after the commit timestamp. If there exists a TimelyResource instance with
 * start and commit timestamps ts1, tc1 then any attempt to add another
 * TimelyResource will fail unless ts2 > tc1.
 * </p>
 * @author peter
 */

/**
 * Implemented by objects managed within the TimelyResource framework. This
 * interface defines one method, {@link #prune()}, which is called when the
 * resource can be discarded.
 */
interface PrunableResource {
    /**
     * Clean up any state held by this resource. For example, when a
     * {@link Tree} is pruned, all pages allocated to its content are
     * deallocated. This method is called when a newer TimelyResource has been
     * created and is visible to all active transactions.
     */
    void prune();
}

class TimelyResource<T extends PrunableResource> {

    private long _version;
    private TimelyResource<T> _previous;
    private final T _resource;

    public TimelyResource(final long versionHandle, final T resource) {
        _version = versionHandle;
        _resource = resource;
    }

    public T getResource() {
        return _resource;
    }

    public TimelyResource<T> getPrevious() {
        return _previous;
    }
    
    private void setPrevious(final TimelyResource<T> tr) {
        _previous = tr;
    }

    public long getVersion() {
        return _version;
    }
    
    class TimelyResourceHead extends SharedResource {

        private final Object _key;

        private volatile TimelyResource<T> _first;

        public TimelyResource<T> getFirst() {
            return _first;
        }

        TimelyResourceHead(final Persistit persistit, final Object key) {
            super(persistit);
            _key = key;
        }

        public Object getKey() {
            return _key;
        }

        public void addVersion(final TimelyResource<T> tr, final Transaction txn) throws PersistitInterruptedException {
            final TransactionIndex ti = _persistit.getTransactionIndex();
            while (true) {
                claim(true);
                if (_first == null) {
                    _first = tr;
                    break;
                }
                try {
                    for (TimelyResource<T> last = _first; last != null; last = last.getPrevious()) {
                        final long version = last.getVersion();
                        final long depends = ti.wwDependency(version, txn.getTransactionStatus(), 0);
                        if (depends == TransactionStatus.TIMED_OUT) {
                            throw new WWRetryException(version);
                        }
                        if (depends != 0 && depends != TransactionStatus.ABORTED) {
                            /*
                             * version is from concurrent txn that already
                             * committed or timed out waiting to see. Either
                             * way, must abort.
                             */
                            txn.rollback();
                            throw new RollbackException();
                        }
                    }
                    /*
                     * No ww-conflict on any TimelyResource on the list, so add
                     * the new one and mark the old one deleted.
                     */
                    tr.setPrevious(_first);
                    _first.delete(txn.getStartTimestamp());
                    _first = tr;
                    /*
                     * Done - exit retry loop here
                     */
                    break;
                    
                } catch (WWRetryException re) {
                    try {
                        final long depends = _persistit.getTransactionIndex().wwDependency(re.getVersionHandle(),
                                txn.getTransactionStatus(), SharedResource.DEFAULT_MAX_WAIT_TIME);
                        if (depends != 0 && depends != TransactionStatus.ABORTED) {
                            /*
                             * version is from concurrent txn that already
                             * committed or timed out waiting to see. Either
                             * way, must abort.
                             */
                            txn.rollback();
                            throw new RollbackException();
                        }
                    } catch (InterruptedException ie) {
                        throw new PersistitInterruptedException(ie);
                    }
                } catch (InterruptedException ie) {
                    throw new PersistitInterruptedException(ie);
                } finally {
                    release();
                }
            }
        }
        
        public void prune() throws TimeoutException, PersistitInterruptedException {
            claim(true);
            try {
                final TransactionIndex ti = _persistit.getTransactionIndex();
                TimelyResource<T> tr = _first;

                long lastVersionHandle = Long.MIN_VALUE;
                long lastVersionTc = UNCOMMITTED;
                long uncommittedTransactionTs = 0;

                TimelyResource<T> previous = null;

                while (tr != null) {
                    boolean keepIt = false;
                    final long versionHandle = tr.getVersion();
                    final long tc = ti.commitStatus(versionHandle, UNCOMMITTED, 0);
                    if (tc >= 0) {
                        if (tc == UNCOMMITTED) {
                            final long ts = vh2ts(versionHandle);
                            if (uncommittedTransactionTs != 0 && uncommittedTransactionTs != ts) {
                                throw new IllegalStateException("Multiple uncommitted versions");
                            }
                            uncommittedTransactionTs = ts;
                            keepIt = true;
                        } else {
                            if (lastVersionIndex != -1 && ti.hasConcurrentTransaction(lastVersionTc, tc)) {
                                keepIt = true;
                            }
                            // Note: versions and tcs can be the same when there
                            // are
                            // multiple steps
                            assert versionHandle >= lastVersionHandle
                                    || vh2ts(versionHandle) == vh2ts(lastVersionHandle);
                            assert tc >= lastVersionTc || lastVersionTc == UNCOMMITTED;
                            lastVersionHandle = versionHandle;
                            lastVersionTc = tc;
                        }
                    }
                }
            } catch (InterruptedException ie) {
                throw new PersistitInterruptedException(ie);
            } finally {
                release();
            }
        }

    }

}
