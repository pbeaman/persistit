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

import static com.persistit.TransactionIndex.tss2vh;
import static com.persistit.TransactionIndex.vh2ts;
import static com.persistit.TransactionStatus.ABORTED;
import static com.persistit.TransactionStatus.TIMED_OUT;
import static com.persistit.TransactionStatus.UNCOMMITTED;

import java.util.ArrayList;
import java.util.List;

import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.exception.RollbackException;
import com.persistit.exception.TimeoutException;
import com.persistit.exception.WWRetryException;

/**
 * Transactionally manage versions of objects such as {@link Tree}.
 * 
 * A TimelyResource instance has an identity, such as a TreeName or an
 * Accumulator identifier which serves as its key in a map managed by the host
 * Persistit instance.
 * 
 * 
 * 
 * @author peter
 * 
 * @param <T>
 */
public class TimelyResource<T extends PrunableResource> {

    private final Persistit _persistit;

    private final Object _key;

    private volatile Entry _first;

    TimelyResource(final Persistit persistit, final Object key) {
        _persistit = persistit;
        _key = key;
    }

    public Object getKey() {
        return _key;
    }

    public void addVersion(final T resource, final Transaction txn) throws PersistitException, RollbackException {
        if (txn.isActive()) {
            addVersion(new Entry(tss2vh(txn.getStartTimestamp(), txn.getStep()), resource), txn);
        } else {
            addVersion(new Entry(tss2vh(_persistit.getTimestampAllocator().updateTimestamp(), 0), resource), txn);
        }
    }

    public T getVersion(final Transaction txn) throws TimeoutException, PersistitInterruptedException {
        if (txn.isActive()) {
            return getVersion(txn.getStartTimestamp(), txn.getStep());
        } else {
            return getVersion(UNCOMMITTED, 0);
        }
    }

    public void prune() throws TimeoutException, PersistitException {
        final List<Entry> entriesToPrune = new ArrayList<Entry>();
        synchronized (this) {
            try {
                final TransactionIndex ti = _persistit.getTransactionIndex();

                long lastVersionHandle = Long.MAX_VALUE;
                long lastVersionTc = UNCOMMITTED;
                long uncommittedTransactionTs = 0;

                Entry newer = null;
                Entry latest = null;
                for (Entry tr = _first; tr != null; tr = tr.getPrevious()) {
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
                        } else if (tc > 0) {
                            if (latest == null || ti.hasConcurrentTransaction(tc, lastVersionTc)) {
                                keepIt = true;
                                if (latest == null) {
                                    latest = tr;
                                }
                            }
                            /*
                             * Note: versions and tcs can be the same when there
                             * are multiple steps
                             */
                            assert versionHandle < lastVersionHandle
                                    || vh2ts(versionHandle) == vh2ts(lastVersionHandle);
                            assert tc < lastVersionTc || lastVersionTc == UNCOMMITTED;
                            lastVersionHandle = versionHandle;
                            lastVersionTc = tc;
                        } else {
                            if (tc == 0) {
                                keepIt = true;
                            }
                        }
                    } else {
                        assert tc == ABORTED;
                    }
                    if (keepIt) {
                        newer = tr;
                    } else {
                        entriesToPrune.add(tr);
                        if (newer == null) {
                            _first = tr.getPrevious();
                        } else {
                            newer.setPrevious(tr.getPrevious());
                        }
                    }
                }
                if (_first != null && _first.getResource() == null && _first.getPrevious() == null) {
                    _first = null;
                }
            } catch (final InterruptedException ie) {
                throw new PersistitInterruptedException(ie);
            }
        }
        for (final Entry e : entriesToPrune) {
            e.prune();
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TimelyResource(");
        boolean first = true;
        for (Entry entry = _first; entry != null; entry = entry.getPrevious()) {
            if (sb.length() > 1000) {
                sb.append("...");
                break;
            }
            if (!first) {
                sb.append(',');
            } else {
                first = false;
            }
            sb.append(entry);
        }
        sb.append(')');
        return sb.toString();
    }

    void addVersion(final Entry tr, final Transaction txn) throws PersistitException, RollbackException {
        final TransactionIndex ti = _persistit.getTransactionIndex();
        Entry toPrune = null;
        while (toPrune == null) {
            try {
                synchronized (this) {
                    if (_first == null) {
                        _first = tr;
                        break;
                    }
                    Entry newer = null;
                    if (_first.getVersion() > tr.getVersion()) {
                        /*
                         * This thread lost a race to make the most recent
                         * version
                         */
                        throw new RollbackException();
                    }
                    for (Entry e = _first; e != null; e = e.getPrevious()) {
                        /*
                         * If this is a version replacing a version created by
                         * this transaction, then as a short-cut, simply remove
                         * the other one and splice this in.
                         */
                        if (e.getVersion() == tr.getVersion()) {
                            tr.setPrevious(e.getPrevious());
                            if (newer == null) {
                                _first = tr;
                            } else {
                                newer.setPrevious(tr);
                            }
                            toPrune = e;
                            break;
                        } else if (txn.isActive()) {
                            final long version = e.getVersion();
                            final long depends = ti.wwDependency(version, txn.getTransactionStatus(), 0);
                            if (depends == TIMED_OUT) {
                                throw new WWRetryException(version);
                            }
                            if (depends != 0 && depends != ABORTED) {
                                /*
                                 * version is from a concurrent txn that already
                                 * committed or timed out waiting to see. Either
                                 * way, must abort.
                                 */
                                throw new RollbackException();
                            }
                        }
                        newer = e;

                    }
                    if (_first != null) {
                        assert tr.getVersion() > _first.getVersion();
                    }
                    tr.setPrevious(_first);
                    _first = tr;
                    /*
                     * Done - exit retry loop here
                     */
                    break;
                }

            } catch (final WWRetryException re) {
                try {
                    final long depends = _persistit.getTransactionIndex().wwDependency(re.getVersionHandle(),
                            txn.getTransactionStatus(), SharedResource.DEFAULT_MAX_WAIT_TIME);
                    if (depends != 0 && depends != ABORTED) {
                        /*
                         * version is from concurrent txn that already committed
                         * or timed out waiting to see. Either way, must abort.
                         */
                        txn.rollback();
                        throw new RollbackException();
                    }
                } catch (final InterruptedException ie) {
                    throw new PersistitInterruptedException(ie);
                }
            } catch (final InterruptedException ie) {
                throw new PersistitInterruptedException(ie);
            }
        }
        if (toPrune != null) {
            toPrune.prune();
        }
    }

    T getVersion(final long ts, final int step) throws TimeoutException, PersistitInterruptedException {
        final TransactionIndex ti = _persistit.getTransactionIndex();
        try {
            for (Entry e = _first; e != null; e = e._previous) {
                final long commitTs = ti.commitStatus(e.getVersion(), ts, step);
                if (commitTs >= 0 && commitTs != UNCOMMITTED) {
                    return e.getResource();
                }
            }
            return null;
        } catch (final InterruptedException e) {
            throw new PersistitInterruptedException(e);
        }
    }

    int versionCount() {
        int count = 0;
        for (Entry e = _first; e != null; e = e._previous) {
            count++;
        }
        return count;
    }

    /**
     * <p>
     * Holder for one instance of an object, such as a {@link Tree}, whose
     * availability is governed by visibility within a transaction.
     * </p>
     * <p>
     * An Entry has a version handle just like a version in an {@link MVV}. It
     * is invisible to any transactions that started before the start timestamp,
     * in write-write conflict with any concurrent transaction, and available to
     * any transaction that starts after the version has committed. Visibility
     * within the transaction that creates the TimelyResourceEntry is determined
     * by the relative step numbers of the current transaction and the version
     * handle. If there exists a TimelyResource instance with start and commit
     * timestamps ts1, tc1 then any attempt to add another TimelyResource will
     * fail unless ts2 > tc1.
     * </p>
     * 
     * @author peter
     */

    private class Entry {

        private final long _version;
        private final T _resource;
        private volatile Entry _previous;

        public Entry(final long versionHandle, final T resource) {
            _version = versionHandle;
            _resource = resource;
        }

        public T getResource() {
            return _resource;
        }

        public Entry getPrevious() {
            return _previous;
        }

        private void setPrevious(final Entry tr) {
            _previous = tr;
        }

        public long getVersion() {
            return _version;
        }

        private void prune() throws PersistitException {
            if (_resource != null) {
                _resource.prune();
            }
            setPrevious(null);
        }

        @Override
        public String toString() {
            String tcStatus;
            try {
                final long tc = _persistit.getTransactionIndex().commitStatus(_version, Long.MAX_VALUE, 0);
                tcStatus = TransactionStatus.tcString(tc);
            } catch (final Exception e) {
                tcStatus = e.toString();
            }
            return String.format("(tc=%s ts=%s)->%s%s", TransactionStatus.versionString(_version), tcStatus, _resource,
                    _previous != null ? "*" : "");
        }
    }

}
