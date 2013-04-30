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

import static com.persistit.TransactionIndex.tss2vh;
import static com.persistit.TransactionIndex.vh2step;
import static com.persistit.TransactionIndex.vh2ts;
import static com.persistit.TransactionStatus.ABORTED;
import static com.persistit.TransactionStatus.PRIMORDIAL;
import static com.persistit.TransactionStatus.TIMED_OUT;
import static com.persistit.TransactionStatus.UNCOMMITTED;

import java.util.ArrayList;
import java.util.List;

import com.persistit.Version.PrunableVersion;
import com.persistit.Version.VersionCreator;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.exception.RollbackException;
import com.persistit.exception.TimeoutException;
import com.persistit.exception.WWRetryException;

/**
 * <p>
 * Transactionally manage multiple versions of a resource. For example, a
 * resource which caches state created and either committed or rolled back by a
 * transaction may be use a TimelyResource to hold its versions. Each version
 * must implement the {@link Version} interface and may optionally implement
 * {@link PrunableVersion} in which case its {@link PrunableVersion#prune()}
 * method is called when <code>TimelyResource</code> detects that the version is
 * obsolete.
 * </p>
 * <p>
 * The method {@link #addVersion(Version, Transaction)} attempts to add a new
 * version on behalf of the supplied transaction.
 * </p>
 * <p>
 * The method {@link #getVersion()} returns the snapshot version associated with
 * the transaction, in other words, an instance of a P which was either
 * committed last before this transaction started, or which was created by this
 * transaction. If there is no such version the method returns <code>null</code>
 * .
 * </p>
 * <p>
 * The method {@link #getVersion(com.persistit.Version.VersionCreator)} does the
 * same thing, except that if there is no snapshot version the
 * {@link VersionCreator#createVersion(TimelyResource)} method is called to
 * construct a new version.
 * </p>
 * 
 * @author peter
 * 
 * @param <V>
 *            specific type of {@link Version} this <code>TimelyResource</code>
 *            manages
 */
public class TimelyResource<V extends Version> {

    private final Persistit _persistit;
    private volatile Entry _first;

    public TimelyResource(final Persistit persistit) {
        _persistit = persistit;
        _persistit.addTimelyResource(this);
    }

    private long tss2v(final Transaction txn) {
        if (txn == null || !txn.isActive()) {
            return tss2vh(_persistit.getTimestampAllocator().updateTimestamp(), 0);
        } else {
            return tss2vh(txn.getStartTimestamp(), txn.getStep());
        }
    }

    public synchronized void delete() throws RollbackException, PersistitException {
        if (_first == null) {
            throw new IllegalStateException("There is no resource to delete");
        }
        if (!_first.isDeleted()) {
            final Transaction txn = _persistit.getTransaction();
            final long version = tss2v(txn);
            if (version == _first.getVersion()) {
                _first.setDeleted();
            } else {
                final V resource = _first.getResource();
                final Entry entry = new Entry(version, resource);
                entry.setDeleted();
                addVersion(entry, txn);
            }
        }
    }

    public void addVersion(final V resource, final Transaction txn) throws PersistitInterruptedException,
            RollbackException {
        if (resource == null) {
            throw new NullPointerException("Null resource");
        }
        addVersion(new Entry(tss2v(txn), resource), txn);
    }

    public V getVersion() throws TimeoutException, PersistitInterruptedException {
        final Entry first = _first;
        if (first != null && first.getVersion() == PRIMORDIAL) {
            return first.getResource();
        }
        final Transaction txn = _persistit.getTransaction();
        return getVersion(tss2v(txn));
    }

    public V getVersion(final VersionCreator<V> creator) throws PersistitException, RollbackException {
        final Entry first = _first;
        if (first != null && first.getVersion() == PRIMORDIAL) {
            return first.getResource();
        }
        final Transaction txn = _persistit.getTransaction();
        V version = getVersion(tss2v(txn));
        if (version == null) {
            version = creator.createVersion(this);
            addVersion(version, txn);
        }
        return version;
    }

    /**
     * @return <code>true</code> if and only if this <code>TimelyResource</code>
     *         has no <code>Version</code> instances.
     */
    public boolean isEmpty() throws TimeoutException, PersistitInterruptedException {
        Entry first = _first;
        if (first == null) {
            return true;
        }
        if (first.getVersion() == PRIMORDIAL) {
            return false;
        }
        first = getEntry(tss2v(_persistit.getTransaction()));
        if (first == null) {
            return true;
        }
        if (first.isDeleted()) {
            return true;
        }
        return false;
    }

    /**
     * 
     * @return Whether this resource exists only within the context of the
     *         current transaction.
     * @throws TimeoutException
     * @throws PersistitInterruptedException
     */

    public boolean isTransactionPrivate(final boolean byStep) throws TimeoutException, PersistitInterruptedException {
        Entry entry = _first;
        if (entry != null && entry.getVersion() == PRIMORDIAL) {
            return false;
        }
        final Transaction txn = _persistit.getTransaction();
        final long versionHandle = tss2v(txn);
        entry = getEntry(versionHandle);
        if (entry == null) {
            return true;
        } else {
            if (byStep) {
                return entry.getVersion() == versionHandle;
            } else {
                return vh2ts(entry.getVersion()) == vh2ts(versionHandle);
            }
        }
    }

    /**
     * @return Count of versions currently being managed.
     */
    int getVersionCount() {
        int count = 0;
        for (Entry e = _first; e != null; e = e._previous) {
            count++;
        }
        return count;
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

    synchronized void setPrimordial() {
        if (_first != null && _first.getPrevious() == null) {
            _first.setPrimordial();
        } else {
            throw new IllegalStateException("Cannot be made primordial: " + this);
        }
    }

    /**
     * Remove all obsolete <code>Version</code> instances. For each instance
     * that implements <code>PrunableVersion</code>, invoke its
     * {@link PrunableVersion#prune()} method.
     * 
     * @throws TimeoutException
     * @throws PersistitException
     */
    void prune() throws TimeoutException, PersistitException {
        final List<Entry> entriesToPrune = new ArrayList<Entry>();
        PrunableVersion versionToVacate = null;

        final TransactionIndex ti = _persistit.getTransactionIndex();

        synchronized (this) {
            try {

                Entry newer = null;
                Entry latest = null;
                boolean isPrimordial = true;
                long lastCommit = UNCOMMITTED;

                for (Entry entry = _first; entry != null; entry = entry.getPrevious()) {
                    boolean keepIt = false;
                    final long versionHandle = entry.getVersion();
                    final long tc = ti.commitStatus(versionHandle, UNCOMMITTED, 0);
                    if (tc >= PRIMORDIAL) {
                        if (tc == UNCOMMITTED) {
                            keepIt = true;
                            isPrimordial = false;
                        } else {
                            final boolean hasConcurrent = ti.hasConcurrentTransaction(tc, lastCommit);
                            if (latest == null || hasConcurrent) {
                                keepIt = true;
                                if (latest == null) {
                                    latest = entry;
                                }
                            }
                            if (keepIt && ti.hasConcurrentTransaction(0, tc)) {
                                isPrimordial = false;
                            }
                        }
                        lastCommit = tc;
                    } else {
                        assert tc == ABORTED;
                    }
                    if (keepIt) {
                        newer = entry;
                    } else {
                        if (tc == ABORTED ^ entry.isDeleted()) {
                            entriesToPrune.add(entry);
                        }
                        if (newer == null) {
                            _first = entry.getPrevious();
                        } else {
                            newer.setPrevious(entry.getPrevious());
                        }
                    }
                }
                if (isPrimordial && _first != null) {
                    assert _first.getPrevious() == null;
                    if (_first.isDeleted()) {
                        final V version = _first.getResource();
                        if (version instanceof PrunableVersion) {
                            versionToVacate = (PrunableVersion) version;
                        }
                        entriesToPrune.add(_first);
                        _first = null;
                    } else {
                        _first.setPrimordial();
                    }
                }
            } catch (final InterruptedException ie) {
                throw new PersistitInterruptedException(ie);
            }
        }
        for (final Entry e : entriesToPrune) {
            if (versionToVacate != null) {
                versionToVacate.vacate();
            }
            e.prune();
        }
    }

    /**
     * Helper method that adds an Entry containing a Version and its timestamp
     * information. This method checks for write-write dependencies throws a
     * RollbackException if there is a conflict.
     * 
     * @param entry
     * @param txn
     * @throws PersistitException
     * @throws RollbackException
     */
    private void addVersion(final Entry entry, final Transaction txn) throws PersistitInterruptedException,
            RollbackException {
        final TransactionIndex ti = _persistit.getTransactionIndex();
        while (true) {
            try {
                synchronized (this) {
                    if (_first != null) {
                        if (_first.getVersion() > entry.getVersion()) {
                            /*
                             * This thread lost a race to make the most recent
                             * version
                             */
                            throw new RollbackException();
                        }
                        if (txn.isActive()) {
                            for (Entry e = _first; e != null; e = e.getPrevious()) {
                                final long version = e.getVersion();
                                final long depends = ti.wwDependency(version, txn.getTransactionStatus(), 0);
                                if (depends == TIMED_OUT) {
                                    throw new WWRetryException(version);
                                }
                                if (depends != 0 && depends != ABORTED) {
                                    /*
                                     * version is from a concurrent transaction
                                     * that already committed or timed out
                                     * waiting to see. Either way, must abort.
                                     */
                                    throw new RollbackException();
                                }
                            }
                        }
                    }
                    entry.setPrevious(_first);
                    _first = entry;
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
                        throw new RollbackException();
                    }
                } catch (final InterruptedException ie) {
                    throw new PersistitInterruptedException(ie);
                }
            } catch (final InterruptedException ie) {
                throw new PersistitInterruptedException(ie);
            }
        }
    }

    /**
     * Get the <code>Version</code> from the snapshot view specified by the
     * supplied version handle.
     * 
     * @param version
     *            versionHandle
     * @return <code>Version</code> for given version handle
     * @throws TimeoutException
     * @throws PersistitInterruptedException
     */
    V getVersion(final long version) throws TimeoutException, PersistitInterruptedException {
        final Entry e = getEntry(version);
        return e == null ? null : e.getResource();
    }

    Entry getEntry(final long version) throws TimeoutException, PersistitInterruptedException {
        final TransactionIndex ti = _persistit.getTransactionIndex();
        try {
            /*
             * Note: not necessary to synchronize here. A concurrent transaction
             * may modify _first, but this method does not need to see that
             * version since it has not been committed. Conversely, if there is
             * some transaction that committed before this transaction's start
             * timestamp, then there is a happened-before relationship due to
             * the synchronization in transaction registration; since _first is
             * volatile, we are guaranteed to see the modification made by the
             * committed transaction.
             */
            for (Entry e = _first; e != null; e = e._previous) {
                final long commitTs = ti.commitStatus(e.getVersion(), vh2ts(version), vh2step(version));
                if (commitTs >= 0 && commitTs != UNCOMMITTED) {
                    if (e.isDeleted()) {
                        return null;
                    }
                    return e;
                }
            }
            return null;
        } catch (final InterruptedException e) {
            throw new PersistitInterruptedException(e);
        }
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

        private long _version;
        private V _resource;
        private volatile boolean _deleted;
        private volatile Entry _previous;

        private Entry(final long versionHandle, final V resource) {
            _version = versionHandle;
            _resource = resource;
        }

        private V getResource() {
            return _resource;
        }

        private void setResource(final V resource) {
            _resource = resource;
        }

        private Entry getPrevious() {
            return _previous;
        }

        private void setPrevious(final Entry tr) {
            _previous = tr;
        }

        private long getVersion() {
            return _version;
        }

        private void setPrimordial() {
            _version = PRIMORDIAL;
        }

        private void setDeleted() {
            _deleted = true;
        }

        private boolean isDeleted() {
            return _deleted;
        }

        private boolean prune() throws PersistitException {
            if (_resource instanceof PrunableVersion) {
                return ((PrunableVersion) _resource).prune();
            } else {
                return true;
            }
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
            return String.format("(ts=%s tc=%s)->%s%s", TransactionStatus.versionString(_version), tcStatus, _resource,
                    _previous != null ? "*" : "");
        }
    }

}
