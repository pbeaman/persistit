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

import static com.persistit.JournalRecord.getLength;
import static com.persistit.JournalRecord.getType;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.persistit.JournalRecord.CU;
import com.persistit.JournalRecord.DR;
import com.persistit.JournalRecord.DT;
import com.persistit.JournalRecord.SR;
import com.persistit.JournalRecord.TC;
import com.persistit.JournalRecord.TS;
import com.persistit.TimestampAllocator.Checkpoint;
import com.persistit.TransactionalCache.Update;
import com.persistit.exception.InvalidKeyException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.RollbackException;
import com.persistit.exception.TimeoutException;
import com.persistit.util.Debug;
import com.persistit.util.InternalHashSet;

/**
 * <p>
 * Represents the transaction context for atomic units of work performed by
 * Persistit. The application determines when to {@link #begin}, {@link #commit}, {@link #rollback} and {@link #end} transactions. Once a transaction has
 * started, no update operation performed within its context will actually be
 * written to the database until <code>commit</code> is performed. At that
 * point, all the updates are written atomically - that is, completely or not at
 * all. If the application is abruptly terminated while <code>commit</code> is
 * writing updates to the database, they will be completed during recovery
 * processing when the Persistit is next initialized.
 * </p>
 * <p>
 * All Persistit database operations are performed in the context of a
 * <code>Transaction</code>. If the <code>begin</code> operation has not
 * explicitly been called then Persistit behaves as if each database update
 * operation is implicitly preceded by invocation of <code>begin</code> and
 * concluded by invocation of <code>commit</code> and <code>end</code> to
 * perform a memory commit.
 * </p>
 * <h2>Lexical Scoping</h2>
 * <p>
 * Applications using Persistit transactions must terminate the scope of any
 * transaction. To do this, applications must ensure that whenever the
 * <code>begin</code> method is called, there is a concluding invocation of the
 * <code>end</code> method. The <code>commit</code> and <code>rollback</code>
 * methods do not end the scope of a transaction; they merely signify the
 * transaction's outcome when <code>end</code> is invoked. Applications should
 * follow either the <a href="#_pattern1"><code>try/finally</code></a> or the <a
 * href="#_pattern2"> <code>TransactionRunnable</code></a> pattern to ensure
 * correctness.
 * </p>
 * <p>
 * <a name="diskVsMemoryCommit">
 * <h2>Memory and Disk Commits</h2>
 * A transaction is capable of performing either a <i>disk commit</i> or a
 * <i>memory commit</i>. A disk commit synchronously forces all pending database
 * updates to the volume files; a memory commit allows the normal lazy writing
 * mechanism to perform the writes. Memory commits are much faster because the
 * results of numerous update operations are often aggregated and written to
 * disk in a much smaller number of I/O operations. However, when using memory
 * commits, it is possible that the recovered state of a database after an
 * abrupt termination of the JVM will not contain transactions that were
 * committed shortly before the termination.
 * </p>
 * <p>
 * For memory commits, the state of the database after restart will be such that
 * for any memory committed transaction T, either all or none of its
 * modifications will be present in the recovered database. Further, if a
 * transaction T2 reads or updates data that was written by any other
 * transaction T1, and if T2 is present in the recovered database, then so is
 * T1. Any transaction that was in progress, but had not been committed at the
 * time of the failure, is guaranteed not to be present in the recovered
 * database. Lazy writing writes all updated pages to disk within within a
 * reasonable period of time (several seconds), so that the likelihood of
 * missing data is relatively small. Thus the recovered database is consistent
 * with some valid execution schedule of committed transactions and is
 * <i>nearly</i> up to date - within seconds - of the abrupt termination. For
 * many applications this guarantee is sufficient for correct operation.
 * </p>
 * <h2>Optimistic Concurrent Scheduling</h2>
 * <p>
 * Persistit normally schedules concurrently executing transactions
 * optimistically, meaning that Persistit does not implicitly lock any data
 * records that a transaction reads or writes. Instead, it allows each
 * transaction to proceed concurrently and then verifies that no other thread
 * has changed data that the transaction has relied upon before it commits. If
 * conflicting changes are found, then Persistit rolls back all changes
 * performed within the scope of the transaction and throws a
 * <code>RollbackException</code>. In most database applications, such a
 * collision is relatively rare, and the application can simply retry the
 * transaction with a high likelihood of success after a small number of
 * retries. To minimize the likelihood of collisions, applications should favor
 * short transactions with small numbers of database operations when possible
 * and practical.
 * </p>
 * <a name="_pattern1" /> <h3>The try/finally Pattern</h3>
 * <p>
 * The following code fragment illustrates a transaction executed with up to to
 * RETRY_COUNT retries. If the <code>commit</code> method succeeds, the whole
 * transaction is completed and the retry loop terminates. If after RETRY_COUNT
 * retries, <code>commit</code> has not been successfully completed, the
 * application throws a <code>TransactionFailedException</code>.
 * </p>
 * <blockquote><code><pre>
 *     Transaction txn = Persistit.getTransaction();
 *     int remainingAttempts = RETRY_COUNT;
 *     for (;;)
 *     {
 *         txn.begin();         // Begins transaction scope
 *         try
 *         {
 *             //
 *             // ...perform Persistit fetch, remove and store operations...
 *             //
 *             txn.commit();     // attempt to commit the updates
 *             break;            // Exit retry loop
 *         }
 *         catch (RollbackException re)
 *         {
 *             if (--remainingAttempts &lt; 0)
 *             {
 *                 throw new TransactionFailedException(); 
 *             {
 *         }
 *         finally
 *         {
 *             txn.end();       // Ends transaction scope.  Implicitly 
 *                              // rolls back all updates unless
 *                              // commit has completed successfully.
 *         }
 *     }
 * }
 * </pre></code></blockquote> </p> <a name="_pattern2" /> <h3>The
 * TransactionRunnable Pattern</h3>
 * <p>
 * As an alternative, the application can embed the actual database operations
 * within an implementation of the {@link TransactionRunnable} interface and
 * invoke the {@link #run} method to execute it. The retry logic detailed in the
 * fragment shown above is handled automatically by <code>run</code>; it could
 * be rewritten as follows: <blockquote><code><pre>
 *     Transaction txn = Persistit.getTransaction();
 *     txn.run(new TransactionRunnable()
 *     {
 *         public void runTransaction()
 *         throws PersistitException, RollbackException
 *         {
 *             //
 *             //...perform Persistit fetch, remove and store operations...
 *             //
 *         }
 *     }, RETRY_COUNT, 0);
 * </pre></code></blockquote>
 * </p>
 * <p>
 * Optimistic concurrency control works well when the likelihood of conflicting
 * transactions - that is, concurrent execution of two or more transactions that
 * modify the same database records or that read records that other another
 * transaction is updating - is low. For most applications this assumption is
 * valid.
 * </p>
 * <p>
 * For best performance, applications in which multiple threads frequently
 * operate on overlapping data such that rollbacks are likely, the application
 * should use an external locking mechanism to prevent or reduce the likelihood
 * of collisions.
 * </p>
 * <a name="_pessimisticMode"/> <h3>Pessimistic Scheduling Mode</h3>
 * <p>
 * Persistit also provides an internal mechanism to ensure that every
 * transaction is eventually permitted to run to completion without rollback.
 * The <i>pessimistic retry threshold</i>, accessible through
 * {@link #getPessimisticRetryThreshold()} and
 * {@link #setPessimisticRetryThreshold(int)}, determines the maximum number of
 * retries allowed for a transaction using optimistic scheduling. Once the
 * number of times a transaction has been rolled back since the last successful
 * <code>commit</code> operation reaches this threshold, Persistit switches to
 * <i>pessimistic scheduling mode</i> in which the failing transaction is given
 * exclusive access to the Persistit database. To force a transaction to execute
 * in pessimistic mode on the first attempt, an application can set the
 * pessimistic retry threshold to zero.
 * </p>
 * <p>
 * An application can examine counts of commits, rollbacks and rollbacks since
 * the last successful commit using {@link #getCommittedTransactionCount()},
 * {@link #getRolledBackTransactionCount()} and
 * {@link #getRolledBackSinceLastCommitCount()}, respectively.
 * </p>
 * <a name="#_scopedCodePattern"/> <h2>Nested Transaction Scope</h2>
 * <p>
 * Persistit supports nested transactions by counting the number of nested
 * {@link #begin} and {@link #end} operations. Each invocation of
 * <code>begin</code> increments this count and each invocation of
 * <code>end</code> decrements the count. These methods are intended to be used
 * in a standard essential pattern, shown here, to ensure that the scope of of
 * the transaction is reliably determined by the lexical the structure of the
 * code rather than conditional logic: <blockquote>
 * 
 * <pre>
 * <code>
 *     <b>txn.begin();</b>
 *     try
 *     {
 *         // application transaction logic here
 *         <b>txn.commit();</b>
 *     }
 *     finally
 *     {
 *         <b>txn.end();</b>
 *     }
 * </code>
 * </pre>
 * 
 * </blockquote> This pattern ensures that the transaction scope is ended
 * properly regardless of whether the application code throws an exception or
 * completes and commits normally.
 * </p>
 * <p>
 * The {@link #commit} method performs the actual commit operation only when the
 * current nested level count is 1. That is, if <code>begin</code> has been
 * invoked N times, then <code>commit</code> will actually commit the data only
 * if <code>end</code> has been invoked N-1 times. Thus data updated by an inner
 * (nested) transaction is never actually committed until the outermost
 * <code>commit</code> is called. This permits transactional code to invoke
 * other code (possibly an opaque library supplied by a third party) that may
 * itself <code>begin</code> and <code>commit</code> transactions.
 * </p>
 * <p>
 * Invoking {@link #rollback} removes all pending but uncommitted updates, marks
 * the current transaction scope as <i>rollback pending</i> and throws a
 * <code>RollbackException</code>. Any subsequent attempt to perform any
 * Persistit operation, including <code>commit</code> in the current transaction
 * scope, will fail with a <code>RollbackException</code>. The
 * <code>commit</code> method throws a <code>RollbackException</code> (and
 * therefore does not commit the pending updates) if either the transaction
 * scope is marked <i>rollback pending</i> by a prior call to
 * <code>rollback</code> or if the attempt to commit the updates would generate
 * an inconsistent database state.
 * </p>
 * <p>
 * Application developers should beware that the <code>end</code> method
 * performs an implicit rollback if <code>commit</code> has not completed. If an
 * application fails to call <code>commit</code>, the transaction will silently
 * fail. The <code>end</code> method sends a warning message to the log
 * subsystem when this happens, but does not throw an exception. The
 * <code>end</code> method is designed this way to allow an exception thrown
 * within the application code to be caught and handled without being obscured
 * by a RollbackException thrown by <code>end</code>. But as a consequence,
 * developers must carefully verify that the <code>commit</code> method is
 * always invoked when the transaction completes normally. Upon completion of
 * the <code>end</code> method, an application can query whether a rollback
 * occurred with the {@link #getRollbackException()} method. This method returns
 * <code>null</code> if the transaction committed and ended normal; otherwise it
 * contains a {@link com.persistit.exception.RollbackException} whose stack
 * trace indicates the location of the implicit rollback.
 * 
 * @author pbeaman
 * @version 1.1
 */
public class Transaction {
    public final static int DEFAULT_PESSIMISTIC_RETRY_THRESHOLD = 3;

    private final static int DEFAULT_TXN_BUFFER_SIZE = 64 * 1024;

    private final static int MIN_TXN_BUFFER_CAPACITY = 1024;

    private final static int MAX_TXN_BUFFER_CAPACITY = 64 * 1024 * 1024;

    private final static boolean DISABLE_TXN_BUFFER = false;

    private final static String TRANSACTION_TREE_NAME = "_txn_";

    private final static long COMMIT_CLAIM_TIMEOUT = 30000;

    private final static int NEUTERED_LONGREC = 254;

    private static long _idCounter = 100000000;

    private final Persistit _persistit;
    private final SessionId _sessionId;
    private final long _id;
    private int _nestedDepth;
    private int _pendingStoreCount = 0;
    private int _pendingRemoveCount = 0;
    private boolean _rollbackPending;
    private boolean _commitCompleted;
    private RollbackException _rollbackException;

    private boolean _recoveryMode = false;

    private long _rollbackCount = 0;
    private long _commitCount = 0;
    private int _rollbacksSinceLastCommit = 0;

    private int _pessimisticRetryThreshold = DEFAULT_PESSIMISTIC_RETRY_THRESHOLD;

    private ArrayList<DeallocationChain> _longRecordDeallocationList = new ArrayList<DeallocationChain>();

    private Exchange _ex1;

    private Exchange _ex2;

    private final Key _rootKey;

    private final InternalHashSet _touchedPagesSet = new InternalHashSet();

    private final List<Integer> _visbilityOrder = new ArrayList<Integer>();

    private long _rollbackDelay;

    // Valid only during the commit() method
    private AtomicLong _startTimestamp = new AtomicLong(-1);

    // Valid only during the commit() method
    private AtomicLong _commitTimestamp = new AtomicLong(-1);

    // Valid only during the commit() method
    private AtomicBoolean _toDisk = new AtomicBoolean();

    private List<CommitListener> _commitListeners = new ArrayList<CommitListener>();

    private TransactionBuffer _txnBuffer = new TransactionBuffer();

    private Map<TransactionalCache, List<Update>> _transactionCacheUpdates = new HashMap<TransactionalCache, List<Update>>();

    private Checkpoint _transactionalCacheCheckpoint;

    private Map<Integer, WeakReference<Tree>> _treeCache = new HashMap<Integer, WeakReference<Tree>>();

    private class TransactionBuffer implements TransactionWriter {

        private ByteBuffer _bb = ByteBuffer.allocate(DEFAULT_TXN_BUFFER_SIZE);

        @Override
        public boolean writeTransactionStartToJournal(long startTimestamp) throws PersistitIOException {
            if (DISABLE_TXN_BUFFER || _bb.remaining() < TS.OVERHEAD) {
                return false;
            }
            _persistit.getJournalManager().writeTransactionStartToJournal(_bb, TS.OVERHEAD, startTimestamp);
            return true;

        }

        @Override
        public boolean writeTransactionCommitToJournal(long timestamp, long commitTimestamp)
                throws PersistitIOException {
            if (DISABLE_TXN_BUFFER || _bb.remaining() < TC.OVERHEAD) {
                return false;
            }
            _persistit.getJournalManager()
                    .writeTransactionCommitToJournal(_bb, TC.OVERHEAD, timestamp, commitTimestamp);
            return true;

        }

        @Override
        public boolean writeStoreRecordToJournal(long timestamp, int treeHandle, Key key, Value value)
                throws PersistitIOException {
            final int recordSize = SR.OVERHEAD + key.getEncodedSize() + value.getEncodedSize();
            if (DISABLE_TXN_BUFFER || _bb.remaining() < recordSize) {
                return false;
            }
            _persistit.getJournalManager()
                    .writeStoreRecordToJournal(_bb, recordSize, timestamp, treeHandle, key, value);
            return true;
        }

        @Override
        public boolean writeDeleteRecordToJournal(long timestamp, int treeHandle, Key key1, Key key2)
                throws PersistitIOException {
            final int recordSize = DR.OVERHEAD + key1.getEncodedSize() + key2.getEncodedSize();
            if (DISABLE_TXN_BUFFER || _bb.remaining() < recordSize) {
                return false;
            }
            _persistit.getJournalManager().writeDeleteRecordToJournal(_bb, recordSize, timestamp, treeHandle, key1,
                    key2);
            return true;
        }

        @Override
        public boolean writeDeleteTreeToJournal(long timestamp, int treeHandle) throws PersistitIOException {
            if (DISABLE_TXN_BUFFER || _bb.remaining() < DT.OVERHEAD) {
                return false;
            }
            _persistit.getJournalManager().writeDeleteTreeToJournal(_bb, DT.OVERHEAD, timestamp, treeHandle);
            return true;
        }

        @Override
        public boolean writeCacheUpdatesToJournal(final long timestamp, final long cacheId, final List<Update> updates)
                throws PersistitIOException {
            int estimate = CU.OVERHEAD;
            for (int index = 0; index < updates.size(); index++) {
                estimate += (1 + updates.get(index).size());
            }
            if (DISABLE_TXN_BUFFER || _bb.remaining() < estimate) {
                return false;
            }
            _persistit.getJournalManager().writeCacheUpdatesToJournal(_bb, timestamp, cacheId, updates);
            return true;
        }

        void clear() {
            _bb.clear();
        }

        void flip() {
            _bb.flip();
        }

        int capacity() {
            return _bb.capacity();
        }

        void allocate(final int capacity) {
            _bb = ByteBuffer.allocate(capacity);
        }
    }

    /**
     * Call-back for commit() processing. Methods of this class are called
     * during execution of the {@link Transaction#commit()} method.
     * Implementations of this interface must return quickly without blocking
     * for synchronization of physical I/O.
     * <p />
     * The default implementation of this class does nothing.
     */
    public static interface CommitListener {
        /**
         * Called when a transaction is committed. The implementation may
         * perform actions consistent with the committed transaction.
         */
        public void committed();

        /**
         * Called when a transaction is rolled back.
         */
        public void rolledBack();
    }

    /**
     * Implementation of CommitListener that does nothing.
     * 
     */
    public static class DefaultCommitListener implements CommitListener {
        @Override
        public void committed() {
            // do nothing
        }

        @Override
        public void rolledBack() {
            // do nothing
        }
    }

    private static class TouchedPage extends InternalHashSet.Entry {
        final Volume _volume;
        final long _pageAddr;
        final long _timestamp;

        private TouchedPage(Buffer buffer) {
            _volume = buffer.getVolume();
            _pageAddr = buffer.getPageAddress();
            _timestamp = buffer.getTimestamp();
        }

        @Override
        public int hashCode() {
            return _volume.hashCode() ^ ((int) _pageAddr);
        }

        @Override
        public String toString() {
            return "Touched(" + _volume.getPath() + ", page " + _pageAddr + ", timestamp=" + _timestamp + ")";
        }
    }

    private static class DeallocationChain {
        Volume _volume;
        long _leftPage;
        long _rightPage;

        DeallocationChain(Volume volume, long leftPage, long rightPage) {
            _volume = volume;
            _leftPage = leftPage;
            _rightPage = rightPage;
        }

        @Override
        public int hashCode() {
            return (int) _leftPage ^ (int) _rightPage ^ _volume.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || !(o instanceof DeallocationChain)) {
                return false;
            }
            DeallocationChain dc = (DeallocationChain) o;
            return (dc._leftPage == _leftPage && dc._rightPage == _rightPage && dc._volume == _volume);
        }
    }

    /**
     * Creates a new transaction context. Any transaction performed within this
     * context will be isolated from all other transactions.
     */
    Transaction(final Persistit persistit, final SessionId sessionId) {
        this(persistit, sessionId, nextId());
    }

    /**
     * Creates a Transaction context with a specified ID value. This is used
     * only during recovery processing.
     * 
     * @param id
     */
    private Transaction(final Persistit persistit, final SessionId sessionId, final long id) {
        _persistit = persistit;
        _sessionId = sessionId;
        _id = id;
        _rollbackDelay = persistit.getLongProperty("rollbackDelay", 10, 0, 100000);
        _rootKey = new Key(_persistit);
    }

    private static synchronized long nextId() {
        return ++_idCounter;
    }

    void setupExchanges() throws PersistitException {
        if (_ex1 == null) {
            int saveDepth = _nestedDepth;
            _nestedDepth = 0;
            try {
                Volume txnVolume = _persistit.createTemporaryVolume();
                _ex1 = _persistit.getExchange(txnVolume, TRANSACTION_TREE_NAME + _id, true);
                _ex2 = new Exchange(_ex1);
                _ex1.ignoreTransactions();
                _ex2.ignoreTransactions();
                _ex1.removeAll();
            } finally {
                _nestedDepth = saveDepth;
            }
        }
    }

    void close() throws PersistitException {
        final Exchange ex = _ex1;
        if (ex != null) {
            ex.getVolume().close();
            _ex1 = null;
            _ex2 = null;
        }
    }

    private int handleForTree(Tree tree) throws PersistitException {
        return _persistit.getJournalManager().handleForTree(tree);
    }

    /**
     * Given a handle, return the corresponding Tree. For better performance,
     * this method caches a handle->Tree map privately in this Transaction to
     * avoid synchronizing on the <code>JournalManager</code>.
     * 
     * @param handle
     * @return the corresponding <code>Tree</code>
     * @throws PersistitException
     */
    private Tree treeForHandle(int handle) throws PersistitException {
        final Integer key = Integer.valueOf(handle);
        WeakReference<Tree> ref = _treeCache.get(key);
        Tree tree = ref == null ? null : ref.get();
        if (tree == null || !tree.isValid()) {
            tree = _persistit.getJournalManager().treeForHandle(handle);
            if (tree != null) {
                _treeCache.put(key, new WeakReference<Tree>(tree));
            }
        }
        return tree;
    }

    /**
     * Throws a {@link RollbackException} if this transaction context has a
     * rollback transition pending.
     * 
     * @throws RollbackException
     */
    public void checkPendingRollback() throws RollbackException {
        if (_rollbackPending) {
            throw _rollbackException;
        }
    }

    /**
     * Indicates whether the application has invoked {@link #begin} but has not
     * yet invoked a matching {@link #commit} or {@link #rollback}.
     * 
     * @return <code>true</code> if a transaction has begun, but is not yet
     *         either committed or rolled back
     */
    public boolean isActive() {
        return _nestedDepth > 0;
    }

    /**
     * Indicates whether the {@link #commit} method has run to successful
     * completion at the current nested level. If that level is 1, then
     * successful completion of <code>commit</code> means that the transaction
     * has actually been committed to the database. Within the scope of inner
     * (nested) transactions, data is not actually committed until the outermost
     * transaction scope commits.
     * 
     * @return <code>true</code> if the current transaction scope has completed
     *         its <code>commit</code> operation; otherwise <code>false</code>.
     */
    public boolean isCommitted() {
        return _commitCompleted;
    }

    /**
     * Indicates whether the current transaction scope has been marked for
     * rollback. If so, and if this is an inner (nested) transaction scope, all
     * updates performed by the containing scope will also be rolled back.
     * 
     * @return <code>true</code> if the current transaction is marked for
     *         rollback; otherwise <code>false</code>.
     */
    public boolean isRollbackPending() {
        return _rollbackPending;
    }

    /**
     * Start a transaction. If there already is an active transaction then this
     * method merely increments a counter that indicates how many times
     * <code>begin</code> has been called. Application code should ensure that
     * every method that calls <code>begin</code> also invokes <code>end</code>
     * using a <code>try/finally</code> pattern described <a
     * href="#_scopedCodePattern">above</a></code>.
     * 
     * @throws IllegalStateException
     *             if the current transaction scope has already been committed.
     */
    public void begin() throws PersistitException {
        if (_commitCompleted) {
            throw new IllegalStateException("Attempt to begin a committed transaction");
        }

        _rollbackPending = false;
        _rollbackException = null;

        if (_nestedDepth == 0) {
            try {
                setupExchanges();
                clear();
                _commitListeners.clear();
                _transactionCacheUpdates.clear();
                _startTimestamp.set(_persistit.getTimestampAllocator().updateTimestamp());

            } catch (PersistitException pe) {
                _persistit.getLogBase().txnBeginException.log(pe, this);
                throw pe;
            }
            if (!_persistit.getTransactionResourceA().claim(_rollbacksSinceLastCommit >= _pessimisticRetryThreshold,
                    COMMIT_CLAIM_TIMEOUT)) {
                throw new TimeoutException("Unavailable TransactionResourceA lock");
            }
        }
        _nestedDepth++;
    }

    /**
     * <p>
     * Ends the current transaction scope. Application code should ensure that
     * every method that calls <code>begin</code> also invokes <code>end</code>
     * using a <code>try/finally</code> pattern described in <a
     * href="#_scopedCodePattern">above</a></code>.
     * </p>
     * <p>
     * This method implicitly rolls back any pending, uncommitted updates.
     * Updates are committed only if the <code>commit</code> method completes
     * successfully.
     * </p>
     * 
     * @throws IllegalStateException
     *             if there is no current transaction scope.
     */
    public void end() {

        if (_nestedDepth < 1) {
            throw new IllegalStateException("No transaction scope: begin() not called");
        }
        _nestedDepth--;

        // Decide whether this is a rollback or a commit.
        if (!_commitCompleted || _rollbackPending) {
            if (_rollbackException == null) {
                _rollbackException = new RollbackException();
            }
            if (!_rollbackPending) {
                _persistit.getLogBase().txnNotCommitted.log(_rollbackException);
            }
            _rollbackPending = true;
        }

        // Special handling for the outermost scope.
        if (_nestedDepth == 0) {
            _commitListeners.clear();
            // First release the pessimistic lock if we claimed it.
            if (_rollbackPending) {
                Debug.$assert1.t(_rollbacksSinceLastCommit - _pessimisticRetryThreshold < 20);
            }
            _persistit.getTransactionResourceA().release();
            _startTimestamp.set(-1);
            _commitTimestamp.set(-1);
            _transactionCacheUpdates.clear();
            //
            // Perform rollback if needed.
            //
            if (_rollbackPending) {
                _rollbackCount++;
                _rollbacksSinceLastCommit++;

                try {
                    rollbackUpdates();
                    if (_rollbackDelay > 0) {
                        Thread.sleep(_rollbackDelay); // TODO
                    }
                } catch (PersistitException pe) {
                    _persistit.getLogBase().txnEndException.log(pe, this);
                } catch (InterruptedException ie) {
                }
            } else {
                _commitCount++;
                _rollbacksSinceLastCommit = 0;
            }
            _rollbackPending = false;
            _visbilityOrder.clear();

            Debug.$assert0.t(_touchedPagesSet.size() == 0);
            _touchedPagesSet.clear();
        }
        _commitCompleted = false;
    }

    /**
     * <p>
     * Explicitly rolls back all work done within the scope of this transaction
     * and throws a RollbackException. If this transaction is not active, then
     * this method does nothing. No further updates can be applied within the
     * scope of this transaction.
     * </p>
     * <p>
     * If called within the scope of a nested transaction, this method causes
     * all enclosing transactions to roll back. This ensures that the outermost
     * transaction will not commit if any inner transaction has rolled back.
     * </p>
     * 
     * @throws IllegalStateException
     *             if there is no transaction scope or the current scope has
     *             already been committed.
     * @throws RollbackException
     *             in all other cases
     */
    public void rollback() throws PersistitException {
        if (_commitCompleted) {
            throw new IllegalStateException("Already committed");
        }

        if (_nestedDepth < 1) {
            throw new IllegalStateException("No transaction scope: begin() not called");
        }

        _rollbackPending = true;
        if (_rollbackException == null) {
            _rollbackException = new RollbackException();
        }
        try {
            rollbackUpdates();
        } catch (PersistitException pe) {
            _persistit.getLogBase().txnRollbackException.log(pe, this);
        }

        for (int index = _commitListeners.size(); --index >= 0;) {
            try {
                _commitListeners.get(index).rolledBack();
            } catch (Exception e) {
                // ignore
            }
        }
        throw _rollbackException;
    }

    /**
     * <p>
     * Commit this transaction. To so do, this method verifies that no data read
     * within the scope of this transaction has changed, and then atomically
     * writes all the updates associated with the transaction scope to the
     * database. This method commits to <a href="#diskVsMemoryCommit">memory</a>
     * so the resulting state is not guaranteed to be durable. Use
     * {@link #commit(boolean)} to commit with durability.
     * </p>
     * <p>
     * If executed within the scope of a nested transaction, this method simply
     * sets a flag indicating that the current transaction level has committed
     * without modifying any data. The commit for the outermost transaction
     * scope is responsible for actually committing the changes.
     * </p>
     * <p>
     * Once an application thread has called <code>commit</code>, no subsequent
     * Persistit database operations are permitted until the corresponding
     * <code>end</code> method has been called. An attempt to store, fetch or
     * remove data after <code>commit</code> has been called throws an
     * <code>IllegalStateException</code>.
     * </p>
     * 
     * @throws PersistitException
     * @throws RollbackException
     */
    public void commit() throws PersistitException, RollbackException {
        commit(false);
    }

    /**
     * <p>
     * Commit this transaction. To so do, this method verifies that no data read
     * within the scope of this transaction has changed, and then atomically
     * writes all the updates associated with the transaction scope to the
     * database. This method optionally commits to <a
     * href="#diskVsMemoryCommit">memory</a> or <a
     * href="#diskVsMemoryCommit">disk</a>.
     * </p>
     * <p>
     * If executed within the scope of a nested transaction, this method simply
     * sets a flag indicating that the current transaction level has committed
     * without modifying any data. The commit for the outermost transaction
     * scope is responsible for actually committing the changes.
     * </p>
     * <p>
     * Once an application thread has called <code>commit</code>, no subsequent
     * Persistit database operations are permitted until the corresponding
     * <code>end</code> method has been called. An attempt to store, fetch or
     * remove data after <code>commit</code> has been called throws an
     * <code>IllegalStateException</code>.
     * </p>
     * 
     * @param toDisk
     *            <code>true</code> to commit to disk, or <code>false</code> to
     *            commit to memory.
     * 
     * @throws PersistitException
     * 
     * @throws RollbackException
     * 
     * @throws IllegalStateException
     *             if no transaction scope is active or this transaction scope
     *             has already called <code>commit</code>.
     */
    public void commit(boolean toDisk) throws PersistitException, RollbackException {

        if (_nestedDepth < 1) {
            throw new IllegalStateException("No transaction scope: begin() not called");
        } else if (_commitCompleted) {
            throw new IllegalStateException("Already committed");
        }

        try {
            if (_rollbackPending)
                throw _rollbackException;
            if (_nestedDepth == 1) {
                boolean done = false;
                done = doCommit(toDisk);
                if (!done && _rollbackPending) {
                    rollback();
                }
            }
            _commitCompleted = true;
        } finally {
            _rollbackPending = _rollbackPending & (_nestedDepth > 0);
        }
    }

    /**
     * <p>
     * Commit this transaction. To so do, this method verifies that no data read
     * within the scope of this transaction has changed, and then atomically
     * writes all the updates associated with the transaction scope to the
     * database. This method optionally commits to <a
     * href="#diskVsMemoryCommit">memory</a> or <a
     * href="#diskVsMemoryCommit">disk</a>.
     * </p>
     * <p>
     * If executed within the scope of a nested transaction, this method simply
     * sets a flag indicating that the current transaction level has committed
     * without modifying any data. The commit for the outermost transaction
     * scope is responsible for actually committing the changes.
     * </p>
     * <p>
     * Once an application thread has called <code>commit</code>, no subsequent
     * Persistit database operations are permitted until the corresponding
     * <code>end</code> method has been called. An attempt to store, fetch or
     * remove data after <code>commit</code> has been called throws an
     * <code>IllegalStateException</code>.
     * </p>
     * 
     * @param commitListener
     *            CommitListener instance whose methods are called when this
     *            Transaction is committed or rolled back
     * 
     * @param toDisk
     *            <code>true</code> to commit to disk, or <code>false</code> to
     *            commit to memory.
     * 
     * @throws PersistitException
     * 
     * @throws RollbackException
     * 
     * @throws IllegalStateException
     *             if no transaction scope is active or this transaction scope
     *             has already called <code>commit</code>.
     */
    public void commit(final CommitListener commitListener, final boolean toDisk) throws PersistitException,
            RollbackException {
        _commitListeners.add(commitListener);
        commit(toDisk);
    }

    /**
     * Returns the nested level count. When no transaction scope is active this
     * method returns 0. Within the outermost transaction scope this this method
     * returns 1. For nested transactions this method returns a value of 2 or
     * higher.
     * 
     * @return The nested transaction depth
     */
    public int getNestedTransactionDepth() {
        return _nestedDepth;
    }

    /**
     * <p>
     * Invokes the {@link TransactionRunnable#runTransaction} method of an
     * object that implements {@link TransactionRunnable} within the scope of a
     * transaction. The <code>TransactionRunnable</code> should neither
     * <code>begin</code> nor <code>commit</code> the transaction; these
     * operations are performed by this method. The
     * <code>TransactionRunnable</code> <i>may</i> explicitly throw a
     * <code>RollbackException</code> in which case the current transaction will
     * be rolled back.
     * </p>
     * <p>
     * This method does not perform retries, so upon the first failed attempt to
     * commit the transaction this method will throw a
     * <code>RollbackException</code>. See
     * {@link #run(TransactionRunnable, int, long, boolean)} for retry handling.
     * This method commits the transaction to <a
     * href="#diskVsMemoryCommit">memory</a>, which means that the update is not
     * guaranteed to be durable.
     * </p>
     * 
     * @param runnable
     * 
     * @return Count of passes needed to complete the transaction. Always 1 on
     *         successful completion.
     * 
     * @throws PersistitException
     */
    public int run(TransactionRunnable runnable) throws PersistitException {
        return run(runnable, 0, 0, false);
    }

    /**
     * <p>
     * Invokes the {@link TransactionRunnable#runTransaction} method of an
     * object that implements {@link TransactionRunnable} within the scope of a
     * transaction. The <code>TransactionRunnable</code> should neither
     * <code>begin</code> nor <code>commit</code> the transaction; these
     * operations are performed by this method. The
     * <code>TransactionRunnable</code> <i>may</i> explicitly or implicitly
     * throw a <code>RollbackException</code> in which case the current
     * transaction will be rolled back.
     * </p>
     * <p>
     * If <code>retryCount</code> is greater than zero, this method will make up
     * to <code>retryCount</code> additional of attempts to complete and commit
     * thetransaction. Once the retry count is exhausted, this method throws a
     * <code>RollbackException</code>.
     * </p>
     * 
     * @param runnable
     *            An application specific implementation of
     *            <code>TransactionRunnable</code> containing logic to access
     *            and update Persistit data.
     * 
     * @param retryCount
     *            Number of attempts (not including the first attempt) to make
     *            before throwing a <code>RollbackException</code>
     * 
     * @param retryDelay
     *            Time, in milliseconds, to wait before the next retry attempt.
     * 
     * @param toDisk
     *            <code>true</code> to commit the transaction to <a
     *            href="#diskVsMemoryCommit">disk</a>, or <code>false</code> to
     *            commit to <a href="#diskVsMemoryCommit">memory</a>
     * 
     * @return Count of attempts needed to complete the transaction
     * 
     * @throws PersistitException
     * @throws RollbackException
     *             If after <code>retryCount+1</code> attempts the transaction
     *             cannot be completed or committed due to concurrent updated
     *             performed by other threads.
     */
    public int run(TransactionRunnable runnable, int retryCount, long retryDelay, boolean toDisk)
            throws PersistitException {
        if (retryCount < 0)
            throw new IllegalArgumentException();
        for (int count = 1;; count++) {
            begin();
            try {
                runnable.runTransaction();
                commit(toDisk);
                return count;
            } catch (RollbackException re) {
                if (retryCount <= 0 || _nestedDepth > 1) {
                    throw re;
                }

                retryCount--;
                if (retryDelay > 0) {
                    try {
                        Thread.sleep(retryDelay);
                    } catch (InterruptedException ie) {
                        throw re;
                    }
                }
            } finally {
                end();
            }
        }
    }

    /**
     * Returns displayable information about this transaction.
     * 
     * @return Information about this Transaction
     */
    @Override
    public String toString() {
        return "Transaction_" + _id + " depth=" + _nestedDepth + " _rollbackPending=" + _rollbackPending
                + " _commitCompleted=" + _commitCompleted;
    }

    /**
     * Returns the internal transaction identifier.
     * 
     * @return The transaction identifier
     */
    public long getId() {
        return _id;
    }

    /**
     * @return the SessionId this Transaction context belongs too.
     */
    public SessionId getSessionId() {
        return _sessionId;
    }

    /**
     * Return the timestamp assigned at the beginning of the commit() process,
     * or -1 if commit is not in progress.
     * 
     * @return transaction start timestamp
     */
    public long getStartTimestamp() {
        return _startTimestamp.get();
    }

    /**
     * Return the timestamp assigned at the end of the commit() process, or -1
     * if commit has not occurred yet.
     * 
     * @return transaction commit timestamp
     */
    public long getCommitTimestamp() {
        return _commitTimestamp.get();
    }

    /**
     * Return the number of transactions committed by this transaction context.
     * 
     * @return The count
     */
    public long getCommittedTransactionCount() {
        return _commitCount;
    }

    /**
     * Return the number of transactions rolled back by this transaction
     * context.
     * 
     * @return The count
     */
    public long getRolledBackTransactionCount() {
        return _rollbackCount;
    }

    /**
     * Return the number of times a transaction in this <code>Transaction</code>
     * context has rolled back since the last successful <code>commit</code>
     * operations. When this count reaches the <i>pessimistic retry
     * threshold</i>, Persistit switches to pessimistic mode on the next attempt
     * to execute a transaction in this context. See <a
     * href="#_pessimisticMode">pessimistic scheduling mode</a> for futher
     * information.
     * 
     * @return The count
     */
    public int getRolledBackSinceLastCommitCount() {
        return _rollbacksSinceLastCommit;
    }

    /**
     * Specify the number of times a transaction will be retried before
     * switching to pessimistic mode. See <a
     * href="#_pessimisticMode">pessimistic scheduling mode</a> for futher
     * information.
     * 
     * @param count
     */
    public void setPessimisticRetryThreshold(int count) {
        if (count < 0)
            throw new IllegalArgumentException("Count must be >= 0");
        _pessimisticRetryThreshold = count;
    }

    /**
     * Returns the threshold count for pessimistic scheduling. See <a
     * href="#_pessimisticMode">pessimistic scheduling mode</a> for futher
     * information.
     * 
     * @return the count
     */
    public int getPessimisticRetryThreshold() {
        return _pessimisticRetryThreshold;
    }

    /**
     * Returns the most recent occurrence of a <code>RollbackException</code>
     * within this transaction context. This method can be used to detect and
     * diagnose implicit rollback from the {@link #end} method.
     * 
     * 
     * @return The <code>RollbackException</code>, if <code>end</code> generated
     *         an implicit rollback due to a missing call to <code>commit</code>
     *         , or <code>null</code> if the transaction committed and ended
     *         normally.
     */
    public RollbackException getRollbackException() {

        return _rollbackException;
    }

    public int getTransactionBufferCapacity() {
        return _txnBuffer.capacity();
    }

    public void setTransactionBufferCapacity(final int capacity) {
        if (capacity < MIN_TXN_BUFFER_CAPACITY || capacity > MAX_TXN_BUFFER_CAPACITY) {
            throw new IllegalArgumentException("Invalid request capacity: " + capacity);
        }
        if (capacity != _txnBuffer.capacity()) {
            _txnBuffer.allocate(capacity);
        }
    }

    void touchedPage(Exchange exchange, Buffer buffer) throws PersistitException {
        int hashCode = buffer.getVolume().hashCode() ^ ((int) buffer.getPageAddress());
        TouchedPage entry = (TouchedPage) _touchedPagesSet.lookup(hashCode);
        while (entry != null) {
            if (entry._volume == buffer.getVolume() && entry._pageAddr == buffer.getPageAddress()) {
                if (entry._timestamp != buffer.getTimestamp()) {
                    // can't actually roll back here because the context is
                    // wrong.
                    _rollbackPending = true;
                    if (_rollbackException == null) {
                        // Capture the stack trace here.
                        _rollbackException = new RollbackException();
                    }
                }
                //
                // No need to put a redundant entry
                //
                return;
            }
            entry = (TouchedPage) entry.getNext();
        }
        entry = new TouchedPage(buffer);
        _touchedPagesSet.put(entry);
    }

    /**
     * Attempt to perform the actual work of commitment.
     * 
     * @return <code>true</code> if completed. If not completed, it is due
     *         either to a transient problem, such as failure to claim a volume
     *         within the timeout, or it is due to a rollback condition. Caller
     *         should check _rollbackPending flag.
     * 
     * @throws PersistitException
     */
    private boolean doCommit(final boolean toDisk) throws PersistitException {
        boolean committed = false;
        boolean enqueued = false;
        try {
            //
            // Attempt to copy the transaction into the TransactionWriter
            // buffer.
            //
            _txnBuffer.clear();
            boolean fastMode = writeUpdatesToTransactionWriter(_txnBuffer);
            if (!fastMode) {
                _txnBuffer.clear();
            }
            _txnBuffer.flip();

            //
            // Step 1 - Get exclusive commit claim throws PersistitException.
            // For Version 2.1, we do brain-dead, totally single-threaded
            // commits.
            //
            boolean exclusiveClaim = false;

            try {
                exclusiveClaim = _persistit.getTransactionResourceB().claim(true, COMMIT_CLAIM_TIMEOUT);

                if (!exclusiveClaim) {
                    throw new TimeoutException("Unable to commit transaction " + this);
                }

                //
                // Step 2
                // Verify that no touched page has changed. If any have
                // been changed then we need to roll back. Since
                // transactionResourveB has been claimed, no other thread
                // can further modify one of these pages.
                //
                TouchedPage tp = null;
                while ((tp = (TouchedPage) _touchedPagesSet.next(tp)) != null) {
                    BufferPool pool = tp._volume.getPool();
                    Buffer buffer = pool.get(tp._volume, tp._pageAddr, false, true);
                    boolean changed = buffer.getTimestamp() != tp._timestamp;
                    buffer.release();

                    if (changed) {
                        _rollbackPending = true;
                        if (_rollbackException == null) {
                            _rollbackException = new RollbackException();
                        }
                        return false;
                    }
                }

                _toDisk.set(toDisk);
                //
                // Step 3 - apply the updates to their B-Trees
                //
                if (_pendingStoreCount > 0 || _pendingRemoveCount > 0) {
                    enqueued = true;
                    if (fastMode) {
                        applyUpdatesFast();
                    } else {
                        applyUpdates();
                    }
                }
                _commitTimestamp.set(_persistit.getTimestampAllocator().updateTimestamp());

                //
                // To serialize the TransactionalCache's we run the save()
                // method of each one within the scope of a transaction that
                // artificially sets its start and commit timestamps to the
                // timestamp of the checkpoint itself. This guarantees that this
                // transaction will get executed first during recovery. This is
                // valid because the TransactionalCache version being written is
                // pinned at the checkpoint's timestamp.
                //
                if (_transactionalCacheCheckpoint != null) {
                    _startTimestamp.set(_transactionalCacheCheckpoint.getTimestamp() - 1);
                    _commitTimestamp.set(_transactionalCacheCheckpoint.getTimestamp());
                    _transactionalCacheCheckpoint = null;
                }

                committed = true;
                for (int index = _commitListeners.size(); --index >= 0;) {
                    try {
                        _commitListeners.get(index).committed();
                    } catch (RuntimeException e) {
                        _persistit.getLogBase().txnCommitException.log(e, this);
                    }
                }

                if (!_transactionCacheUpdates.isEmpty()) {
                    for (final TransactionalCache tc : _transactionCacheUpdates.keySet()) {
                        enqueued |= tc.commit(this);
                    }
                }

                // all done

            } finally {
                if (exclusiveClaim) {
                    _persistit.getTransactionResourceB().release();
                }
            }
            if (enqueued) {
                writeUpdatesToTransactionWriter(_persistit.getJournalManager());
            }
            return committed;
        } finally {
            _longRecordDeallocationList.clear();
            _touchedPagesSet.clear();
        }
    }

    private void prepareTxnExchange(Tree tree, Key key, char type) throws PersistitException {
        setupExchanges();
        int treeHandle = handleForTree(tree);
        _ex1.clear().append(treeHandle).append(type);
        _ex1.getKey().copyTo(_rootKey);
        int keySize = key.getEncodedSize();
        byte[] bytes = key.getEncodedBytes();
        Key ex1Key = _ex1.getKey();
        byte[] ex1Bytes = ex1Key.getEncodedBytes();

        if (keySize > Key.MAX_KEY_LENGTH - 32 || keySize + _rootKey.getEncodedSize() > Key.MAX_KEY_LENGTH) {
            throw new InvalidKeyException("Key too long for transaction " + keySize);
        }

        System.arraycopy(bytes, 0, ex1Bytes, _rootKey.getEncodedSize(), keySize);
        ex1Key.setEncodedSize(keySize + _rootKey.getEncodedSize());
        final Integer treeId = Integer.valueOf(treeHandle);
        if (!_visbilityOrder.contains(treeId)) {
            _visbilityOrder.add(treeId);
        }
    }

    private boolean sameTree() {
        return _ex1.getKey().compareKeyFragment(_rootKey, 0, _rootKey.getEncodedSize()) == 0;
    }

    private void shiftOut() {
        int rsize = _rootKey.getEncodedSize();
        Key key = _ex1.getKey();
        byte[] bytes = key.getEncodedBytes();
        int size = key.getEncodedSize() - rsize;
        System.arraycopy(bytes, rsize, bytes, 0, size);
        key.setEncodedSize(size);
    }

    private void clear() throws PersistitException {
        _visbilityOrder.clear();
        if (_pendingStoreCount > 0 || _pendingRemoveCount > 0) {
            setupExchanges();
            _ex1.removeAll();
            _pendingStoreCount = 0;
            _pendingRemoveCount = 0;
        }
    }

    private void checkState() throws PersistitException {
        if (_nestedDepth < 1 || _commitCompleted) {
            throw new IllegalStateException();
        }
        if (_rollbackPending)
            rollback();
    }

    /**
     * Tests whether a previously posted, but as yet uncommitted update, affects
     * the value to be returned by a fetch() operation on an Exchange. If so,
     * then this method modifies the Exchange's Value according to the
     * uncommitted update, and returns <code>true</code>. Otherwise, this method
     * return <code>false</code>.
     * 
     * @param exchange
     *            The <code>Exchange</code> on which a fetch() operation is
     *            being performed.
     * 
     * @param value
     *            The <code>Value</code> to receive stored state.
     * 
     * @return <code>TRUE</code> if the result is determined by a pending but
     *         uncommitted store operation, <code>FALSE</code> if the result is
     *         determined by a pending remove operation, or <code>null</code> if
     *         the pending operations do not influence the result.
     */
    Boolean fetch(Exchange exchange, Value value, int minimumBytes) throws PersistitException {
        checkState();

        if (_pendingRemoveCount == 0 && _pendingStoreCount == 0) {
            return null;
        }
        setupExchanges();
        Tree tree = exchange.getTree();
        Key key = exchange.getKey();
        if (_pendingStoreCount > 0) {
            //
            // First see if there is a pending store operation
            //
            prepareTxnExchange(tree, key, 'S');
            _ex1.fetch(minimumBytes);
            if (_ex1.getValue().isDefined()) {
                if (minimumBytes > 0) {
                    Value value1 = _ex1.getValue();
                    if (value1.getEncodedSize() >= Buffer.LONGREC_PREFIX_OFFSET
                            && (value1.getEncodedBytes()[0] & 0xFF) == NEUTERED_LONGREC) {
                        byte[] bytes = value1.getEncodedBytes();
                        bytes[0] = (byte) Buffer.LONGREC_TYPE;
                        exchange.fetchFixupForLongRecords(value1, minimumBytes);
                    }
                    value1.copyTo(value);
                }
                return Boolean.TRUE;
            }
        }

        if (_pendingRemoveCount > 0) {
            //
            // If not, see if there is a pending remove operation that
            // covers the fetch key. To do this we look for a remove
            // operation that covers the key being fetched.
            //
            prepareTxnExchange(tree, key, 'R');
            if (_ex1.traverse(Key.LTEQ, true) && sameTree()) {
                Key key1 = _ex1.getAuxiliaryKey1();
                Key key2 = _ex1.getAuxiliaryKey2();
                shiftOut();
                _ex1.getKey().copyTo(key1);
                Value txnValue = _ex1.getValue();
                txnValue.decodeAntiValue(_ex1);
                if (key.compareTo(key2) <= 0) {
                    if (minimumBytes > 0) {
                        value.clear();
                    }
                    return Boolean.FALSE;
                }
            }
        }

        return null;
    }

/**
     * Tests whether a candidate key value that exists in the live database is
     * influenced by pending update operations. There are two cases in which the
     * pending update influences the result: (a) there is a close key in a
     * pending store operation, or (b) the candidate key is subject to a pending
     * remove operation.
     * 
     * @param Tree
     *            The original Tree
     *@param originalKey The key value at the beginning of
     *            the {@link Exchange#traverse(com.persistit.Key.Direction, boolean, int)
     *            method.
     * @param candidateKey
     *            The candidate Key value.
     * @param direction
     *            Key.LT, Key.LTEQ, Key.GT or Key.GTEQ
     * @return <code>TRUE</code> if there is a pending store operation that modifies
     *         the result, <code>FALSE</code> if there is a pending remove operation
     *         that modifies the result, or <code>null</code> if the pending updates
     *         do not modify the result.
     * 
     * @throws PersistitException
     */
    Boolean traverse(Tree tree, Key originalKey, Key candidateKey, Key.Direction direction, boolean deep, int minBytes)
            throws PersistitException {
        checkState();

        if (_pendingRemoveCount == 0 && _pendingStoreCount == 0) {
            return null;
        }

        setupExchanges();
        if (_pendingStoreCount > 0) {
            Key candidateKey2 = _ex1.getKey();
            //
            // First see if there is a pending store operation
            //
            prepareTxnExchange(tree, originalKey, 'S');
            if (_ex1.traverse(direction, deep, minBytes) && sameTree()) {
                shiftOut();
                int comparison = candidateKey.compareTo(candidateKey2);

                boolean reverse = (direction == Key.LT) || (direction == Key.LTEQ);

                if (reverse && comparison <= 0 || !reverse && comparison >= 0) {
                    candidateKey2.copyTo(candidateKey);
                    return Boolean.TRUE;
                }
            }
        }

        if (_pendingRemoveCount > 0) {
            //
            // If not, see if there is a pending remove operation that
            // covers the fetch key. To do this we look for a remove
            // operation that covers the key being fetched.
            //
            prepareTxnExchange(tree, candidateKey, 'R');
            if (_ex1.traverse(Key.LTEQ, true) && sameTree()) {
                Key key1 = _ex1.getAuxiliaryKey1();
                Key key2 = _ex1.getAuxiliaryKey2();
                shiftOut();
                _ex1.getKey().copyTo(key1);
                Value txnValue = _ex1.getValue();
                txnValue.decodeAntiValue(_ex1);
                // TODO - skip to end of AntiValue
                if (candidateKey.compareTo(key2) <= 0) {
                    return Boolean.FALSE;
                }
            }
        }

        return null;
    }

    void fetchFromLastTraverse(Exchange exchange, int minimumBytes) throws PersistitException {
        if (_ex1.getValue().isDefined()) {
            if (minimumBytes > 0) {
                Value value1 = _ex1.getValue();
                if (value1.getEncodedSize() >= Buffer.LONGREC_PREFIX_OFFSET
                        && (value1.getEncodedBytes()[0] & 0xFF) == NEUTERED_LONGREC) {
                    byte[] bytes = value1.getEncodedBytes();
                    bytes[0] = (byte) Buffer.LONGREC_TYPE;
                    exchange.fetchFixupForLongRecords(value1, minimumBytes);
                }
                value1.copyTo(exchange.getValue());
            }
        }
    }

    /**
     * Record a store operation.
     * 
     * @param exchange
     * @param volume
     * @param tree
     * @param key
     * @param value
     * @throws PersistitException
     */
    void store(Exchange exchange, Key key, Value value) throws PersistitException {
        checkState();

        Tree tree = exchange.getTree();
        prepareTxnExchange(tree, key, 'S');

        boolean longRec = value.isLongRecordMode();

        if (longRec) {
            //
            // If the value represents a long record, then remember the
            // information necessary to deallocate the long record change in the
            // event the transaction gets rolled back.
            //
            Volume volume = tree.getVolume();
            long pageAddr = Buffer.decodeLongRecordDescriptorPointer(value.getEncodedBytes(), 0);
            //
            // Synchronously flush all the member pages of the long record to
            // the journal. This ensures that recovery will find them. These
            // pages aren't likely to change soon anyway, so flushing them
            // immediately does not impact performance.
            //
            exchange.writeLongRecordPagesToJournal();

            _longRecordDeallocationList.add(new DeallocationChain(volume, pageAddr, 0));

            value.getEncodedBytes()[0] = (byte) NEUTERED_LONGREC;

        }
        _ex1.storeInternal(_ex1.getKey(), value, 0, false, false);
        _pendingStoreCount++;
    }

    boolean removeTree(Exchange exchange) throws PersistitException {
        final boolean removed = exchange.hasChildren();
        prepareTxnExchange(exchange.getTree(), exchange.getKey(), 'D');
        _ex1.storeInternal(_ex1.getKey(), exchange.getValue(), 0, false, false);
        _pendingRemoveCount++;
        return removed;
    }

    boolean remove(Exchange exchange, Key key1, Key key2, boolean fetchFirst) throws PersistitException {
        checkState();
        //
        // Remove any applicable store operations
        //
        setupExchanges();
        Key spareKey1 = _ex1.getAuxiliaryKey1();
        Key spareKey2 = _ex1.getAuxiliaryKey2();
        Tree tree = exchange.getTree();
        //
        // First remove any posted but uncommitted store operations from the
        // log.
        //
        prepareTxnExchange(tree, key1, 'S');
        _ex1.getKey().copyTo(spareKey1);
        prepareTxnExchange(tree, key2, 'S');
        _ex1.getKey().copyTo(spareKey2);

        boolean result1 = _ex1.removeKeyRangeInternal(spareKey1, spareKey2, fetchFirst);

        if (result1 && fetchFirst) {
            _ex1.getAuxiliaryValue().copyTo(exchange.getAuxiliaryValue());
        }
        //
        // Determine whether any records will be removed by this operation.
        //
        _ex2.setTree(tree);
        key1.copyTo(_ex2.getKey());

        boolean result2 = _ex2.traverse(Key.GT, true);
        if (result2 && _ex2.getKey().compareTo(key2) > 0) {
            result2 = false;
        }
        //
        // No need to post a Remove entry in the log if there are known to be
        // no committed records to remove.
        //
        if (result2) {
            // This logic coalesces previous remove operations when possible.
            // This simplifies the work necessary to perform a correct
            // traversal across the uncommitted updates.
            //
            Value value = _ex1.getValue();
            if (_pendingRemoveCount > 0) {
                // If the left edge of this key removal range overlaps a
                // previously posted remove operation, then reset the left edge
                // of this range to the overlapping one.
                //
                prepareTxnExchange(tree, key1, 'R');
                if (_ex1.traverse(Key.LT, true) && sameTree()) {
                    shiftOut();
                    _ex1.getKey().copyTo(spareKey1);
                    value.decodeAntiValue(_ex1);
                    if (spareKey2.compareTo(key1) >= 0) {
                        spareKey1.copyTo(key1);
                    }
                }

                // If the right edge of this key removal range overlaps a
                // previously posted remove operation, then reset the right edge
                // of this range to the overlapping one.
                //
                prepareTxnExchange(tree, key2, 'R');
                if (_ex1.traverse(Key.LT, true) && sameTree()) {
                    shiftOut();
                    _ex1.getKey().copyTo(spareKey1);
                    value.decodeAntiValue(_ex1);
                    if (spareKey2.compareTo(key2) >= 0) {
                        spareKey2.copyTo(key2);
                    }
                }
                //
                // Remove any other remove operations that are spanned by this
                // one.
                //
                prepareTxnExchange(tree, key1, 'R');
                _ex1.getKey().copyTo(spareKey1);

                prepareTxnExchange(tree, key2, 'R');
                _ex1.getKey().copyTo(spareKey2);

                _ex1.removeKeyRangeInternal(spareKey1, spareKey2, false);
            }

            AntiValue.putAntiValue(value, key1, key2);
            //
            // Save the Remove operation as an AntiValue in the transaction
            // tree.
            //

            prepareTxnExchange(tree, key1, 'R');
            _ex1.storeInternal(_ex1.getKey(), value, 0, false, false);
            _pendingRemoveCount++;
        }
        //
        // result1 indicates whether uncommitted records were deleted.
        // result2 indicates whether committed records will be deleted.
        //
        return result1 || result2;
    }

    boolean writeUpdatesToTransactionWriterFast(TransactionWriter tw) throws PersistitException {

        final ByteBuffer bb = _txnBuffer._bb;

        bb.mark();
        final long startTimestamp = _startTimestamp.get();
        final long commitTimestamp = _commitTimestamp.get();

        while (bb.hasRemaining()) {
            final int recordSize = getLength(bb);
            final int type = getType(bb);

            switch (type) {

            case TS.TYPE:
                JournalRecord.putTimestamp(bb, startTimestamp);
                break;

            case TC.TYPE:
                JournalRecord.putTimestamp(bb, startTimestamp);
                TC.putCommitTimestamp(bb, commitTimestamp);
                break;

            case SR.TYPE: {
                JournalRecord.putTimestamp(bb, startTimestamp);
                break;
            }

            case DR.TYPE: {
                JournalRecord.putTimestamp(bb, startTimestamp);
                break;
            }

            case DT.TYPE:
                JournalRecord.putTimestamp(bb, startTimestamp);
                break;

            case CU.TYPE:
                JournalRecord.putTimestamp(bb, startTimestamp);
                break;

            default:
                break;
            }

            bb.position(bb.position() + recordSize);
        }
        bb.reset();

        _persistit.getJournalManager().writeTransactionBufferToJournal(bb, startTimestamp, commitTimestamp);
        return true;

    }

    boolean writeUpdatesToTransactionWriter(TransactionWriter tw) throws PersistitException {

        if (tw != _txnBuffer && _txnBuffer._bb.hasRemaining()) {
            return writeUpdatesToTransactionWriterFast(tw);
        }

        setupExchanges();

        _ex1.clear();
        Value txnValue = _ex1.getValue();

        if (!tw.writeTransactionStartToJournal(_startTimestamp.get())) {
            return false;
        }

        for (Integer treeId : _visbilityOrder) {
            _ex1.clear().append(treeId.intValue());
            while (_ex1.traverse(Key.GT, true)) {
                Key key1 = _ex1.getKey();
                key1.reset();
                int treeHandle = key1.decodeInt();
                if (treeHandle != treeId.intValue()) {
                    break;
                }
                char type = key1.decodeChar();
                if (type != 'R' && type != 'S' && type != 'D')
                    continue;

                int offset = key1.getIndex();
                int size = key1.getEncodedSize() - offset;

                Key key2 = _ex2.getKey();
                System.arraycopy(key1.getEncodedBytes(), offset, key2.getEncodedBytes(), 0, size);
                key2.setEncodedSize(size);

                if (type == 'R') {
                    key2.copyTo(_ex2.getAuxiliaryKey1());
                    txnValue.decodeAntiValue(_ex2);

                    if (!tw.writeDeleteRecordToJournal(_startTimestamp.get(), treeHandle, _ex2.getAuxiliaryKey1(), _ex2
                            .getAuxiliaryKey2())) {
                        return false;
                    }
                } else if (type == 'S') {
                    if (txnValue.isDefined() && txnValue.getEncodedSize() >= Buffer.LONGREC_SIZE
                            && (txnValue.getEncodedBytes()[0] & 0xFF) == NEUTERED_LONGREC) {
                        txnValue.getEncodedBytes()[0] = (byte) Buffer.LONGREC_TYPE;
                    }
                    if (!tw.writeStoreRecordToJournal(_startTimestamp.get(), treeHandle, key2, txnValue)) {
                        return false;
                    }
                } else if (type == 'D') {
                    if (!tw.writeDeleteTreeToJournal(_startTimestamp.get(), treeHandle)) {
                        return false;
                    }
                }
            }
        }
        if (!_transactionCacheUpdates.isEmpty()) {
            for (final Map.Entry<TransactionalCache, List<Update>> entry : _transactionCacheUpdates.entrySet()) {
                if (_transactionalCacheCheckpoint == null && entry.getValue().isEmpty()) {
                    continue;
                }
                if (!tw.writeCacheUpdatesToJournal(_startTimestamp.get(), entry.getKey().cacheId(), entry.getValue())) {
                    return false;
                }
            }
        }
        if (!tw.writeTransactionCommitToJournal(_startTimestamp.get(), _commitTimestamp.get())) {
            return false;
        }
        return true;
    }

    private void applyUpdatesFast() throws PersistitException {
        setupExchanges();
        final Set<Tree> removedTrees = new HashSet<Tree>();
        final ByteBuffer bb = _txnBuffer._bb;
        bb.mark();

        while (bb.hasRemaining()) {
            final int recordSize = getLength(bb);
            final int type = getType(bb);

            switch (type) {

            case TS.TYPE:
                break;

            case TC.TYPE:
                break;

            case SR.TYPE: {
                final int keySize = SR.getKeySize(bb);
                final int treeHandle = SR.getTreeHandle(bb);
                _ex2.setTree(treeForHandle(treeHandle));
                final Key key = _ex2.getKey();
                final Value value = _ex2.getValue();
                System.arraycopy(bb.array(), bb.position() + SR.OVERHEAD, key.getEncodedBytes(), 0, keySize);
                key.setEncodedSize(keySize);
                final int valueSize = recordSize - SR.OVERHEAD - keySize;
                value.ensureFit(valueSize);
                System.arraycopy(bb.array(), bb.position() + SR.OVERHEAD + keySize, value.getEncodedBytes(), 0,
                        valueSize);
                value.setEncodedSize(valueSize);
                _ex2.storeInternal(key, value, 0, false, false);
                removedTrees.remove(_ex2.getTree());
                break;
            }

            case DR.TYPE: {
                final int key1Size = DR.getKey1Size(bb);
                final int treeHandle = DR.getTreeHandle(bb);
                _ex2.setTree(treeForHandle(treeHandle));
                if (removedTrees.contains(_ex2.getTree())) {
                    break;
                }
                final Key key1 = _ex2.getAuxiliaryKey1();
                final Key key2 = _ex2.getAuxiliaryKey2();
                System.arraycopy(bb.array(), bb.position() + DR.OVERHEAD, key1.getEncodedBytes(), 0, key1Size);
                key1.setEncodedSize(key1Size);
                final int key2Size = recordSize - DR.OVERHEAD - key1Size;
                System.arraycopy(bb.array(), bb.position() + DR.OVERHEAD + key1Size, key2.getEncodedBytes(), 0,
                        key2Size);
                key2.setEncodedSize(key2Size);
                _ex2.removeKeyRangeInternal(_ex2.getAuxiliaryKey1(), _ex2.getAuxiliaryKey2(), false);
                break;
            }

            case DT.TYPE:
                final int treeHandle = DT.getTreeHandle(bb);
                final Tree tree = treeForHandle(treeHandle);
                removedTrees.add(tree);
                break;

            case CU.TYPE:
                break;

            default:
                break;
            }

            bb.position(bb.position() + recordSize);
        }
        bb.reset();
        removeTrees(removedTrees);
    }

    private void applyUpdates() throws PersistitException {
        setupExchanges();
        Value txnValue = _ex1.getValue();

        final Set<Tree> removedTrees = new HashSet<Tree>();

        for (Integer treeId : _visbilityOrder) {
            _ex1.clear().append(treeId.intValue());
            while (_ex1.traverse(Key.GT, true)) {
                Key key1 = _ex1.getKey();
                key1.reset();
                int treeHandle = key1.decodeInt();
                if (treeHandle != treeId.intValue()) {
                    break;
                }
                char type = key1.decodeChar();
                if (type != 'R' && type != 'S' && type != 'D')
                    continue;

                _ex2.setTree(treeForHandle(treeHandle));
                int offset = key1.getIndex();
                int size = key1.getEncodedSize() - offset;

                Key key2 = _ex2.getKey();
                System.arraycopy(key1.getEncodedBytes(), offset, key2.getEncodedBytes(), 0, size);
                key2.setEncodedSize(size);

                if (type == 'R') {
                    key2.copyTo(_ex2.getAuxiliaryKey1());
                    txnValue.decodeAntiValue(_ex2);

                    _ex2.removeKeyRangeInternal(_ex2.getAuxiliaryKey1(), _ex2.getAuxiliaryKey2(), false);
                } else if (type == 'S') {
                    if (txnValue.isDefined() && txnValue.getEncodedSize() >= Buffer.LONGREC_SIZE
                            && (txnValue.getEncodedBytes()[0] & 0xFF) == NEUTERED_LONGREC) {
                        txnValue.getEncodedBytes()[0] = (byte) Buffer.LONGREC_TYPE;
                    }
                    _ex2.storeInternal(key2, txnValue, 0, false, false);
                    removedTrees.remove(_ex2.getTree());
                } else if (type == 'D') {
                    removedTrees.add(_ex2.getTree());
                }
            }
        }
        removeTrees(removedTrees);
    }

    private void removeTrees(final Set<Tree> removedTrees) throws PersistitException {
        for (final Iterator<WeakReference<Tree>> iterator = _treeCache.values().iterator(); iterator.hasNext();) {
            final WeakReference<Tree> ref = iterator.next();
            final Tree tree = ref.get();
            if (tree != null && removedTrees.contains(tree)) {
                iterator.remove();
            }
        }
        for (final Tree tree : removedTrees) {
            tree.getVolume().getStructure().removeTree(tree);
        }
    }

    void rollbackUpdates() throws PersistitException {
        setupExchanges();
        _touchedPagesSet.clear();
        harvestLongRecords();

        if (_pendingStoreCount > 0 || _pendingRemoveCount > 0) {
            //
            // Remove the update list.
            //
        } else if (!_recoveryMode) {
            return;
        }

        //
        // Deallocate all long record chains.
        //
        if (_longRecordDeallocationList.size() > 0) {
            for (int index = _longRecordDeallocationList.size(); --index >= 0;) {
                DeallocationChain dc = _longRecordDeallocationList.get(index);

                if (Debug.ENABLED) {
                    Volume volume = dc._volume;
                    long pageAddr = dc._leftPage;
                    Buffer buffer = volume.getPool().get(volume, pageAddr, false, true);
                    Debug.$assert0.t(buffer.isLongRecordPage());
                    buffer.releaseTouched();
                }

                dc._volume.getStructure().deallocateGarbageChain(dc._leftPage, dc._rightPage);

                _longRecordDeallocationList.remove(index);
            }
        }
    }

    private void harvestLongRecords() throws PersistitException {
        int currentTreeHandle = -1;
        Tree currentTree = null;
        Value txnValue = _ex1.getValue();
        _ex1.clear().append('S');
        while (_ex1.traverse(Key.GT, true)) {
            Key key1 = _ex1.getKey();
            key1.reset();
            if (key1.decodeType() != Character.class || key1.decodeChar() != 'S') {
                break;
            }

            if (key1.decodeType() != Integer.class || key1.decodeInt() != _id) {
                break;
            }
            int treeHandle = key1.decodeInt();
            if (treeHandle != currentTreeHandle || currentTree == null) {
                currentTree = treeForHandle(treeHandle);
                currentTreeHandle = treeHandle;
            }
            if (txnValue.getEncodedSize() >= Buffer.LONGREC_PREFIX_OFFSET
                    && (txnValue.getEncodedBytes()[0] & 0xFF) == NEUTERED_LONGREC) {
                byte[] bytes = txnValue.getEncodedBytes();
                bytes[0] = (byte) Buffer.LONGREC_TYPE;
                Volume volume = currentTree.getVolume();
                long pageAddress = Buffer.decodeLongRecordDescriptorPointer(bytes, 0);

                Debug.$assert0.t(pageAddress > 0 && pageAddress < Buffer.MAX_VALID_PAGE_ADDR);

                if (Debug.ENABLED) {
                    Buffer buffer = volume.getPool().get(volume, pageAddress, false, true);
                    Debug.$assert0.t(buffer.isLongRecordPage());
                    buffer.release();
                }

                _longRecordDeallocationList.add(new DeallocationChain(volume, pageAddress, 0));
            }
        }
    }

    /**
     * Indicates whether this Transaction must be made durable to disk before
     * the commit() method returns. This value is valid only during commit
     * processing and is used by the JournalFlusher.
     * 
     * @return
     */
    boolean isToDisk() {
        return _toDisk.get();
    }

    /**
     * @return The current timestamp value.
     */
    public long getTimestamp() {
        return _persistit.getCurrentTimestamp();
    }

    void setTransactionalCacheCheckpoint(final Checkpoint checkpoint) {
        _transactionalCacheCheckpoint = checkpoint;
    }

    List<Update> updateList(final TransactionalCache tc) {
        List<Update> list = _transactionCacheUpdates.get(tc);
        if (list == null) {
            list = new ArrayList<Update>();
            _transactionCacheUpdates.put(tc, list);
        }
        return list;
    }
}
