/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 */
package com.persistit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.persistit.exception.InvalidKeyException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.persistit.exception.TreeNotFoundException;
import com.persistit.exception.VolumeNotFoundException;

/**
 * <p>
 * Represents the transaction context for atomic units of work performed by
 * Persistit. The application determines when to {@link #begin}, {@link #commit}, {@link #rollback} and {@link #end} transactions. Once a transaction has
 * started, no update operation performed within its context will actually be
 * written to the database until <tt>commit</tt> is performed. At that point,
 * all the updates are written atomically - that is, completely or not at all.
 * If the application is abruptly terminated while <tt>commit</tt> is writing
 * updates to the database, they will be completed during recovery processing
 * when the Persistit is next initialized.
 * </p>
 * <p>
 * All Persistit database operations are performed in the context of a
 * <tt>Transaction</tt>. If the <tt>begin</tt> operation has not explicitly been
 * called then Persistit behaves as if each database update operation is
 * implicitly preceeded by invocation of <tt>begin</tt> and concluded by
 * invocation of <tt>commit</tt> and <tt>end</tt> to perform a memory commit.
 * </p>
 * <h2>Lexical Scoping</h2>
 * <p>
 * Applications using Persistit transactions must terminate the scope of any
 * transaction. To do this, applications must ensure that whenever the
 * <tt>begin</tt> method is called, there is a concluding invocation of the
 * <tt>end</tt> method. The <tt>commit</tt> and <tt>rollback</tt> methods do not
 * end the scope of a transaction; they merely signify the transaction's outcome
 * when <tt>end</tt> is invoked. Applications should follow either the <a
 * href="#_pattern1"><tt>try/finally</tt></a> or the <a href="#_pattern2">
 * <tt>TransactionRunnable</tt></a> pattern to ensure correctness.
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
 * <tt>RollbackException</tt>. In most database applications, such a collision
 * is relatively rare, and the application can simply retry the transaction with
 * a high likelihood of success after a small number of retries. To minimize the
 * likelihood of collisions, applications should favor short transactions with
 * small numbers of database operations when possible and practical.
 * </p>
 * <a name="_pattern1" /> <h3>The try/finally Pattern</h3>
 * <p>
 * The following code fragment illustrates a transaction executed with up to to
 * RETRY_COUNT retries. If the <tt>commit</tt> method succeeds, the whole
 * transaction is completed and the retry loop terminates. If after RETRY_COUNT
 * retries, <tt>commit</tt> has not been successfully completed, the application
 * throws a <tt>TransactionFailedException</tt>.
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
 * fragment shown above is handled automatically by <tt>run</tt>; it could be
 * rewritten as follows: <blockquote><code><pre>
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
 * <tt>commit</tt> operation reaches this threshold, Persistit switches to
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
 * {@link #begin} and {@link #end} operations. Each invocation of <tt>begin</tt>
 * increments this count and each invocation of <tt>end</tt> decrements the
 * count. These methods are intended to be used in a standard essential pattern,
 * shown here, to ensure that the scope of of the transaction is reliably
 * determined by the lexical the structure of the code rather than conditional
 * logic: <blockquote>
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
 * current nested level count is 1. That is, if <tt>begin</tt> has been invoked
 * N times, then <tt>commit</tt> will actually commit the data only if
 * <tt>end</tt> has been invoked N-1 times. Thus data updated by an inner
 * (nested) transaction is never actually committed until the outermost
 * <tt>commit</tt> is called. This permits transactional code to invoke other
 * code (possibly an opaque library supplied by a third party) that may itself
 * <tt>begin</tt> and <tt>commit</tt> transactions.
 * </p>
 * <p>
 * Invoking {@link #rollback} removes all pending but uncomitted updates, marks
 * the current transaction scope as <i>rollback pending</i> and throws a
 * <tt>RollbackException</tt>. Any subsequent attempt to perform any Persistit
 * operation, including <tt>commit</tt> in the current transaction scope, will
 * fail with a <tt>RollbackException</tt>. The <tt>commit</tt> method throws a
 * <tt>RollbackException</tt> (and therefore does not commit the pending
 * updates) if either the transaction scope is marked <i>rollback pending</i> by
 * a prior call to <tt>rollback</tt> or if the attempt to commit the updates
 * would generate an inconsistent database state.
 * </p>
 * <p>
 * Application developers should beware that the <tt>end</tt> method performs an
 * implicit rollback if <tt>commit</tt> has not completed. If an application
 * fails to call <tt>commit</tt>, the transaction will silently fail. The
 * <tt>end</tt> method sends a warning message to the log subsystem when this
 * happens, but does not throw an exception. The <tt>end</tt> method is designed
 * this way to allow an exception thrown within the application code to be
 * caught and handled without being obscured by a RollbackException thrown by
 * <tt>end</tt>. But as a consequence, developers must carefully verify that the
 * <tt>commit</tt> method is always invoked when the transaction completes
 * normally. Upon completion of the <tt>end</tt> method, an application can
 * query whether a rollback occurred with the {@link #getRollbackException()}
 * method. This method returns <tt>null</tt> if the transaction committed and
 * ended normal; otherwise it contains a
 * {@link com.persistit.exception.RollbackException} whose stack trace indicates
 * the location of the implicit rollback.
 * 
 * @author pbeaman
 * @version 1.1
 */
public class Transaction {
    public final static int DEFAULT_PESSIMISTIC_RETRY_THRESHOLD = 3;

    private final static CommitListener DEFAULT_COMMIT_LISTENER = new CommitListener();

    final static Transaction NEVER_ACTIVE_TRANSACTION = new Transaction();

    private final static String TRANSACTION_TREE_NAME = "_txn_";

    private final static long COMMIT_CLAIM_TIMEOUT = 30000;

    private final static int COMMIT_RETRY_COUNT = 10;

    private final static int NEUTERED_LONGREC = 254;

    private static long _idCounter = 100000000;

    private final Persistit _persistit;
    private final long _id;
    private long _timestamp;
    private int _nestedDepth;
    private int _pendingStoreCount = 0;
    private int _pendingRemoveCount = 0;
    private boolean _rollbackPending;
    private boolean _commitCompleted;
    private RollbackException _rollbackException;

    private boolean _recoveryMode;

    private long _rollbackCount = 0;
    private long _commitCount = 0;
    private int _rollbacksSinceLastCommit = 0;

    private int _pessimisticRetryThreshold = DEFAULT_PESSIMISTIC_RETRY_THRESHOLD;

    private ArrayList<DeallocationChain> _longRecordDeallocationList = new ArrayList<DeallocationChain>();

    private Exchange _ex1;
    private Exchange _ex2;

    private final Key _rootKey;

    private final InternalHashSet _touchedPagesSet = new InternalHashSet();
    private final HashMap<Tree, Integer> _treeHandlesByName = new HashMap<Tree, Integer>();
    private final HashMap<Integer, Tree> _treesByHandle = new HashMap<Integer, Tree>();

    private long _expirationTime;
    private long _timeout;

    private long _rollbackDelay = 50;

    private CommitListener _commitListener = DEFAULT_COMMIT_LISTENER;

    private byte[] _stuff = new byte[128];

    /**
     * Call-back for commit() processing. Methods of this class are called
     * during execution of the {@link Transaction#commit()} method.
     * Implementations of this interface must return quickly without blocking
     * for synchronization of physical I/O.
     * <p />
     * The default implementation of this class does nothing.
     */
    public static class CommitListener {
        /**
         * Called when a transaction is committed. The implementation may
         * perform actions consistent with the committed transaction.
         */
        public void committed() {
            // default implementation: do nothing
        }

        /**
         * Called when a transaction is rolled back.
         */
        public void rolledBack() {
            // default implementation: do nothing
        }
    }

    private static class TouchedPage extends InternalHashSet.Entry {
        Volume _volume;
        long _pageAddr;
        long _changeCount;
        int _bufferIndex;

        TouchedPage(Buffer buffer) {
            _volume = buffer.getVolume();
            _pageAddr = buffer.getPageAddress();
            _changeCount = buffer.getTimestamp();
            _bufferIndex = buffer.getIndex();
        }

        @Override
        public int hashCode() {
            return ((int) _volume.getId()) ^ ((int) _pageAddr);
        }

        @Override
        public String toString() {
            return "Touched(" + _volume.getPath() + ", page " + _pageAddr
                    + ", changeCount=" + _changeCount + ")";
        }
    }

    private static class DeallocationChain {
        Volume _volume;
        int _treeIndex;
        long _leftPage;
        long _rightPage;

        DeallocationChain(Volume volume, int treeIndex, long leftPage,
                long rightPage) {
            _volume = volume;
            _treeIndex = treeIndex;
            _leftPage = leftPage;
            _rightPage = rightPage;
        }

        @Override
        public int hashCode() {
            return (int) _leftPage ^ (int) _rightPage ^ (int) _volume.getId();
        }

        @Override
        public boolean equals(Object o) {
            DeallocationChain dc = (DeallocationChain) o;
            return (dc._leftPage == _leftPage && dc._rightPage == _rightPage && dc._volume == _volume);
        }
    }

    /**
     * Creates a new transaction context. Any transaction performed within this
     * context will be isolated from all other transactions.
     */
    public Transaction(final Persistit persistit) {
        this(persistit, nextId());
    }

    /**
     * Creates a Transaction context with a specified ID value. This is used
     * only during recovery processing.
     * 
     * @param id
     */
    private Transaction(final Persistit persistit, final long id) {
        _persistit = persistit;

        _id = id;
        _rollbackDelay = persistit.getLongProperty("rollbackDelay", 10, 0,
                100000);
        _rootKey = new Key(_persistit);
        _timeout = _persistit.getDefaultTimeout();
    }

    private Transaction() {
        _id = -1;
        _persistit = null;
        _rootKey = null;
    }

    private static synchronized long nextId() {
        return ++_idCounter;
    }

    void setupExchanges() throws PersistitException {
        int saveDepth = _nestedDepth;
        _nestedDepth = 0;
        try {
            Volume txnVolume = _persistit.getTransactionVolume();

            _ex1 = _persistit.getExchange(txnVolume, TRANSACTION_TREE_NAME
                    + _id, true);
            _ex2 = new Exchange(_ex1);
            _ex1.ignoreTransactions();
            _ex2.ignoreTransactions();
            _ex1.removeAll();
        } finally {
            _nestedDepth = saveDepth;
        }
    }

    private synchronized int handleForTree(Tree tree) throws PersistitException {
        Integer v = _treeHandlesByName.get(tree);
        if (v != null)
            return v.intValue();

        int handle;
        if (_ex1 == null)
            setupExchanges();
        final Exchange ex = new Exchange(_ex1);
        ex.ignoreTransactions();

        ex.clear().append('@').append(tree.getName()).append(
                tree.getVolume().getId());
        ex.fetch();
        if (ex.getValue().isDefined()) {
            handle = ex.getValue().getInt();
        } else {
            Value value = ex.getValue();
            Volume volume = tree.getVolume();
            ex.clear().append('@');
            handle = (int) ex.incrementValue();
            ex.append(tree.getName()).append(tree.getVolume().getId());
            value.put(handle);
            ex.store();
            value.clear();
            value.setStreamMode(true);
            try {
                value.put(volume.getId());
                value.put(volume.getPath());
                value.put(volume.getName());
                value.put(tree.getName());
                ex.clear().append('@').append(handle).store();
            } finally {
                value.setStreamMode(false);
            }
        }
        Integer handleObject = new Integer(handle);

        _treeHandlesByName.put(tree, handleObject);
        _treesByHandle.put(handleObject, tree);

        return handle;
    }

    private synchronized Tree treeForHandle(int handle)
            throws PersistitException {
        Integer handleObject = new Integer(handle);
        Tree tree = _treesByHandle.get(handleObject);

        if (tree == null) {
            if (_ex1 == null)
                setupExchanges();
            final Exchange ex = new Exchange(_ex1);

            ex.clear().append('@').append(handle).fetch();
            Value value = ex.getValue();

            if (value.isDefined()) {
                String volumePathName;
                String volumeAlias;
                String treeName;

                value.setStreamMode(true);
                try {
                    /* long volumeId = */value.getLong();
                    volumePathName = value.getString();
                    volumeAlias = value.getString();
                    treeName = value.getString();
                } finally {
                    value.setStreamMode(false);
                }
                Volume volume = _persistit.getVolume(volumePathName);
                if (volume == null && volumeAlias != null) {
                    volume = _persistit.getVolume(volumeAlias);
                }
                if (volume == null) {
                    throw new VolumeNotFoundException(volumePathName);
                }
                tree = volume.getTree(treeName, false);
                if (tree == null) {
                    throw new TreeNotFoundException(treeName);
                }
                _treeHandlesByName.remove(tree);
                _treesByHandle.remove(handleObject);

                _treeHandlesByName.put(tree, handleObject);
                _treesByHandle.put(handleObject, tree);

            }
        }
        if (!tree.isValid() || !tree.isInitialized()) {
            final Tree newTree = tree.getVolume()
                    .getTree(tree.getName(), false);
            _treeHandlesByName.remove(tree);
            _treesByHandle.remove(handleObject);
            tree = newTree;
            _treeHandlesByName.put(tree, handleObject);
            _treesByHandle.put(handleObject, tree);
        }
        return tree;
    }

    private synchronized void removeTreeHandle(final Tree tree)
            throws PersistitException {
        int handle;
        if (_ex1 == null)
            setupExchanges();

        final Exchange ex = new Exchange(_ex1);
        ex.ignoreTransactions();

        ex.clear().append('@').append(tree.getName()).append(
                tree.getVolume().getId()).fetch();
        if (ex.getValue().isDefined()) {
            handle = ex.getValue().getInt();
            ex.remove();
            ex.clear().append('@').append(handle).remove();
            Integer handleObject = new Integer(handle);

            _treeHandlesByName.remove(tree);
            _treesByHandle.remove(handleObject);
        }
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
     * @return <tt>true</tt> if a transaction has begun, but is not yet either
     *         committed or rolled back
     */
    public boolean isActive() {
        return _nestedDepth > 0;
    }

    /**
     * Indicates whether the {@link #commit} method has run to successful
     * completion at the current nested level. If that level is 1, then
     * successful completion of <tt>commit</tt> means that the transaction has
     * actually been committed to the database. Within the scope of inner
     * (nested) transactions, data is not actually committed until the outermost
     * transaction scope commits.
     * 
     * @return <tt>true</tt> if the current transaction scope has completed its
     *         <tt>commit</tt> operation; otherwise <tt>false</tt>.
     */
    public boolean isCommitted() {
        return _commitCompleted;
    }

    /**
     * Indicates whether the current transaction scope has been marked for
     * rollback. If so, and if this is an inner (nested) transaction scope, all
     * updates performed by the containing scope will also be rolled back.
     * 
     * @return <tt>true</tt> if the current transaction is marked for rollback;
     *         otherwise <tt>false</tt>.
     */
    public boolean isRollbackPending() {
        return _rollbackPending;
    }

    /**
     * Start a transaction. If there already is an active transaction then this
     * method merely increments a counter that indicates how many times
     * <tt>begin</tt> has been called. Application code should ensure that every
     * method that calls <tt>begin</tt> also invokes <tt>end</tt> using a
     * <tt>try/finally</tt> pattern described <a
     * href="#_scopedCodePattern">above</a></tt>.
     * 
     * @throws IllegalStateException
     *             if the current transaction scope has already been committed.
     */
    public void begin() {
        if (this == NEVER_ACTIVE_TRANSACTION) {
            throw new IllegalStateException(
                    "Attempt to begin NEVER_ACTIVE_TRANSACTION");
        }

        if (_commitCompleted) {
            throw new IllegalStateException(
                    "Attempt to begin a committed transaction");
        }

        _rollbackPending = false;
        _rollbackException = null;

        if (_nestedDepth == 0) {
            if (_pendingStoreCount != 0 || _pendingRemoveCount != 0) {
                try {
                    clear();
                } catch (PersistitException pe) {
                    if (_persistit.getLogBase().isLoggable(
                            LogBase.LOG_TXN_EXCEPTION)) {
                        _persistit.getLogBase().log(LogBase.LOG_TXN_EXCEPTION,
                                pe, this);
                    }
                }
            }
            _persistit.getTransactionResourceA().claim(
                    _rollbacksSinceLastCommit >= _pessimisticRetryThreshold,
                    COMMIT_CLAIM_TIMEOUT);
            assignTimestamp();
        }
        _nestedDepth++;
    }

    /**
     * <p>
     * Ends the current transaction scope. Application code should ensure that
     * every method that calls <tt>begin</tt> also invokes <tt>end</tt> using a
     * <tt>try/finally</tt> pattern described <a
     * href="#_scopedCodePattern">above</a></tt>.
     * </p>
     * <p>
     * This method implicitly rolls back any pending, uncommitted updates.
     * Updates are committed only if the <tt>commit</tt> method completes
     * successfully.
     * </p>
     * 
     * @throws IllegalStateException
     *             if there is no current transaction scope.
     */
    public void end() {

        if (_nestedDepth < 1) {
            throw new IllegalStateException(
                    "No transaction scope: begin() not called");
        }
        _nestedDepth--;

        // Decide whether this is a rollback or a commit.
        if (!_commitCompleted || _rollbackPending) {
            if (_rollbackException == null) {
                _rollbackException = new RollbackException();
            }
            if (!_rollbackPending
                    && _persistit.getLogBase().isLoggable(
                            LogBase.LOG_TXN_NOT_COMMITTED)) {
                _persistit.getLogBase().log(LogBase.LOG_TXN_NOT_COMMITTED,
                        _rollbackException);
                _rollbackPending = true;
            }
        }

        // Special handling for the outermost scope.
        if (_nestedDepth == 0) {
            // First release the pessimistic lock if we claimed it.
            if (Debug.ENABLED && _rollbackPending) {
                Debug.debug1(_rollbacksSinceLastCommit
                        - _pessimisticRetryThreshold > 20);
            }
            _persistit.getTransactionResourceA().release();
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
                    if (_persistit.getLogBase().isLoggable(
                            LogBase.LOG_TXN_EXCEPTION)) {
                        _persistit.getLogBase().log(LogBase.LOG_TXN_EXCEPTION,
                                pe, this);
                    }
                } catch (InterruptedException ie) {
                }
            } else {
                _commitCount++;
                _rollbacksSinceLastCommit = 0;
            }
            _rollbackPending = false;
        }
        // PDB 20050808 - to be sure. Should have been cleared by either
        // commit() or rollbackUpdates().
        if (Debug.ENABLED) {
            Debug.$assert(_touchedPagesSet.size() == 0);
        }
        _touchedPagesSet.clear();
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
        if (this == NEVER_ACTIVE_TRANSACTION) {
            throw new IllegalStateException(
                    "Can't rollback NEVER_ACTIVE_TRANSACTION");
        }

        if (_commitCompleted) {
            throw new IllegalStateException("Already committed");
        }

        if (_nestedDepth < 1) {
            throw new IllegalStateException(
                    "No transaction scope: begin() not called");
        }

        _rollbackPending = true;
        if (_rollbackException == null) {
            _rollbackException = new RollbackException();
        }
        try {
            rollbackUpdates();
        } catch (PersistitException pe) {
            if (_persistit.getLogBase().isLoggable(LogBase.LOG_TXN_EXCEPTION)) {
                _persistit.getLogBase()
                        .log(LogBase.LOG_TXN_EXCEPTION, pe, this);
            }
        }
        _commitListener.rolledBack();
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
     * Once an application thread has called <tt>commit</tt>, no subsequent
     * Persistit database operations are permitted until the corresponding
     * <tt>end</tt> method has been called. An attempt to store, fetch or remove
     * data after <tt>commit</tt> has been called throws an
     * <tt>IllegalStateException</tt>.
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
     * Once an application thread has called <tt>commit</tt>, no subsequent
     * Persistit database operations are permitted until the corresponding
     * <tt>end</tt> method has been called. An attempt to store, fetch or remove
     * data after <tt>commit</tt> has been called throws an
     * <tt>IllegalStateException</tt>.
     * </p>
     * 
     * @param toDisk
     *            <tt>true</tt> to commit to disk, or <tt>false</tt> to commit
     *            to memory.
     * 
     * @throws PersistitException
     * 
     * @throws RollbackException
     * 
     * @throws IllegalStateException
     *             if no transaction scope is active or this transaction scope
     *             has already called <tt>commit</tt>.
     */
    public void commit(boolean toDisk) throws PersistitException,
            RollbackException {
        if (this == NEVER_ACTIVE_TRANSACTION) {
            throw new IllegalStateException(
                    "Can't commit NEVER_ACTIVE_TRANSACTION");
        }

        else if (_nestedDepth < 1) {
            throw new IllegalStateException(
                    "No transaction scope: begin() not called");
        }

        else if (_commitCompleted) {
            throw new IllegalStateException("Already committed");
        }

        try {
            if (_rollbackPending)
                throw _rollbackException;
            if (_nestedDepth == 1) {
                boolean done = false;
                for (int retries = 0; !done && retries < COMMIT_RETRY_COUNT; retries++) {
                    done = doCommit();
                    if (!done && _rollbackPending) {
                        rollback();
                    }
                }

                if (toDisk) {
                    _persistit.getLogManager().force();
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
     * Once an application thread has called <tt>commit</tt>, no subsequent
     * Persistit database operations are permitted until the corresponding
     * <tt>end</tt> method has been called. An attempt to store, fetch or remove
     * data after <tt>commit</tt> has been called throws an
     * <tt>IllegalStateException</tt>.
     * </p>
     * 
     * @param commitListener
     *            CommitListener instance whose methods are called when this
     *            Transaction is committed or rolled back
     * 
     * @param toDisk
     *            <tt>true</tt> to commit to disk, or <tt>false</tt> to commit
     *            to memory.
     * 
     * @throws PersistitException
     * 
     * @throws RollbackException
     * 
     * @throws IllegalStateException
     *             if no transaction scope is active or this transaction scope
     *             has already called <tt>commit</tt>.
     */
    public void commit(final CommitListener commitListener, final boolean toDisk)
            throws PersistitException, RollbackException {
        final CommitListener saveListener = _commitListener;
        _commitListener = commitListener;
        try {
            commit(toDisk);
        } finally {
            _commitListener = saveListener;
        }
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
     * transaction. The <tt>TransactionRunnable</tt> should neither
     * <tt>begin</tt> nor <tt>commit</tt> the transaction; these operations are
     * performed by this method. The <tt>TransactionRunnable</tt> <i>may</i>
     * explicitly throw a <tt>RollbackException</tt> in which case the current
     * transaction will be rolled back.
     * </p>
     * <p>
     * This method does not perform retries, so upon the first failed attempt to
     * commit the transaction this method will throw a
     * <tt>RollbackException</tt>. See
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
     * transaction. The <tt>TransactionRunnable</tt> should neither
     * <tt>begin</tt> nor <tt>commit</tt> the transaction; these operations are
     * performed by this method. The <tt>TransactionRunnable</tt> <i>may</i>
     * explicitly or implicitly throw a <tt>RollbackException</tt> in which case
     * the current transaction will be rolled back.
     * </p>
     * <p>
     * If <tt>retryCount</tt> is greater than zero, this method will make up to
     * <tt>retryCount</tt> additional of attempts to complete and commit
     * thetransaction. Once the retry count is exhausted, this method throws a
     * <tt>RollbackException</tt>.
     * </p>
     * 
     * @param runnable
     *            An application specific implementation of
     *            <tt>TransactionRunnable</tt> containing logic to access and
     *            update Persistit data.
     * 
     * @param retryCount
     *            Number of attempts (not including the first attempt) to make
     *            before throwing a <tt>RollbackException</tt>
     * 
     * @param retryDelay
     *            Time, in milliseconds, to wait before the next retry attempt.
     * 
     * @param toDisk
     *            <tt>true</tt> to commit the transaction to <a
     *            href="#diskVsMemoryCommit">disk</a>, or <tt>false</tt> to
     *            commit to <a href="#diskVsMemoryCommit">memory</a>
     * 
     * @return Count of attempts needed to complete the transaction
     * 
     * @throws PersistitException
     * @throws RollbackException
     *             If after <tt>retryCount+1</tt> attempts the transaction
     *             cannot be completed or committed due to concurrent updated
     *             performed by other threads.
     */
    public int run(TransactionRunnable runnable, int retryCount,
            long retryDelay, boolean toDisk) throws PersistitException {
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
        return "Transaction_" + _id + " depth=" + _nestedDepth
                + " _rollbackPending=" + _rollbackPending
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
     * Return the number of times a transaction in this <tt>Transaction</tt>
     * context has rolled back since the last successful <tt>commit</tt>
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
     * Returns the most recent occurrence of a <tt>RollbackException</tt> within
     * this transaction context. This method can be used to detect and diagnose
     * implicit rollback from the {@link #end} method.
     * 
     * @return The <tt>RollbackException</tt>, if <tt>end</tt> generated an
     *         implicit rollback due to a missing call to <tt>commit</tt>, or
     *         <tt>null</tt> if the transaction committed and ended normally.
     */
    public RollbackException getRollbackException() {
        return _rollbackException;
    }

    void touchedPage(Exchange exchange, Buffer buffer)
            throws PersistitException {
        int hashCode = ((int) buffer.getVolume().getId())
                ^ ((int) buffer.getPageAddress());
        TouchedPage entry = (TouchedPage) _touchedPagesSet.lookup(hashCode);
        while (entry != null) {
            if (entry._volume == buffer.getVolume()
                    && entry._pageAddr == buffer.getPageAddress()) {
                if (entry._changeCount != buffer.getTimestamp()) {
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
     * @return <tt>true</tt> if completed. If not completed, it is due either to
     *         a transient problem, such as failure to claim a volume within the
     *         timeout, or it is due to a rollback condition. Caller should
     *         check _rollbackPending flag.
     * 
     * @throws PersistitException
     */
    private boolean doCommit() throws PersistitException {
        boolean committed = false;
        _expirationTime = 0;
        try {
            for (;;) {
                //
                // Step 1 - get a Writer claim on every Volume we are going to
                // touch.
                // For Version 1.0, we do brain-dead, totally single-threaded
                // commits.
                //
                boolean exclusiveClaim = false;
                try {
                    exclusiveClaim = _persistit.getTransactionResourceB()
                            .claim(true, COMMIT_CLAIM_TIMEOUT);

                    if (!exclusiveClaim) {
                        // debugReportRollback(null, null);
                        return false;
                    }
                    //
                    // Step 2
                    // Verify that no touched page has changed. If any have
                    // been changed then we need to roll back. Since all the
                    // volumes
                    // we care about are now claimed, no other thread can
                    // further modify
                    // one of these pages.
                    //
                    TouchedPage tp = null;
                    while ((tp = (TouchedPage) _touchedPagesSet.next(tp)) != null) {
                        BufferPool pool = tp._volume.getPool();
                        Buffer buffer = pool.get(tp._volume, tp._pageAddr,
                                false, true);
                        boolean changed = buffer.getTimestamp() != tp._changeCount;
                        buffer.release(); // Do not make Least-Recently-Used

                        if (changed) {
                            _rollbackPending = true;
                            if (_rollbackException == null) {
                                _rollbackException = new RollbackException();
                            }
                            // debugReportRollback(buffer, tp); //TODO
                            return false;
                        }
                    }

                    //
                    // Step 3
                    // Mark the transaction as COMMIT_STARTED. This means that
                    // if
                    // the JVM is interrupted after this point, but prior to
                    // completion,
                    // RecoveryPlan will apply these updates.
                    //
                    if (_pendingStoreCount > 0 || _pendingRemoveCount > 0) {
                        _ex1.getValue().put(
                                _pendingStoreCount + _pendingRemoveCount);
                        _ex1.clear().append('C').store();
                        applyUpdates();
                        //
                        // Step 4
                        // Remove the pending updates. Don't need them any more!
                        //
                        clear();
                    }
                    _longRecordDeallocationList.clear();
                    // simulates writing a transaction to the log.
                    // _persistit.getLogManager().writeStuff(_stuff, 0,
                    // _stuff.length);
                    committed = true;
                    try {
                        _commitListener.committed();
                    } catch (RuntimeException e) {
                        // ignore
                    }
                    // all done
                    break;
                } finally {
                    if (exclusiveClaim) {
                        _persistit.getTransactionResourceB().release();
                    }
                }
            }
            return committed;
        } finally {
            //
            // Finally, release all the pages we claimed above.
            // PDB 20050808 - moved this outside of RetryExcepion loop because
            // touched pages needed to be rechecked after relinquishing and
            // then reclaiming exclusive lock.
            _touchedPagesSet.clear();
        }
    }

    private void prepareTxnExchange(Tree tree, Key key, char type)
            throws PersistitException {
        if (_ex1 == null)
            setupExchanges();
        int treeHandle = handleForTree(tree);
        _ex1.clear().append(type).append(treeHandle);
        _ex1.getKey().copyTo(_rootKey);
        int keySize = key.getEncodedSize();
        byte[] bytes = key.getEncodedBytes();
        Key ex1Key = _ex1.getKey();
        byte[] ex1Bytes = ex1Key.getEncodedBytes();

        if (keySize > Key.MAX_KEY_LENGTH - 32
                || keySize + _rootKey.getEncodedSize() > Key.MAX_KEY_LENGTH) {
            throw new InvalidKeyException("Key too long for transaction "
                    + keySize);
        }

        System
                .arraycopy(bytes, 0, ex1Bytes, _rootKey.getEncodedSize(),
                        keySize);

        ex1Key.setEncodedSize(keySize + _rootKey.getEncodedSize());
    }

    private boolean sameTree() {
        return _ex1.getKey().compareKeyFragment(_rootKey, 0,
                _rootKey.getEncodedSize()) == 0;
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
        if (_pendingStoreCount > 0 || _pendingRemoveCount > 0) {
            if (_ex1 == null)
                setupExchanges();

            Key key1 = _ex1.getAuxiliaryKey1();
            Key key2 = _ex1.getAuxiliaryKey2();
            //
            // Note: don't clear tree handles
            //
            _ex1.clear().append('A');
            _ex1.getKey().copyTo(key1);
            _ex1.clear().append('Z');
            _ex1.getKey().copyTo(key2);
            _ex1.removeKeyRangeInternal(key1, key2, false);
        }
        _pendingStoreCount = 0;
        _pendingRemoveCount = 0;
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
     * uncommitted update, and returns <tt>true</tt>. Otherwise, this method
     * return <tt>false</tt>.
     * 
     * @param exchange
     *            The <tt>Exchange</tt> on which a fetch() operation is being
     *            performed.
     * 
     * @param value
     *            The <tt>Value</tt> to receive stored state.
     * 
     * @return <tt>TRUE</tt> if the result is determined by a pending but
     *         uncommitted store operation, <tt>FALSE</tt> if the result is
     *         determined by a pending remove operation, or <tt>null</tt> if the
     *         pending operations do not influence the result.
     */
    Boolean fetch(Exchange exchange, Value value, int minimumBytes)
            throws PersistitException {
        checkState();

        if (_pendingRemoveCount == 0 && _pendingStoreCount == 0) {
            return null;
        }
        if (_ex1 == null)
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
     * @param exchange
     *            The original Exchange
     * @param candidateKey
     *            The candidate Key value.
     * @param direction
     *            Key.LT, Key.LTEQ, Key.GT or Key.GTEQ
     * @return <tt>TRUE</tt> if there is a pending store operation that modifies
     *         the result, <tt>FALSE</tt> if there is a pending remove operation
     *         that modifies the result, or <tt>null</tt> if the pending updates
     *         do not modify the result.
     * 
     * @throws PersistitException
     */
    Boolean traverse(Exchange exchange, Key candidateKey,
            Key.Direction direction, boolean deep, int minBytes)
            throws PersistitException {
        checkState();

        if (_pendingRemoveCount == 0 && _pendingStoreCount == 0) {
            return null;
        }

        if (_ex1 == null)
            setupExchanges();
        Tree tree = exchange.getTree();

        if (_pendingStoreCount > 0) {
            Key key = exchange.getKey();
            Key candidateKey2 = _ex1.getKey();
            //
            // First see if there is a pending store operation
            //
            prepareTxnExchange(tree, key, 'S');
            if (_ex1.traverse(direction, deep, minBytes) && sameTree()) {
                shiftOut();
                int comparison = candidateKey.compareTo(candidateKey2);

                boolean reverse = (direction == Key.LT)
                        || (direction == Key.LTEQ);

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
                if (candidateKey.compareTo(key2) <= 0) {
                    return Boolean.FALSE;
                }
            }
        }

        return null;
    }

    void fetchFromLastTraverse(Exchange exchange, int minimumBytes)
            throws PersistitException {
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
    void store(Exchange exchange, Key key, Value value)
            throws PersistitException {
        checkState();

        Tree tree = exchange.getTree();
        prepareTxnExchange(tree, key, 'S');

        boolean longRec = value.isDefined()
                && (value.getEncodedBytes()[0] & 0xFF) == Buffer.LONGREC_TYPE
                && value.getEncodedSize() >= Buffer.LONGREC_SIZE;

        if (longRec) {
            //
            // If the value represents a long record, then remember the
            // information necessary to deallocate the long record change in the
            // event the transaction gets rolled back.
            //
            Volume volume = tree.getVolume();
            long pageAddr = Buffer.decodeLongRecordDescriptorPointer(value
                    .getEncodedBytes(), 0);

            if (Debug.ENABLED) {
                Buffer buffer = volume.getPool().get(volume, pageAddr, false,
                        true);
                Debug.$assert(buffer.isLongRecordPage());
                buffer.release();
            }

            _longRecordDeallocationList.add(new DeallocationChain(volume, tree
                    .getTreeIndex(), pageAddr, 0));

            value.getEncodedBytes()[0] = (byte) NEUTERED_LONGREC;

        }
        _ex1.storeInternal(_ex1.getKey(), value, 0, false, false);
        _pendingStoreCount++;
    }

    boolean removeTree(Exchange exchange) throws PersistitException {
        exchange.getAuxiliaryKey1().clear().append(Key.BEFORE);
        exchange.getAuxiliaryKey2().clear().append(Key.AFTER);
        boolean removed = remove(exchange, exchange.getAuxiliaryKey1(),
                exchange.getAuxiliaryKey2(), false);
        exchange.getValue().clear();
        exchange.clear();
        prepareTxnExchange(exchange.getTree(), exchange.getKey(), 'D');
        _ex1.storeInternal(_ex1.getKey(), exchange.getValue(), 0, false, false);
        return removed;
    }

    boolean remove(Exchange exchange, Key key1, Key key2, boolean fetchFirst)
            throws PersistitException {
        checkState();
        //
        // Remove any applicable store operations
        //
        if (_ex1 == null)
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

        boolean result1 = _ex1.removeKeyRangeInternal(spareKey1, spareKey2,
                fetchFirst);

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
            // This logic coallesces previous remove operations when possible.
            // This simplifies the work necessary to perform a correct
            // traversal across the uncommitted updates.
            //
            Value value = _ex1.getValue();
            if (_pendingRemoveCount > 0) {
                // If the left edge of this key removal range overlaps a
                // previously posted remove oepration, then reset the left edge
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

    private void applyUpdates() throws PersistitException {
        if (_ex1 == null)
            setupExchanges();
        _ex1.clear().append('C').fetch();
        Value txnValue = _ex1.getValue();
        if (!txnValue.isDefined())
            return;

        int currentTreeHandle = -1;
        Tree currentTree = null;

        final Set<Tree> removedTrees = new HashSet<Tree>();

        while (_ex1.traverse(Key.GT, true)) {
            Key key1 = _ex1.getKey();
            key1.reset();
            char type = key1.decodeChar();
            if (type != 'R' && type != 'S' && type != 'D')
                continue;

            int treeHandle = key1.decodeInt();
            if (treeHandle != currentTreeHandle || currentTree == null) {
                currentTree = treeForHandle(treeHandle);
                currentTreeHandle = treeHandle;
            }

            _ex2.setTree(currentTree);
            int offset = key1.getIndex();
            int size = key1.getEncodedSize() - offset;

            Key key2 = _ex2.getKey();
            System.arraycopy(key1.getEncodedBytes(), offset, key2
                    .getEncodedBytes(), 0, size);
            key2.setEncodedSize(size);

            if (type == 'R') {
                key2.copyTo(_ex2.getAuxiliaryKey1());
                txnValue.decodeAntiValue(_ex2);

                _ex2.removeKeyRangeInternal(_ex2.getAuxiliaryKey1(), _ex2
                        .getAuxiliaryKey2(), false);
            } else if (type == 'S') {
                if (txnValue.isDefined()
                        && txnValue.getEncodedSize() >= Buffer.LONGREC_SIZE
                        && (txnValue.getEncodedBytes()[0] & 0xFF) == NEUTERED_LONGREC) {
                    txnValue.getEncodedBytes()[0] = (byte) Buffer.LONGREC_TYPE;
                }
                _ex2.storeInternal(key2, txnValue, 0, false, false);
                removedTrees.remove(_ex2.getTree());
            } else if (type == 'D') {
                removedTrees.add(_ex2.getTree());
            }
        }

        for (final Tree tree : removedTrees) {
            removeTreeHandle(tree);
            tree.getVolume().removeTree(tree);
        }
    }

    void rollbackUpdates() throws PersistitException {
        if (_ex1 == null)
            setupExchanges();
        _touchedPagesSet.clear();
        harvestLongRecords();

        if (_pendingStoreCount > 0 || _pendingRemoveCount > 0) {
            //
            // Remove the update list.
            //
            clear();
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
                    Buffer buffer = volume.getPool().get(volume, pageAddr,
                            false, true);
                    Debug.$assert(buffer.isLongRecordPage());
                    buffer.release();
                }

                dc._volume.deallocateGarbageChain(dc._treeIndex, dc._leftPage,
                        dc._rightPage);

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
            if (key1.decodeType() != Character.class
                    || key1.decodeChar() != 'S') {
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
                long pageAddress = Buffer.decodeLongRecordDescriptorPointer(
                        bytes, 0);

                if (Debug.ENABLED)
                    Debug.$assert(pageAddress > 0
                            && pageAddress < Buffer.MAX_VALID_PAGE_ADDR);

                if (Debug.ENABLED) {
                    Buffer buffer = volume.getPool().get(volume, pageAddress,
                            false, true);
                    Debug.$assert(buffer.isLongRecordPage());
                    buffer.release();
                }

                _longRecordDeallocationList.add(new DeallocationChain(volume,
                        currentTree.getTreeIndex(), pageAddress, 0));
            }
        }
    }

    static void recover(final Persistit persistit) {
        int committed = 0;
        int rolledBack = 0;

        if (persistit.getLogBase().isLoggable(LogBase.LOG_TXN_RECOVERY_START)) {
            persistit.getLogBase().log(LogBase.LOG_TXN_RECOVERY_START);
        }
        try {
            Volume txnVolume = persistit.getTransactionVolume();
            String txnTreeName = TRANSACTION_TREE_NAME;

            for (;;) {
                txnTreeName = txnVolume.nextTreeName(txnTreeName);
                if (txnTreeName == null
                        || !txnTreeName.startsWith(TRANSACTION_TREE_NAME)) {
                    break;
                }
                Exchange exchange = persistit.getExchange(txnVolume,
                        txnTreeName, false);
                exchange.ignoreTransactions();
                long id = Long.parseLong(txnTreeName
                        .substring(TRANSACTION_TREE_NAME.length()));

                Transaction txn = new Transaction(persistit, id);
                txn._recoveryMode = true;
                Value txnValue = exchange.getValue();
                try {
                    if (txnValue.isDefined()) {
                        txn.applyUpdates();
                        committed++;
                    } else {
                        txn.rollbackUpdates();
                        rolledBack++;
                    }
                    exchange.removeTree();
                } catch (PersistitException pe) {
                    if (persistit.getLogBase().isLoggable(
                            LogBase.LOG_TXN_RECOVERY_FAILURE)) {
                        persistit.getLogBase().log(
                                LogBase.LOG_TXN_RECOVERY_FAILURE, pe);
                    }
                }
            }
        } catch (TreeNotFoundException tnfe) {
            // This is okay - it means there never has been a "_txn"
            // tree in the transaction volume.
        } catch (VolumeNotFoundException tnfe) {
            // This is okay - it means is no transaction volume.
        } catch (PersistitException pe) {
            persistit.getLogBase().log(LogBase.LOG_EXCEPTION, pe);
        }

        if (persistit.getLogBase().isLoggable(LogBase.LOG_TXN_RECOVERY_END)) {
            persistit.getLogBase().log(LogBase.LOG_TXN_RECOVERY_END, committed,
                    rolledBack);
        }
    }

    /**
     * Allocate a new timestamp. Actions performed by this Transaction will be
     * marked with this (or possibly a larger) timestamp.
     */
    void assignTimestamp() {
        if (this != NEVER_ACTIVE_TRANSACTION) {
            _timestamp = _persistit.getTimestampAllocator().timestamp();
        }
    }

    /**
     * Return the timestamp most recently allocated to this Transaction.
     * 
     * @return The timestamp.
     */
    long getTimestamp() {
        return _timestamp;
    }

}
