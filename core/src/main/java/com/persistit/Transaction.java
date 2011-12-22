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

import java.nio.ByteBuffer;

import com.persistit.Accumulator.Delta;
import com.persistit.JournalRecord.D0;
import com.persistit.JournalRecord.D1;
import com.persistit.JournalRecord.DR;
import com.persistit.JournalRecord.DT;
import com.persistit.JournalRecord.SR;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.exception.RollbackException;
import com.persistit.util.Util;

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
    final static int TRANSACTION_BUFFER_SIZE = 65536;
    final static int MAXIMUM_STEP = TransactionIndex.VERSION_HANDLE_MULTIPLIER - 1;

    private static long _idCounter = 100000000;

    private final Persistit _persistit;
    private final SessionId _sessionId;
    private final long _id;
    private int _nestedDepth;
    private boolean _rollbackPending;
    private boolean _commitCompleted;

    private long _rollbackCount = 0;
    private long _commitCount = 0;
    private int _rollbacksSinceLastCommit = 0;

    private TransactionStatus _transactionStatus;
    private long _startTimestamp;
    private long _commitTimestamp;

    private final ByteBuffer _buffer = ByteBuffer.allocate(TRANSACTION_BUFFER_SIZE);

    private long _previousJournalAddress;

    private int _step;

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
    }

    private static synchronized long nextId() {
        return ++_idCounter;
    }

    /**
     * Release all resources associated with this transaction context.
     * 
     * @throws PersistitException
     */
    void close() throws PersistitException {
    }

    /**
     * Throws a {@link RollbackException} if this transaction context has a
     * rollback transition pending.
     * 
     * @throws RollbackException
     */
    public void checkPendingRollback() throws RollbackException {
        if (_rollbackPending) {
            throw new RollbackException();
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
        if (_nestedDepth == 0) {
            try {
                _transactionStatus = _persistit.getTransactionIndex().registerTransaction();
            } catch (InterruptedException e) {
                throw new PersistitInterruptedException(e);
            }
            _rollbackPending = false;
            _startTimestamp = _transactionStatus.getTs();
            _commitTimestamp = 0;
            _step = 0;
            _buffer.clear();
            _previousJournalAddress = 0;
        } else {
            checkPendingRollback();
        }
        _nestedDepth++;
    }

    void beginCheckpoint() throws PersistitException {
        if (_commitCompleted) {
            throw new IllegalStateException("Attempt to begin a committed transaction");
        }
        if (_nestedDepth == 0) {
            try {
                _transactionStatus = _persistit.getTransactionIndex().registerCheckpointTransaction();
            } catch (InterruptedException e) {
                throw new PersistitInterruptedException(e);
            }
            _rollbackPending = false;
            _startTimestamp = _transactionStatus.getTs();
            _commitTimestamp = 0;
            _step = 0;
            _buffer.clear();
            _previousJournalAddress = 0;
        } else {
            checkPendingRollback();
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

        if (_nestedDepth == 0) {
            // If not committed, this is an implicit rollback (with a log
            // message
            // if rollback was not called explicitly).
            //
            if (!_commitCompleted) {
                if (!_rollbackPending) {
                    _persistit.getLogBase().txnNotCommitted.log(new RollbackException());
                }
                _rollbackPending = true;

            }
            //
            // Perform rollback if needed.
            //
            if (_rollbackPending) {
                _rollbackCount++;
                _rollbacksSinceLastCommit++;

                // TODO - rollback

                if (!_transactionStatus.isNotified()) {
                    _transactionStatus.abort();
                    _persistit.getTransactionIndex().notifyCompleted(_transactionStatus,
                            _persistit.getTimestampAllocator().getCurrentTimestamp());
                }
            } else {
                _commitCount++;
                _rollbacksSinceLastCommit = 0;
            }
            _transactionStatus = null;
            _rollbackPending = false;
        }
        _commitCompleted = false;
    }

    /**
     * <p>
     * Explicitly rolls back all work done within the scope of this transaction.
     * If this transaction is not active, then this method throws an Exception.
     * No further updates can be applied within the scope of this transaction.
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
     */
    public void rollback() {
        if (_commitCompleted) {
            throw new IllegalStateException("Already committed");
        }

        if (_nestedDepth < 1) {
            throw new IllegalStateException("No transaction scope: begin() not called");
        }

        _rollbackPending = true;

        // TODO - rollback
        _transactionStatus.abort();
        _persistit.getTransactionIndex().notifyCompleted(_transactionStatus,
                _persistit.getTimestampAllocator().getCurrentTimestamp());
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

        checkPendingRollback();
        if (_nestedDepth == 1) {
            for (Delta delta = _transactionStatus.getDelta(); delta != null; delta = delta.getNext()) {
                writeDeltaToJournal(delta);
            }
            _transactionStatus.commit(_persistit.getTimestampAllocator().getCurrentTimestamp());
            _commitTimestamp = _persistit.getTimestampAllocator().updateTimestamp();
            try {
                /*
                 * TODO - figure out what to do if writes fail - I believe we
                 * will want to mark the transaction status as ABORTED in that
                 * case, but need to go look hard at TransactionIndex.
                 */
                flushTransactionBuffer();
                if (toDisk) {
                    _persistit.getJournalManager().force();
                }
            } finally {
                _persistit.getTransactionIndex().notifyCompleted(_transactionStatus, _commitTimestamp);
            }
            _commitCompleted = true;
        }
    }

    /**
     * Returns the nested level count. When no transaction scope is active this
     * method returns 0. Within the outermost transaction scope this method
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
     * the transaction. Once the retry count is exhausted, this method throws a
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
                        Util.sleep(retryDelay);
                    } catch (PersistitInterruptedException ie) {
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
     * @return the internal start timestamp of this transaction.
     */
    long getStartTimestamp() {
        return _startTimestamp;
    }

    /**
     * @return the commit timestamp - is zero during a currently executing
     *         transaction.
     */
    long getCommitTimestamp() {
        return _commitTimestamp;
    }

    /**
     * @return the SessionId this Transaction context belongs too.
     */
    public SessionId getSessionId() {
        return _sessionId;
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
     * Record a store operation.
     * 
     * @param exchange
     * @param key
     * @param value
     * @throws PersistitException
     */
    void store(Exchange exchange, Key key, Value value) throws PersistitException {
        if (_nestedDepth > 0) {
            checkPendingRollback();
            final int treeHandle = _persistit.getJournalManager().handleForTree(exchange.getTree());
            writeStoreRecordToJournal(treeHandle, key, value);
        }
    }

    /**
     * Record a remove operation.
     * 
     * @param exchange
     * @param key1
     * @param key2
     * @throws PersistitException
     */
    void remove(Exchange exchange, Key key1, Key key2) throws PersistitException {
        if (_nestedDepth > 0) {
            checkPendingRollback();
            final int treeHandle = _persistit.getJournalManager().handleForTree(exchange.getTree());
            writeDeleteRecordToJournal(treeHandle, key1, key2);
        }
    }

    /**
     * Record a tree delete operation
     * 
     * @param exchange
     * @throws PersistitException
     */
    void removeTree(Exchange exchange) throws PersistitException {
        if (_nestedDepth > 0) {
            checkPendingRollback();
            final int treeHandle = _persistit.getJournalManager().handleForTree(exchange.getTree());
            writeDeleteTreeToJournal(treeHandle);
        }
    }

    private void prepare(final int recordSize) throws PersistitIOException {
        if (recordSize > _buffer.remaining()) {
            flushTransactionBuffer();
        }
        if (recordSize > _buffer.remaining()) {
            throw new IllegalStateException("Record size " + recordSize + " is too long for Transaction buffer");
        }
    }

    void flushTransactionBuffer() throws PersistitIOException {
        if (_buffer.position() > 0) {
            _previousJournalAddress = _persistit.getJournalManager().writeTransactionToJournal(_buffer,
                    _startTimestamp, _commitTimestamp, _previousJournalAddress);
        }
    }

    void writeStoreRecordToJournal(final int treeHandle, final Key key, final Value value) throws PersistitIOException {
        final int recordSize = SR.OVERHEAD + key.getEncodedSize() + value.getEncodedSize();
        prepare(recordSize);
        SR.putLength(_buffer, recordSize);
        SR.putType(_buffer);
        SR.putTreeHandle(_buffer, treeHandle);
        SR.putKeySize(_buffer, (short) key.getEncodedSize());
        _buffer.position(_buffer.position() + SR.OVERHEAD);
        _buffer.put(key.getEncodedBytes(), 0, key.getEncodedSize());
        _buffer.put(value.getEncodedBytes(), 0, value.getEncodedSize());
    }

    void writeDeleteRecordToJournal(final int treeHandle, final Key key1, final Key key2) throws PersistitIOException {
        int elisionCount = key2.firstUniqueByteIndex(key1);
        int recordSize = DR.OVERHEAD + key1.getEncodedSize() + key2.getEncodedSize() - elisionCount;
        prepare(recordSize);

        DR.putLength(_buffer, recordSize);
        DR.putType(_buffer);
        DR.putTreeHandle(_buffer, treeHandle);
        DR.putKey1Size(_buffer, key1.getEncodedSize());
        DR.putKey2Elision(_buffer, elisionCount);
        _buffer.position(_buffer.position() + DR.OVERHEAD);
        _buffer.put(key1.getEncodedBytes(), 0, key1.getEncodedSize());
        _buffer.put(key2.getEncodedBytes(), elisionCount, key2.getEncodedSize() - elisionCount);
    }

    void writeDeleteTreeToJournal(final int treeHandle) throws PersistitIOException {
        prepare(DT.OVERHEAD);
        JournalRecord.putLength(_buffer, DT.OVERHEAD);
        DT.putType(_buffer);
        DT.putTreeHandle(_buffer, treeHandle);
        _buffer.position(_buffer.position() + DT.OVERHEAD);
    }

    void writeDeltaToJournal(final Delta delta) throws PersistitIOException {
        final int treeHandle = _persistit.getJournalManager().handleForTree(delta.getAccumulator().getTree());
        if (delta.getValue() == 1) {
            prepare(D0.OVERHEAD);
            JournalRecord.putLength(_buffer, D0.OVERHEAD);
            D0.putType(_buffer);
            D0.putTreeHandle(_buffer, treeHandle);
            D0.putAccumulatorTypeOrdinal(_buffer, delta.getAccumulator().getType().ordinal());
            D0.putIndex(_buffer, delta.getAccumulator().getIndex());
            _buffer.position(_buffer.position() + D0.OVERHEAD);
        } else {
            prepare(D1.OVERHEAD);
            JournalRecord.putLength(_buffer, D1.OVERHEAD);
            D1.putType(_buffer);
            D1.putTreeHandle(_buffer, treeHandle);
            D1.putIndex(_buffer, delta.getAccumulator().getIndex());
            D1.putAccumulatorTypeOrdinal(_buffer, delta.getAccumulator().getType().ordinal());
            D1.putValue(_buffer, delta.getValue());
            _buffer.position(_buffer.position() + D1.OVERHEAD);
        }
    }

    TransactionStatus getTransactionStatus() {
        return _transactionStatus;
    }

    public int getCurrentStep() {
        return _step;
    }

    public void incrementStep() {
        checkPendingRollback();
        if (_step < MAXIMUM_STEP) {
            _step++;
        } else {
            throw new IllegalStateException(this + " is already at step " + MAXIMUM_STEP + " and cannot be incremented");
        }
    }

    /**
     * For unit tests only
     * 
     * @return the buffer used to accumulate update records for this transaction
     */
    ByteBuffer getTransactionBuffer() {
        return _buffer;
    }

}
