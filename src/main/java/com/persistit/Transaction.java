/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
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

import static com.persistit.util.SequencerConstants.COMMIT_FLUSH_A;
import static com.persistit.util.SequencerConstants.COMMIT_FLUSH_B;
import static com.persistit.util.SequencerConstants.COMMIT_FLUSH_C;
import static com.persistit.util.ThreadSequencer.sequence;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.persistit.Accumulator.Delta;
import com.persistit.CleanupManager.CleanupAction;
import com.persistit.CleanupManager.CleanupPruneAction;
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
 * Transaction context for atomic units of work performed by Persistit. Each
 * Persistit thread typically uses one <code>Transaction</code> object for its
 * lifetime. Within that context it may execute and commit many transactions.
 * </p>
 * <p>
 * The application determines when to {@link #begin}, {@link #commit},
 * {@link #rollback} and {@link #end} transactions. Once a transaction has
 * started, no update operation performed within its context will be visible to
 * other threads until <code>commit</code> is performed. At that point, all the
 * updates become visible and durable.
 * </p>
 * <p>
 * Persistit implements Multi-Version Concurrency Control (MVCC) with <a
 * href="http://wikipedia.org/wiki/Snapshot_isolation">Snapshot Isolation</a>
 * for high concurrency and throughput. This protocol is <i>optimistic</i> in
 * that competing concurrent transactions run at full speed without locking, but
 * can arrive in a state where not all transactions can be allowed to commit
 * while preserving correct semantics. In such cases one or more of the
 * transactions must roll back (abort) and retry. These topics are covered in
 * greater detail below.
 * </p>
 * <p>
 * The <code>Transaction</code> object itself is not thread-safe and may be
 * accessed and used by only one thread at a time. However, the database
 * operations being executed within the scope defined by <code>begin</code> and
 * <code>end</code> are capable of highly concurrent execution.
 * </p>
 * <h2>Lexical Scoping</h2>
 * <p>
 * Applications must manage the scope of any transaction by ensuring that
 * whenever the <code>begin</code> method is called, there is a concluding
 * invocation of the <code>end</code> method. The <code>commit</code> and
 * <code>rollback</code> methods do not end the scope of a transaction; they
 * merely signify the transaction's intended outcome when <code>end</code> is
 * invoked. Applications should follow either the <a href="#_pattern1">
 * <code>try/finally</code></a> or the <a href="#_pattern2">
 * <code>TransactionRunnable</code></a> pattern to ensure correctness.
 * </p>
 * <p>
 * <a name="commitPolicy"/>
 * <h2>Commit Policy</h2>
 * Persistit provides three policies that determine the durability of a
 * transaction after it has executed the <code>commit</code> method. These are:
 * <dl>
 * <dt>{@link CommitPolicy#HARD}</dt>
 * <dd>The <code>commit</code> method does not return until all updates created
 * by the transaction have been written to non-volatile storage (e.g., disk
 * storage).</dd>
 * <dt>{@link CommitPolicy#GROUP}</dt>
 * <dd>The <code>commit</code> method does not return until all updates created
 * by the transaction have been written to non-volatile storage. In addition,
 * the committing transaction waits briefly in an attempt to recruit other
 * concurrently running transactions to write their updates with the same
 * physical I/O operation.</dd>
 * <dt>{@link CommitPolicy#SOFT}</dt>
 * <dd>The <code>commit</code> method returns <i>before</i> the updates have
 * been recorded on non-volatile storage. Persistit attempts to write them
 * within 100 milliseconds, but this interval is not guaranteed.</dd>
 * </dl>
 * <p>
 * You can specify a default policy in the Persistit initialization properties
 * using the {@value com.persistit.Configuration#COMMIT_POLICY_PROPERTY_NAME}
 * property or under program control using
 * {@link Persistit#setDefaultTransactionCommitPolicy} . The default policy
 * applies to the {@link #commit()} method. You can override the default policy
 * using {@link #commit(CommitPolicy)}.
 * </p>
 * <p>
 * HARD and GROUP ensure each transaction is written durably to non-volatile
 * storage before the <code>commit</code> method returns. The difference is that
 * GROUP can improve throughput somewhat when many transactions are running
 * concurrently because the average number of I/O operations needed to commit N
 * transactions can be smaller than N. However, for one or a small number of
 * concurrent threads, GROUP reduces throughput because it works by introducing
 * a delay to allow other concurrent transactions to commit within a single I/O
 * operation.
 * </p>
 * <p>
 * SOFT commits are generally much faster than HARD or GROUP commits, especially
 * for single-threaded applications, because the results of numerous update
 * operations can be aggregated and written to disk in a much smaller number of
 * I/O operations. However, transactions written with the SOFT commit policy are
 * not immediately durable and it is possible that the recovered state of a
 * database will be missing transactions that were committed shortly before a
 * crash.
 * </p>
 * <p>
 * For SOFT commits, the state of the database after restart is such that for
 * any committed transaction T, either all or none of its modifications will be
 * present in the recovered database. Further, if a transaction T2 reads or
 * updates data that was written by any other transaction T1, and if T2 is
 * present in the recovered database, then so is T1. Any transaction that was in
 * progress, but had not been committed at the time of the failure, is
 * guaranteed not to be present in the recovered database. SOFT commits are
 * designed to be durable within 100 milliseconds after the commit returns.
 * However, this interval is determined by computing the average duration of
 * recent I/O operations to predict the completion time of the I/O that will
 * write the transaction to disk, and therefore the interval cannot be
 * guaranteed.
 * </p>
 * <h2>Optimistic Concurrent Scheduling - MVCC</h2>
 * <p>
 * Persistit schedules concurrently executing transactions optimistically,
 * without locking any database records. Instead, Persistit uses a well-known
 * protocol called Snapshot Isolation to achieve atomicity and isolation. While
 * transactions are modifying data, Persistit maintains multiple versions of
 * values being modified. Each version is labeled with the commit timestamp of
 * the transaction that modified it. Whenever a transaction reads a value that
 * has been modified by other transactions, it reads the latest version that was
 * committed before its own start timestamp. In other words, all read operations
 * are performed as if from a "snapshot" of the state of the database made at
 * the transaction's start timestamp - hence the name "Snapshot Isolation."
 * </p>
 * <h3>Pruning</h3>
 * <p>
 * Given that all updates written through transactions are created as versions
 * within the MVCC scheme, a large number of versions can accumulate over time.
 * Persistit reduces this number through an activity called "version pruning."
 * Pruning resolves the final state of each version by removing any versions
 * created by aborted transactions and removing obsolete versions no longer
 * needed by currently executing transactions. If a value contains only one
 * version and the commit timestamp of the transaction that created it is before
 * the start of any currently running transaction, that value is called
 * <i>primordial</i>. The goal of pruning is to reduce almost all values in a
 * Persistit Tree to their primordial states because updating and reading
 * primordial values is more efficient than the handling required for multiple
 * version values. Pruning happens automatically and is generally not visible to
 * the application.
 * </p>
 * <h3>Rollbacks</h3>
 * <p>
 * Usually Snapshot Isolation allows concurrent transactions to commit without
 * interference but this is not always the case. Two concurrent transactions
 * that attempt to modify the same Persistit key-value pair before committing
 * are said to have a "write-write dependency". To avoid anomalous results one
 * of them must abort, rolling back any other updates it may have created, and
 * retry. Persistit implements a "first updater wins" policy in which if two
 * transactions attempt to update the same record, the first transaction "wins"
 * by being allowed to continue, while the second transaction "loses" and is
 * required to abort.
 * </p>
 * <p>
 * Once a transaction has aborted, any subsequent database operation it attempts
 * throws a {@link RollbackException}. Application code should generally catch
 * and handle the <code>RollbackException</code>. Usually the correct and
 * desired behavior is simply to retry the transaction. See <a
 * href="#_pattern1"><code>try/finally</code></a> for a code pattern that
 * accomplishes this.
 * </p>
 * <p>
 * A transaction can also voluntarily roll back. For example, transaction logic
 * could detect an error condition that it chooses to handle by throwing an
 * exception back to the application. In this case the transaction should invoke
 * the {@link #rollback} method to explicit declare its intent to abort the
 * transaction.
 * </p>
 * <h3>Read-Only Transactions</h3>
 * <p>
 * Under Snapshot Isolation, transactions that read but do not modify data
 * cannot generate any write-write dependencies and are therefore not subject to
 * being rolled back because of the actions of other transactions. However, note
 * that even if it modifies no data, a long-running transaction can force
 * Persistit to retain old value versions for its duration in order to provide a
 * snapshot view. This behavior can cause congestion and performance degradation
 * by preventing very old values from being pruned. The degree to which this is
 * a problem depends on the volume of update transactions being processed and
 * the duration of long-running transactions.
 * </p>
 * 
 * <a name="_pattern1"/> <h2>The try/finally/retry Code Pattern</h2>
 * <p>
 * The following code fragment illustrates a transaction executed with up to to
 * RETRY_COUNT retries. If the <code>commit</code> method succeeds, the whole
 * transaction is completed and the retry loop terminates. If after RETRY_COUNT
 * retries <code>commit</code> has not been successfully completed, the
 * application throws a <code>TransactionFailedException</code>.
 * </p>
 * 
 * <blockquote><code><pre>
 *     Transaction txn = Persistit.getTransaction();
 *     int remainingAttempts = RETRY_COUNT;
 *     for (;;) {
 *         txn.begin();         // Begin transaction scope
 *         try {
 *             //
 *             // ...perform Persistit fetch, remove and store operations...
 *             //
 *             txn.commit();     // attempt to commit the updates
 *             break;            // Exit retry loop
 *         }
 *         catch (RollbackException re) {
 *             if (--remainingAttempts &lt; 0) {
 *                 throw new TransactionFailedException(); 
 *             {
 *         }
 *         finally {
 *             txn.end();       // End transaction scope. Implicitly 
 *                              // roll back all updates unless
 *                              // commit completed successfully.
 *         }
 *     }
 * </pre></code></blockquote>
 * 
 * <a name="_pattern2" /> <h2>The TransactionRunnable Pattern</h2>
 * <p>
 * As an alternative, the application can embed the actual database operations
 * within a {@link TransactionRunnable} and invoke the {@link #run} method to
 * execute it. The retry logic detailed in the fragment shown above is handled
 * automatically by <code>run</code>; it could be rewritten as follows:
 * 
 * <blockquote><code><pre>
 *     Transaction txn = Persistit.getTransaction();
 *     txn.run(new TransactionRunnable() {
 *         public void runTransaction()
 *         throws PersistitException, RollbackException {
 *             //
 *             //...perform Persistit fetch, remove and store operations...
 *             //
 *         }
 *     }, RETRY_COUNT, 0);
 * </pre></code></blockquote>
 * 
 * </p>
 * <a name="_scopedCodePattern"/> <h2>Nested Transaction Scope</h2>
 * <p>
 * Persistit supports nested transactions by counting the number of nested
 * {@link #begin} and {@link #end} operations. Each invocation of
 * <code>begin</code> increments this count and each invocation of
 * <code>end</code> decrements the count. These methods are intended to be used
 * in a standard essential pattern, shown here, to ensure that the scope of of
 * the transaction is reliably determined by the lexical the structure of the
 * code rather than conditional logic: <blockquote><code><pre>
 *     <b>txn.begin();</b>
 *     try {
 *         //
 *         // Application transaction logic here, possibly including 
 *         // invocation of methods that also call txn.begin() and
 *         // txn.end().
 *         //
 *         <b>txn.commit();</b>
 *     } finally {
 *         <b>txn.end();</b>
 *     }
 * </pre></code></blockquote>
 * </p>
 * <p>
 * This pattern ensures that the transaction scope is ended properly regardless
 * of whether the application code throws an exception or completes and commits
 * normally.
 * </p>
 * <p>
 * The {@link #commit} method performs the actual commit operation only when the
 * current nested level count (see {@link #getNestedTransactionDepth()}) is 1.
 * That is, if <code>begin</code> has been invoked N times, then
 * <code>commit</code> will actually commit the data only when <code>end</code>
 * is invoked the Nth time. Data updated by an inner (nested) transaction is
 * never actually committed until the outermost <code>commit</code> is called.
 * This permits transactional code to invoke other code (possibly an opaque
 * library supplied by a third party) that may itself <code>begin</code> and
 * <code>commit</code> transactions.
 * </p>
 * <p>
 * Invoking {@link #rollback} removes all pending but uncommitted updates and
 * marks the current transaction scope as <i>rollback pending</i>. Any
 * subsequent attempt to perform any Persistit operation, including
 * <code>commit</code> in the current transaction scope, will fail with a
 * <code>RollbackException</code>.
 * </p>
 * <p>
 * Application developers should beware that the {@link #end} method performs an
 * implicit rollback if <code>commit</code> has not completed. Therefore, if an
 * application fails to call <code>commit</code>, the transaction will silently
 * fail. The <code>end</code> method sends a warning message to the log
 * subsystem when this happens, but does not throw an exception. The
 * <code>end</code> method is designed this way to allow an exception thrown
 * within the application code to be caught and handled without being obscured
 * by a RollbackException thrown by <code>end</code>. But as a consequence,
 * developers must carefully verify that the <code>end</code> method is always
 * invoked whether or not the transaction completes normally.
 * </p>
 * <h2>Step Index: Controlling Visibility of Uncommitted Updates</h2>
 * <p>
 * By default, application logic within the scope of a transaction can read two
 * kinds of values: those that were committed by other transactions prior to the
 * start of the current transaction (from the "snapshot") and those that were
 * modified by the transaction itself. However, in some applications it is
 * useful to control the visibility of modifications made by the current
 * transaction. For example, update queries that select records to update and
 * then change the very values used as selection criteria can produce anomalous
 * results. See <a
 * href="http://en.wikipedia.org/wiki/Halloween_Problem">Halloween Problem</a>
 * for a succinct description of this issue. Persistit provides a mechanism to
 * control visibility of a transaction's own modifications to avoid this
 * problem.
 * </p>
 * <p>
 * While a transaction is executing, every updated value it generates is stored
 * within a multi-version value and labeled with the transaction ID of the
 * transaction that produced it <u>and</u> a small integer index (0-99) called
 * the <i>step</i>.
 * </p>
 * <p>
 * The current step index is an attribute of the <code>Transaction</code> object
 * available from {@link #getStep}. The <code>begin</code> method resets its
 * value to zero. An application can invoke {@link #incrementStep} to increment
 * it, or {@link #setStep} to control its current value. Modifications created
 * by the transaction are labeled with the current step value.
 * </p>
 * <p>
 * When reading data, modifications created by the current transaction are
 * visible to Persistit if and only if the step number they were assigned is
 * less or equal to the <code>Transaction</code>'s current step number. An
 * application can take advantage of this by controlling the current step index,
 * for example, by reading data using step 0 while posting updates with a step
 * value of 1.
 * </p>
 * <a name="_threadManagement" /> <h2>Thread Management</h2>
 * <p>
 * As noted above, a <code>Transaction</code> typically belongs to one thread
 * for its entire lifetime and is <i>not</i> threadsafe. However, to support
 * server applications which may manage a large number of sessions among a
 * smaller number of threads, Persisit allows an application to manage sessions
 * explicitly. See {@link Persistit#getSessionId()} and
 * {@link Persistit#setSessionId(SessionId)}. The method
 * {@link Persistit#getTransaction()} is sensitive to the thread's current
 * <code>SessionId</code>, and therefore the following style of interaction is
 * possible:
 * <ul>
 * <li>Thread T1 is assigned work for session S.</li>
 * <li>Thread T1 invokes <code>begin</code>, does some work and then returns
 * control to a client.</li>
 * <li>Thread T2 receives additional work to perform on behalf of session S.</li>
 * <li>Thread T2 sets its current SessionId to session S</li>
 * <li>Thread T2 then uses {@link Persistit#getTransaction()} to acquire the
 * same transaction context previously started by T1.</li>
 * <li>Thread T2 does additional work and then calls <code>commit</code> and
 * <code>end</code> to complete the transaction.</li>
 * </ul>
 * Applications that use this technique must be written carefully to ensure that
 * multiple threads never execute with the same SessionId. Concurrent access to
 * a <code>Transaction</code> or <code>Exchange</code> can cause serious errors,
 * including database corruption.
 * </p>
 * <h2>Additional Notes</h2>
 * <p>
 * Optimistic concurrency control works well when the likelihood of conflicting
 * transactions - that is, concurrent execution of two or more transactions that
 * modify the same database records - is low. For most applications this
 * assumption is valid.
 * </p>
 * <p>
 * For best performance, applications in which multiple threads frequently
 * operate on overlapping data such that roll-backs are likely, the application
 * should implement its own locks to prevent or reduce the likelihood of
 * collisions.
 * </p>
 * <p>
 * An application can examine counts of commits, rollbacks and rollbacks since
 * the last successful commit using {@link #getCommittedTransactionCount()},
 * {@link #getRolledBackTransactionCount()} and
 * {@link #getRolledBackSinceLastCommitCount()}, respectively.
 * </p>
 * 
 * @author peter
 * @version 1.1
 */
public class Transaction {
    final static int MAXIMUM_STEP = TransactionIndex.VERSION_HANDLE_MULTIPLIER - 1;

    final static int TRANSACTION_BUFFER_SIZE = 65536;

    private static long _idCounter = 100000000;

    private final Persistit _persistit;
    private final SessionId _sessionId;
    private final long _id;

    private volatile int _nestedDepth;
    private volatile boolean _rollbackPending;
    private volatile boolean _rollbackCompleted;
    private volatile boolean _commitCompleted;

    private volatile long _rollbackCount = 0;
    private volatile long _commitCount = 0;
    private volatile int _rollbacksSinceLastCommit = 0;

    private volatile TransactionStatus _transactionStatus;
    private volatile long _startTimestamp;
    private volatile long _commitTimestamp;

    private final ByteBuffer _buffer = ByteBuffer.allocate(TRANSACTION_BUFFER_SIZE);

    private long _previousJournalAddress;

    private int _step;

    private String _threadName;

    private final Set<CleanupAction> _lockCleanupActions = new HashSet<CleanupAction>();

    public static enum CommitPolicy {
        /**
         * The {@link Transaction#commit} method returns before all updates have
         * been written to durable storage. This policy is a compromise that
         * offers much better throughput, especially for sequential
         * transactions, but does not provide durability for every committed
         * transactions. Some recently committed transactions may be lost after
         * a crash/recovery cycle.
         */
        SOFT,
        /**
         * Every committed transaction is flushed synchronously to durable
         * storage before the {@link Transaction#commit()} method returns. With
         * this policy every transaction is durable before commit completes.
         */
        HARD,
        /**
         * Every committed transaction is flushed to durable storage before
         * {@link Transaction#commit()} returns. Persistit attempts to
         * coordinate the I/O needed to do this with other pending transactions;
         * as a consequence, the commit method may pause briefly waiting for
         * other transactions to reach their commit points. In general this
         * option provides the slowest rate of sequential commits, but the
         * aggregate transaction throughput across many threads may be much
         * higher than with the HARD policy.
         */
        GROUP;

        static CommitPolicy forName(final String policyName) {
            for (final CommitPolicy policy : values()) {
                if (policy.name().equalsIgnoreCase(policyName)) {
                    return policy;
                }
            }
            throw new IllegalArgumentException("No such CommitPolicy: " + policyName);
        }
    }

    private CommitPolicy _defaultCommitPolicy = CommitPolicy.SOFT;

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
     * Release all resources associated with this transaction context. Abort the
     * transaction if it was abandoned due to thread death.
     * 
     * @throws PersistitException
     */
    void close() throws PersistitException {
        if (_nestedDepth > 0 && !_commitCompleted && !_rollbackCompleted) {
            final TransactionStatus ts = _transactionStatus;
            if (ts != null && ts.getTs() == _startTimestamp && !_commitCompleted && !_rollbackCompleted) {
                rollback();
                _persistit.getLogBase().txnAbandoned.log(this);
            }
        }
        /*
         * The background rollback cleanup should be stopped before calling this
         * method so the following check is deterministic.
         */
        final TransactionStatus status = _persistit.getTransactionIndex().getStatus(_startTimestamp);
        if (status != null && status.getMvvCount() > 0) {
            flushTransactionBuffer(false);
        }
    }

    /**
     * Throws a {@link RollbackException} if this transaction context has a
     * roll-back transition pending.
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
     * href="#_scopedCodePattern">above</a>.
     * 
     * @throws IllegalStateException
     *             if the current transaction scope has already been committed.
     */
    public void begin() throws PersistitException {
        if (_commitCompleted) {
            throw new IllegalStateException("Attempt to begin a committed transaction " + this);
        }
        if (_rollbackPending) {
            throw new IllegalStateException("Attempt to begin a transaction with pending rollback" + this);
        }
        if (_nestedDepth == 0) {
            flushTransactionBuffer(false);
            try {
                _transactionStatus = _persistit.getTransactionIndex().registerTransaction();
            } catch (final InterruptedException e) {
                _rollbackCompleted = true;
                throw new PersistitInterruptedException(e);
            }
            _rollbackPending = false;
            _rollbackCompleted = false;
            _startTimestamp = _transactionStatus.getTs();
            _commitTimestamp = 0;
            _step = 0;
            _threadName = Thread.currentThread().getName();
        } else {
            checkPendingRollback();
        }
        _nestedDepth++;
    }

    void beginCheckpoint() throws PersistitException {
        if (_commitCompleted) {
            throw new IllegalStateException("Attempt to begin a committed transaction " + this);
        }
        if (_rollbackPending) {
            throw new IllegalStateException("Attmpet to begin a transaction with pending rollback" + this);
        }
        if (_nestedDepth == 0) {
            flushTransactionBuffer(false);
            try {
                _transactionStatus = _persistit.getTransactionIndex().registerCheckpointTransaction();
            } catch (final InterruptedException e) {
                _rollbackCompleted = true;
                throw new PersistitInterruptedException(e);
            }
            _rollbackPending = false;
            _rollbackCompleted = false;
            _startTimestamp = _transactionStatus.getTs();
            _commitTimestamp = 0;
            _step = 0;
        } else {
            checkPendingRollback();
        }
        _nestedDepth++;

    }

    /**
     * <p>
     * End the current transaction scope. Application code should ensure that
     * every method that calls <code>begin</code> also invokes <code>end</code>
     * using a <code>try/finally</code> pattern described in <a
     * href="#_scopedCodePattern">above</a>.
     * </p>
     * <p>
     * This method implicitly rolls back any pending, uncommitted updates.
     * Updates are committed only if the <code>commit</code> method completes
     * successfully.
     * </p>
     * 
     * @throws PersistitIOException
     * 
     * @throws IllegalStateException
     *             if there is no current transaction scope.
     */
    public void end() {
        checkActive();

        if (_nestedDepth == 1) {
            //
            // If not committed, this is an implicit rollback (with a log
            // message if rollback was not called explicitly).
            //
            if (!_commitCompleted) {
                if (!_rollbackPending) {
                    _persistit.getLogBase().txnNotCommitted.log(this);
                }
                if (!_rollbackCompleted) {
                    rollback();
                }
            } else {
                _commitCount++;
                _rollbacksSinceLastCommit = 0;
            }
            try {
                pruneLockPages();
            } catch (final Exception e) {
                _persistit.getLogBase().pruneException.log(e, "locks");
            }
            _transactionStatus = null;
            _rollbackPending = false;
            _threadName = null;
        }

        _nestedDepth--;
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
     * @throws PersistitIOException
     * 
     * @throws IllegalStateException
     *             if there is no transaction scope or the current scope has
     *             already been committed.
     */
    public void rollback() {

        checkActive();

        if (_commitCompleted) {
            throw new IllegalStateException("Already committed " + this);
        }

        _rollbackPending = true;

        if (!_rollbackCompleted) {
            _rollbackCount++;
            _rollbacksSinceLastCommit++;
            _transactionStatus.abort();
            try {
                /*
                 * Necessary to enable rollback pruning
                 */
                flushTransactionBuffer(false);
            } catch (final PersistitException e) {
                _persistit.getLogBase().exception.log(e);
            } finally {
                _persistit.getTransactionIndex().notifyCompleted(_transactionStatus,
                        _persistit.getTimestampAllocator().getCurrentTimestamp());
                _rollbackCompleted = true;
            }
        }
    }

    /**
     * <p>
     * Commit this transaction. This method flushes the journal entries created
     * by the transaction to the journal buffer, optionally waits for those
     * changes to be written durably to disk according to the default configured
     * {@link CommitPolicy}, and marks the transaction as completed so that its
     * effects are visible to other transactions.
     * </p>
     * <p>
     * If executed within the scope of an outer transaction, this method simply
     * sets a flag indicating that the current transaction level has committed
     * without modifying any data. The commit for the outermost transaction
     * scope is responsible for actually committing the changes.
     * </p>
     * <p>
     * Once an application thread has called <code>commit</code>, no subsequent
     * Persistit database operations are permitted until the <code>end</code>
     * method has been called. An attempt to store, fetch or remove data after
     * <code>commit</code> has been called throws an
     * <code>IllegalStateException</code>.
     * </p>
     * 
     * @throws PersistitIOException
     *             if the transaction could not be written to the journal due to
     *             an IOException. This exception also causes the transaction to
     *             be rolled back.
     * 
     * @throws RollbackException
     *             if the {@link #rollback()} was previously called
     * 
     * @throws PersistitInterruptedException
     *             if the thread was interrupted while waiting for I/O
     *             completion
     * 
     * @throws IllegalStateException
     *             if no transaction scope is active or this transaction scope
     *             has already called <code>commit</code>
     * 
     * @throws PersistitException
     *             if a PersistitException was caught by the JOURNAL_FLUSHER
     *             thread after this transaction began waiting for durability
     * 
     * @see Persistit#getDefaultTransactionCommitPolicy()
     */
    public void commit() throws PersistitException {
        commit(_persistit.getDefaultTransactionCommitPolicy());
    }

    /**
     * Commit to disk with {@link CommitPolicy} determined by the supplied
     * boolean value. This method is obsolete and will be removed shortly.
     */
    @Deprecated
    public void commit(final boolean toDisk) throws PersistitException {
        commit(toDisk ? CommitPolicy.HARD : CommitPolicy.SOFT);
    }

    /**
     * <p>
     * Commit this transaction. This method flushes the journal entries created
     * by the transaction to the journal buffer, optionally waits for those
     * changes to be written durably to disk according to the supplied
     * {@link CommitPolicy}, and marks the transaction as completed so that its
     * effects are visible to other transactions.
     * </p>
     * <p>
     * If executed within the scope of an outer transaction, this method simply
     * sets a flag indicating that the current transaction level has committed
     * without modifying any data. The commit for the outermost transaction
     * scope is responsible for actually committing the changes.
     * </p>
     * <p>
     * Once an application thread has called <code>commit</code>, no subsequent
     * Persistit database operations are permitted until the <code>end</code>
     * method has been called. An attempt to store, fetch or remove data after
     * <code>commit</code> has been called throws an
     * <code>IllegalStateException</code>.
     * </p>
     * 
     * @param policy
     *            Determines whether the commit method waits until the
     *            transaction has been written to durable storage before
     *            returning to the caller.
     * 
     * @throws PersistitIOException
     *             if the transaction could not be written to the journal due to
     *             an IOException. This exception also causes the transaction to
     *             be rolled back.
     * 
     * @throws RollbackException
     *             if the {@link #rollback()} was previously called
     * 
     * @throws PersistitInterruptedException
     *             if the thread was interrupted while waiting for I/O
     *             completion
     * 
     * @throws IllegalStateException
     *             if no transaction scope is active or this transaction scope
     *             has already called <code>commit</code>
     * 
     * @throws PersistitException
     *             if a PersistitException was caught by the JOURNAL_FLUSHER
     *             thread after this transaction began waiting for durability
     * 
     */
    public void commit(final CommitPolicy policy) throws PersistitException {
        checkActive();

        if (_commitCompleted) {
            throw new IllegalStateException("Already committed " + this);
        }

        checkPendingRollback();
        if (_nestedDepth == 1) {
            if (_rollbackCompleted) {
                throw new IllegalStateException("Already rolled back " + this);
            }
            for (Delta delta = _transactionStatus.getDelta(); delta != null; delta = delta.getNext()) {
                writeDeltaToJournal(delta);
            }
            _transactionStatus.commit(_persistit.getTimestampAllocator().getCurrentTimestamp());
            sequence(COMMIT_FLUSH_A);
            _commitTimestamp = _persistit.getTimestampAllocator().updateTimestamp();
            sequence(COMMIT_FLUSH_C);
            long flushedTimetimestamp = 0;

            for (Delta delta = _transactionStatus.getDelta(); delta != null; delta = delta.getNext()) {
                final Accumulator acc = delta.getAccumulator();
                acc.checkpointNeeded(_commitTimestamp);
            }

            boolean committed = false;
            try {

                if (flushTransactionBuffer(false)) {
                    flushedTimetimestamp = _persistit.getTimestampAllocator().getCurrentTimestamp();
                }
                committed = true;
            } finally {
                _persistit.getTransactionIndex().notifyCompleted(_transactionStatus,
                        committed ? _commitTimestamp : TransactionStatus.ABORTED);
                _commitCompleted = committed;
                _rollbackPending = _rollbackCompleted = !committed;
            }

            _persistit.getJournalManager().throttle();
            if (flushedTimetimestamp != 0) {
                _persistit.getJournalManager().waitForDurability(flushedTimetimestamp,
                        policy == CommitPolicy.SOFT ? _persistit.getTransactionCommitLeadTime() : 0,
                        policy == CommitPolicy.GROUP ? _persistit.getTransactionCommitStallTime() : 0);
            }
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
     * href="#commitPolicy">memory</a>, which means that the update is not
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
    public int run(final TransactionRunnable runnable) throws PersistitException {
        return run(runnable, 0, 0, _persistit.getDefaultTransactionCommitPolicy());
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
     *            <code>SOFT</code>, <code>HARD</code> or <code>GROUP</code> to
     *            commit achieve durability or throughput, as required by the
     *            application.
     * 
     * @return Count of attempts needed to complete the transaction
     * 
     * @throws PersistitException
     * @throws RollbackException
     *             If after <code>retryCount+1</code> attempts the transaction
     *             cannot be completed or committed due to concurrent updated
     *             performed by other threads.
     */

    public int run(final TransactionRunnable runnable, final int retryCount, final long retryDelay, final boolean toDisk)
            throws PersistitException {
        return run(runnable, retryCount, retryDelay, toDisk ? CommitPolicy.HARD : CommitPolicy.SOFT);
    }

    public int run(final TransactionRunnable runnable, int retryCount, final long retryDelay, final CommitPolicy toDisk)
            throws PersistitException {
        if (retryCount < 0)
            throw new IllegalArgumentException();
        for (int count = 1;; count++) {
            begin();
            try {
                runnable.runTransaction();
                commit(toDisk);
                return count;
            } catch (final RollbackException re) {
                if (retryCount <= 0 || _nestedDepth > 1) {
                    throw re;
                }

                retryCount--;
                if (retryDelay > 0) {
                    try {
                        Util.sleep(retryDelay);
                    } catch (final PersistitInterruptedException ie) {
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
        return "Transaction_" + _id + " depth=" + _nestedDepth + " status=" + getStatus()
                + (_threadName == null ? "" : " owner=" + _threadName);
    }

    String getStatus() {
        final TransactionStatus status = _transactionStatus;
        final long ts = getStartTimestamp();
        if (status != null && status.getTs() == ts) {
            return status.toString();
        } else {
            return "<not running>";
        }
    }

    /**
     * @return the current default policy
     */
    public CommitPolicy getDefaultCommitPolicy() {
        return _defaultCommitPolicy;
    }

    /**
     * Set the current default policy
     * 
     * @param policy
     */
    public void setDefaultCommitPolicy(final CommitPolicy policy) {
        _defaultCommitPolicy = policy;
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
    public long getStartTimestamp() {
        return _startTimestamp;
    }

    /**
     * @return the internal timestamp at which transaction committed.
     */
    public long getCommitTimestamp() {
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
     * operations.
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
    void store(final Exchange exchange, final Key key, final Value value) throws PersistitException {
        if (_nestedDepth > 0) {
            checkPendingRollback();
            writeStoreRecordToJournal(treeHandle(exchange.getTree()), key, value);
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
    void remove(final Exchange exchange, final Key key1, final Key key2) throws PersistitException {
        if (_nestedDepth > 0) {
            checkPendingRollback();
            writeDeleteRecordToJournal(treeHandle(exchange.getTree()), key1, key2);
        }
    }

    /**
     * Record a tree delete operation
     * 
     * @param exchange
     * @throws PersistitException
     */
    void removeTree(final Exchange exchange) throws PersistitException {
        if (_nestedDepth > 0) {
            checkPendingRollback();
            writeDeleteTreeToJournal(treeHandle(exchange.getTree()));
        }
    }

    synchronized private void prepare(final int recordSize) throws PersistitException {
        if (recordSize > _buffer.remaining()) {
            flushTransactionBuffer(true);
        }
        if (recordSize > _buffer.remaining()) {
            throw new IllegalStateException("Record size " + recordSize + " is too long for Transaction buffer in "
                    + this);
        }
    }

    synchronized boolean flushTransactionBuffer(final boolean chain) throws PersistitException {
        boolean didWrite = false;
        if (_buffer.position() > 0 || _previousJournalAddress != 0) {
            final long previousJournalAddress = _persistit.getJournalManager().writeTransactionToJournal(_buffer,
                    _startTimestamp, _commitTimestamp, _previousJournalAddress);
            _buffer.clear();
            didWrite = true;
            if (chain) {
                _previousJournalAddress = previousJournalAddress;
            } else {
                _previousJournalAddress = 0;
            }
        }
        return didWrite;
    }

    synchronized void flushOnCheckpoint(final long timestamp) throws PersistitException {
        if (_startTimestamp > 0 && _startTimestamp < timestamp && _commitTimestamp == 0 && _buffer.position() > 0) {
            sequence(COMMIT_FLUSH_B);

            _previousJournalAddress = _persistit.getJournalManager().writeTransactionToJournal(_buffer,
                    _startTimestamp, 0, _previousJournalAddress);
            _buffer.clear();
        }
    }

    synchronized void writeStoreRecordToJournal(final int treeHandle, final Key key, final Value value)
            throws PersistitException {
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

    synchronized void writeDeleteRecordToJournal(final int treeHandle, final Key key1, final Key key2)
            throws PersistitException {
        final int elisionCount = key2.firstUniqueByteIndex(key1);
        final int recordSize = DR.OVERHEAD + key1.getEncodedSize() + key2.getEncodedSize() - elisionCount;
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

    synchronized void writeDeleteTreeToJournal(final int treeHandle) throws PersistitException {
        prepare(DT.OVERHEAD);
        JournalRecord.putLength(_buffer, DT.OVERHEAD);
        DT.putType(_buffer);
        DT.putTreeHandle(_buffer, treeHandle);
        _buffer.position(_buffer.position() + DT.OVERHEAD);
    }

    synchronized void writeDeltaToJournal(final Delta delta) throws PersistitException {
        final int treeHandle = treeHandle(delta.getAccumulator().getTree());
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
        final TransactionStatus ts = _transactionStatus;
        if (_nestedDepth > 0 && ts != null && ts.getTs() == _startTimestamp) {
            return ts;
        } else {
            throw new IllegalArgumentException("Transaction not in scope " + this);
        }
    }

    /**
     * @return the current step index.
     */
    public int getStep() {
        return _step;
    }

    /**
     * Set the current step index. Must be in the range [0, 99].
     * <p>
     * Also see {@link #incrementStep()} for step semantics.
     * </p>
     * 
     * @param step
     *            New step index value.
     * @return Previous step value.
     */
    public int setStep(final int step) {
        checkPendingRollback();
        checkStepRange(step);
        final int previous = _step;
        _step = step;
        return previous;
    }

    void checkActive() {
        if (!isActive()) {
            throw new IllegalStateException("No transaction scope: begin() has not been called in " + this);
        }
    }

    /**
     * Increment this transaction's current step index. For any given step,
     * values written by updates within this transaction are visible (within
     * this transaction) only if they were written with earlier or equal step
     * indexes. In other words, a transaction that writes an update at step N
     * can see the result of that update when reading the database and any step
     * &lt;= N. This mechanism helps solve the "Halloween" problem in which a
     * SELECT query producing values to UPDATE should not be able to read back
     * those update values.
     * 
     * @throws IllegalStateException
     *             if this method is called more than 99 times within the scope
     *             of one transaction.
     * @return The previous value of the step.
     */
    public int incrementStep() {
        return setStep(_step + 1);
    }

    private void checkStepRange(final int newStep) {
        if (newStep < 0) {
            throw new IllegalStateException(this + " cannot have a step of " + newStep + ", less than 0");
        }
        if (newStep > MAXIMUM_STEP) {
            throw new IllegalStateException(this + " cannot have a step of " + newStep + ", greater than maximum "
                    + MAXIMUM_STEP);
        }
    }

    private int treeHandle(final Tree tree) {
        final int treeHandle = tree.getHandle();
        assert treeHandle != 0 : "Undefined tree handle in " + tree;
        return treeHandle;
    }

    void addLockPage(final Long page, final int treeHandle) {
        _lockCleanupActions.add(new CleanupPruneAction(treeHandle, page));
    }

    void pruneLockPages() {
        if (_lockCleanupActions.isEmpty()) {
            return;
        }

        _persistit.getTransactionIndex().updateActiveTransactionCache(_commitTimestamp);
        List<CleanupAction> actions = new ArrayList<CleanupAction>(_lockCleanupActions);
        _lockCleanupActions.clear();

        while (!actions.isEmpty()) {
            final List<CleanupAction> consequentActions = new ArrayList<CleanupAction>();
            for (final CleanupAction cleanupAction : actions) {
                try {
                    cleanupAction.performAction(_persistit, consequentActions);
                } catch (final PersistitException pe) {
                    _persistit.getLogBase().pruneException.log(pe, this);
                }
                actions = consequentActions;
            }
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
