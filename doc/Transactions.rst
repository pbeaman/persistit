.. _Transactions:

Transactions
============

Akiban Persistit supports transactions with multi-version concurrency control (MVCC) using a protocol called Snapshot Isolation (SI). An application calls ``com.persistit.Transaction#begin``, ``com.persistit.Transaction#commit``, ``com.persistit.Transaction#rollback`` and ``com.persistit.Transaction#end`` methods to control the current transaction scope explicitly.  A Transaction allows an application to execute multiple database operations in an atomic, consistent, isolated and durable (ACID) manner.

Applications manage transactions through an instance of a ``com.persistit.Transaction`` object. ``Transaction`` does not represent a single transaction, but is instead a context in which a thread may perform many sequential transactions. The general pattern is that the application gets the current thread’s ``Transaction`` instance, calls its ``begin`` method, performs work, calls ``commit`` and finally ``end``.  The thread uses the same ``Transaction`` instance repeatedly. Generally each thread has one ``Transaction`` that lasts for the entire life of the thread (but see com.persistit.Transaction for a mechanism that allows a transaction to be serviced by multiple threads). 

Using Transactions
------------------

The following code fragment performs two store operations within the scope of a transaction:

.. code-block:: java

  //
  // Get the transaction context for the current thread.
  //
  Transaction txn = myExchange.getTransaction();
  int remainingRetries = RETRY_COUNT;
  for (;;) {
      txn.begin();
      try {
          myExchange.getValue().put("First value");
          myExchange.clear().append(1).store();
          myExchange.getValue().put("Second value");
          myExchange.clear().append(2).store();
          // Required to commit the transaction
          txn.commit();
          break;
      } catch (RollbackException re) {
          // perform any special rollback handling
          // allow loop to repeat until commit succeeds or retries
          // too many times.
          if (--remainingRetries < 0) {
            throw new TransactionFailedException();
          }
      } catch (PersistitException pe) {
          // handle other Persistit exception
      } finally {
          // Required to end the scope of a transaction.
          txn.end();
      }
  }

This example catches ``com.persistit.exception.RollbackException`` which can be thrown by any Persistit operation within the scope of a transaction, including ``commit``. Any code explicitly running within the scope of a transaction should be designed to handle rollbacks.

This example also uses a *try/finally* block to ensure every call to ``begin`` has a matching call to ``end``. This code pattern is mandatory: it is critical to correct transaction nesting behavior.

One convenient way to do this is to encapsulate the logic of a transaction in an implementation of ``com.persisitit.TransactionRunnable`` interface. The ``com.persistit.Transaction#run`` method automatically provides logic to begin the transaction, execute the TransactionRunnable and commit the transaction, repeating the process until no rollback is thrown or a maximum retry count is reached. For example, the code fragment shown above can be rewritten as:

.. code-block:: java

  //
  // Get the transaction context for the current thread.
  //
  Transaction txn = myExchange.getTransaction();
  //
  // Perform the transaction with the following parameters:
  // - try to commit it up to 10 times
  // - delay 2 milliseconds before each retry
  // - use the group commit durability policy
  //
  txn.run(new TransactionRunnable() {
      public void run() throws PersistitException {
          myExchange.getValue().put("First value");
          myExchange.clear().append(1).store();
          myExchange.getValue().put("Second value");
          myExchange.clear().append(2).store();
      }
  }, 10, 2, CommitPolicy.GROUP);

Mixing Transactional and Non-Transactional Operations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Database operations running outside the scope of an explicitly defined transaction are never subject to rollback and therefore do not require retry logic. However, such operations are also not guaranteed to be durable in the event of a system crash. Further, such operations are not isolated. Read operations performed outside of a transaction can read uncommitted updates, and updates performed outside of a transaction are visible within transactions. In other words, non-transactional reads and writes may break both the durability and isolation of concurrently executing transactions.  Therefore it is strongly recommended that in an application that relies on transactions, all interactions with the database should use transactions. 

Optimistic Transaction Scheduling
---------------------------------

To achieve high performance and scalability, Persistit supports an optimistic transaction scheduling protocol called MVCC with `Snapshot Isolation <http://wikipedia.org/wiki/Snapshot_isolation>`_. Under this protocol multiple threads are permitted to execute transactions at full speed without blocking until a potentially inconsistent state is recognized. At that point a transaction suspected of causing the inconsistent state is automatically forced to roll back.

Optimistic scheduling works because transactions usually do not collide, especially when individual database operations are fast, and so in practice transactions are seldom rolled back. But because any transaction may be rolled back at any point, applications must be designed carefully to avoid unintended side-effects. For example, a transaction should never perform non-repeatable or externally visible operations such as file or network I/O within its scope.

Snapshot Isolation
^^^^^^^^^^^^^^^^^^

Persistit schedules concurrently executing transactions optimistically, without locking any database records. Instead, Persistit uses the well-known Snapshot Isolation protocol to achieve atomicity and isolation. While transactions are modifying data, Persistit maintains multiple versions of values being modified. Each version is labeled with the commit timestamp of the transaction that modified it. Whenever a transaction reads a value that has been modified by other transactions, it gets the latest version that was committed before its own start timestamp. In other words, all read operations are performed as if from a "snapshot" of the state of the database made at the transaction's start timestamp - hence the name "Snapshot Isolation."

.. _Pruning:

Pruning 
^^^^^^^

Given that all updates written through transactions are created as versions within the MVCC scheme, a large number of versions can accumulate over time. Persistit reduces this proliferation through an activity called "pruning." Pruning resolves the final state of each version by removing any versions created by aborted transactions and removing obsolete versions no longer needed by other transactions. If a value contains only one version and the commit timestamp of the transaction that created it is before the start of any currently running transaction, that value is called *primordial*. The goal of pruning is to reduce most or all values in the database to their primordial states because updating and reading primordial values is more efficient than than managing multiple version values. Pruning happens automatically and is generally not visible to the application.

Rollbacks
^^^^^^^^^

Usually Snapshot Isolation allows concurrent transactions to commit without interference but this is not always the case. Two concurrent transactions that attempt to modify the same Persistit key/value pair before they commit are said to have a "write-write dependency". To avoid anomalous results one of them must abort, rolling back any other updates it may also have created, and retry. Persistit implements a "first updater wins" policy in which if two transactions attempt to update the same record, the first transaction "wins" by being allowed to continue, while the second transaction "loses" and is required to abort.

Once a transaction has aborted, any subsequent database operation it attempts throws a ``RollbackException``. Application code should catch and handle this Exception. Usually the correct and desired behavior is simply to retry the transaction as shown in the code samples above.

A transaction can also voluntarily roll back. For example, transaction logic could detect an error condition that it chooses to handle by throwing an exception back to the application. In this case the transaction should invoke the ``rollback`` method to explicitly declare its intent to abort the transaction.

Read-Only Transactions
^^^^^^^^^^^^^^^^^^^^^^
 
Under Snapshot Isolation, transactions that read but do not modify data cannot generate any write-write dependencies and are therefore not subject to  being rolled back because of the actions of other transactions. However, even though it modifies no data, a long-running read-only transaction can force Persistit to retain old value versions from other transactions for its duration in order to provide a snapshot view. This behavior can cause congestion and performance degradation by preventing very old values from being pruned. The degree to which this is a problem depends on the volume of update transactions being processed and the duration of long-running transactions.

Snapshot Isolation is not Serializable
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It is well-known that transactions executing under SI are not necessarily serializable. Under SI, so-called *write-skew* anomalies can happen with transactions that have certain kinds of interactions.  Write-skew can be avoided by (a) explicit application-level locking or (b) structuring transactions to add write-write dependencies where write-skew otherwise could occur.

Note that many common transaction patterns, including those defined by the TPC-C benchmark, do not experience write-skew and therefore *are* serializable under SI.

In the unusual case where transactions are susceptible to write skew, the Exchange#lock method offers a way for applications to create a write-write dependency explicitly. This mechanism provides an efficient mechanism for ensuring serializable behavior.

Durability Options: ``CommitPolicy``
------------------------------------

Persistit provides three policies that determine the durability of a transaction after it has executed the ``com.persistit.Transaction#commit`` method. These are:

  ``HARD``
      The ``commit`` method does not return until all updates created by the transaction have been written to non-volatile storage (e.g., disk storage).
  ``GROUP``
      The ``commit`` method does not return until all updates created by the transaction have been written to non-volatile storage. In addition, the committing 
      transaction waits briefly in an attempt to recruit other transactions running in other threads to write their updates with the same physical I/O operation.
  ``SOFT``
      The ``commit`` method returns *before* the updates have been recorded on non-volatile storage. Persistit attempts to write them within 100 milliseconds, but 
      this interval is not guaranteed.

You can specify a default policy in the Persistit initialization properties using the ``txnpolicy`` property or under program control using ``com.persistit.Persistit#setDefaultTransactionCommitPolicy``. The default policy applies whenever the application calls the ``commit()`` method. You can override the default policy using ``commit(CommitPolicy)``.

HARD and GROUP ensure each transaction is written durably to non-volatile storage before the ``commit`` method returns. The difference is that GROUP can improve throughput in multi-threaded applications because the average number of I/O operations needed to commit *N* transactions can be smaller than *N*. However, for one or a small number of concurrent threads, GROUP reduces throughput because it works by introducing a delay to allow other concurrent transactions to commit within a single I/O operation.

SOFT commits are generally much faster than HARD or GROUP commits, especially for single-threaded applications, because the results of numerous transactions committed from a single thread can be aggregated and written to disk in a single I/O operation. However, transactions written with the SOFT commit policy are not immediately durable and it is possible that the recovered state of a database will be missing transactions that reported they were committed shortly before a crash.

For SOFT commits, the state of the database after restart is such that for any committed transaction T, either all or none of its modifications will be present in the recovered database. Further, if a transaction T2 reads or updates data that was written by any other transaction T1, and if T2 is present in the recovered database, then so is T1. Any transaction that was in progress, but had not been committed at the time of the failure, is guaranteed not to be present in the recovered database. SOFT commits are designed to be durable within 100 milliseconds after ``commit`` returns. However, this interval is determined by computing the average duration of recent I/O operations to predict the completion time of the I/O that will write the transaction to disk, and therefore the interval cannot be guaranteed.

Nested Transactions
-------------------

A nested transaction occurs when code that is already executing within the scope of a transaction executes the ``begin`` method to start a new transaction. This might happen, for example, if an application’s transaction logic calls a method that also uses transactions. In this case, the commit processing of the inner transaction scope is deferred until the outermost transaction commits. At that point, all the updates performed within the inner and outer transaction scopes are committed to the database. Similarly, a rollback initiated by the inner transaction causes both it and the outermost transaction to roll back.

Accumulators
------------

Consider an application in which concurrently running transactions share a counter. For example, suppose each transaction is responsible for allocating a unique integer as a primary key for a database record. One way to do this would be to store the counter in a Persistit key/value pair, reading the value at the start of each transaction and committing an update at the end.

The problem with this approach is that under SI, concurrent transactions running in a multi-threaded application would experience very frequent write-write dependencies on the counter value; in fact, the only way to complete any transactions would be serially, one at a time.

Persistit provides the ``com.persistit.Accumulator`` class to avoid this problem.  An accumulator is designed to manage contributions from multiple concurrent transactions without causing write-write dependencies. Accumulators are durable in the sense that each transaction’s contribution is made durable with the transaction itself, and Persistit automatically recovers a correct state for each Accumulator in the event of a system crash.

There are four types of accumulator in Persistit. Each is a concrete subclass of the abstract ``com.persistit.Accumulator`` class:

  ``SUM``
      Tallies a count or sum of contributions by each transaction
  ``MIN``
      Finds the minimum value contributed by all transactions
  ``MAX``
      Finds the maximum value contributed by all transactions
  ``SEQ``
      Special case of the SUM accumulator used to generate sequence numbers

Accumulator instances are associated with a ``com.persistit.Tree``.  Each ``Tree`` may have up to 64 accumulators. The following code fragment creates and/or acquires a ``SumAccumulator``, reads its snapshot value and then adds one to it:

.. code-block:: java

  final Exchange ex = _persistit.getExchange(volume, treeName, true);
  final Transaction txn = ex.getTransaction();
  txn.begin();
  try {
      final Accumulator acc =
          ex.getTree().getAccumulator(Accumulator.Type.SUM, 17);
      long snap = acc.getSnapshotValue(txn);
      acc.update(1, txn);
      txn.commit();
  } finally {
      txn.end();
  }

The value 17 is simply an arbitrary index number between 0 and 63, inclusive. The application is responsible for allocating and managing accumulator indexes.

The snapshot value of an accumulator obtained through ``com.persistit.Accumulator#getSnapshotValue()`` is the value computed from all updates contributed by transactions that had committed at the time the current transaction started, plus the transaction’s own as-yet uncommitted updates. In other words, the snapshot value of the accumulator is consistent with the snapshot view of all other data visible within the transaction.

An accumulator has two ways of accessing its accumulated value:

  ``getSnapshotValue()``
      Is a value computed from updates that were committed at the start of the current transaction. This method may be called only within the scope of a 
      Transaction.
  ``getLiveValue()``
      Is an ephemeral value reflecting all updates performed by all transactions, including concurrent and aborted transactions.

The snapshot value is a precise, consistent tally, while the live value is approximate. For a ``SumAccumulator``, ``MaxAccumulator`` or ``SeqAccumulator``, if all updates are have non-negative arguments, then the live value is always greater than or equal to the snapshot value.

SeqAccumulator
^^^^^^^^^^^^^^

The ``SeqAccumulator`` class has a special role in allocating unique identifier numbers, e.g., synthetic primary keys.  The goal of the ``SeqAccumulator`` is to ensure that every committed transaction has received a unique value integer in all circumstances, including after recovery from a crash. See ``com.persistit.Accumulator`` for details.
