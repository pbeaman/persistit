/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit;

import static com.persistit.util.SequencerConstants.WRITE_WRITE_STORE_A;
import static com.persistit.util.SequencerConstants.WRITE_WRITE_STORE_B;
import static com.persistit.util.SequencerConstants.WRITE_WRITE_STORE_C;
import static com.persistit.util.SequencerConstants.WRITE_WRITE_STORE_SCHEDULE;
import static com.persistit.util.ThreadSequencer.addSchedules;
import static com.persistit.util.ThreadSequencer.array;
import static com.persistit.util.ThreadSequencer.describeHistory;
import static com.persistit.util.ThreadSequencer.describePartialOrdering;
import static com.persistit.util.ThreadSequencer.disableSequencer;
import static com.persistit.util.ThreadSequencer.enableSequencer;
import static com.persistit.util.ThreadSequencer.historyMeetsPartialOrdering;
import static com.persistit.util.ThreadSequencer.out;
import static com.persistit.util.ThreadSequencer.rawSequenceHistoryCopy;
import static com.persistit.util.ThreadSequencer.sequence;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.unit.UnitTestProperties;

public class Bug947182Test extends PersistitUnitTestCase {
    private static final String TREE_NAME = "Bug947182Test";
    private static final String KEY = "key1";
    private static final int A = WRITE_WRITE_STORE_A;
    private static final int B = WRITE_WRITE_STORE_B;
    private static final int C = WRITE_WRITE_STORE_C;

    public void tearDown() throws Exception {
        disableSequencer();
        super.tearDown();
    }

    /*
     * Bug seen many times but only in a large (100 terminal), long running TPCC
     * test as a hung thread inside of Buffer#repack() due to _alloc appearing
     * to have a zero-ed out header. This turned out to be a secondary issue to
     * the real problem.
     * 
     * The Exchange#storeInternal() method tracks a number of variables related
     * to long record chains. There are four in total: new and old for value
     * being stored and MVV itself. Logically, we don't need to keep new ones if
     * the store doesn't succeed and we can't free old ones until we have
     * committed the new. We also don't need create multiple long record chains
     * for the new one if we have to retry for any reason (e.g. split) as it
     * isn't visible to anyone else yet. Conversely, we do have to create
     * multiple long MVVs (if required) upon retry as the state of the MVV may
     * have changed in-between.
     * 
     * That is where the real bug was. The storeInternal() method would
     * incorrectly track the old long MVV pointer across retries. Imagine this
     * sequence: 1) txn1 stores value at A 2) txn2 tries to store at A but is
     * stopped due to WW 3) txn2 releases latches and sleeps for short period 4)
     * txn1 aborts 5) txn2 wakes, sees tx1n aborted, and retries 6) A is
     * converted to short record (pruned, short stored, etc) 7) txn2 re-acquires
     * page and successfully stores value at A
     * 
     * If the value during step 2 was a long MVV and was reduced to a short MVV
     * at step 6 (which would free the long record chain), then when txn2
     * completed at step 7 it would still be holding onto the old long record
     * pointer.
     * 
     * Ultimately, that resulted in the same page address being on the garbage
     * list twice. When the first came off and was being used for data and then
     * simultaneously being used for as the garbage root, we would see the end
     * result of _alloc appearing to be junk/zeroed out.
     */
    @Test
    public void testDoubleFreeOfLongMvvChain() throws InterruptedException {
        enableSequencer(true);
        addSchedules(WRITE_WRITE_STORE_SCHEDULE);

        final Semaphore firstStore = new Semaphore(0);
        final ConcurrentLinkedQueue<Throwable> throwableList = new ConcurrentLinkedQueue<Throwable>();

        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                Exchange ex = null;
                try {
                    Transaction txn = _persistit.getTransaction();
                    ex = getExchange(_persistit);
                    txn.begin();
                    try {
                        ex.clear().append(KEY);
                        storeLongMVV(ex);
                        firstStore.release();
                        sequence(WRITE_WRITE_STORE_B);
                        txn.rollback();
                    } finally {
                        txn.end();
                    }

                    /*
                     * Store and rollback again but this time with a short value
                     * so the long record chain gets de-allocated
                     */
                    txn.begin();
                    try {
                        ex.clear().append(KEY);
                        ex.getValue().clear().put("in-between value");
                        ex.store();
                        txn.rollback();
                    } finally {
                        txn.end();
                    }

                    sequence(WRITE_WRITE_STORE_C);
                } catch (Throwable t) {
                    throwableList.add(t);
                } finally {
                    if (ex != null) {
                        _persistit.releaseExchange(ex);
                    }
                }
            }
        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                Exchange ex = null;
                try {
                    if (!firstStore.tryAcquire(5, TimeUnit.SECONDS)) {
                        throw new Exception("Timed out waiting for first store to complete");
                    }
                    Transaction txn = _persistit.getTransaction();
                    ex = getExchange(_persistit);
                    txn.begin();
                    try {
                        ex.clear().append(KEY).getValue().clear().put(KEY);
                        ex.store();
                        txn.commit();
                    } finally {
                        txn.end();
                        _persistit.releaseExchange(ex);
                    }
                } catch (Throwable t) {
                    throwableList.add(t);
                } finally {
                    _persistit.releaseExchange(ex);
                }
            }
        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        assertEquals("Threads had no exceptions", "[]", throwableList.toString());

        final int[] history = rawSequenceHistoryCopy();
        final int[][] expectedOrdering = { array(A, B), array(out(B)), array(C), array(out(A), out(C)) };
        if (!historyMeetsPartialOrdering(history, expectedOrdering)) {
            assertEquals("History did not meet partial ordering", describePartialOrdering(expectedOrdering),
                    describeHistory(history));
        }
    }

    private static Exchange getExchange(Persistit persistit) throws PersistitException {
        return persistit.getExchange(UnitTestProperties.VOLUME_NAME, TREE_NAME, true);
    }

    private static void storeLongMVV(Exchange ex) throws PersistitException {
        final int size = ex.maxValueSize(ex.getKey().getEncodedSize()) - 1;
        StringBuilder builder = new StringBuilder(size);
        while (builder.length() < size) {
            builder.append("0123456789");
        }
        builder.setLength(size);
        ex.getValue().clear().put(builder.toString());
        ex.store();
        assertEquals("Did store long MVV", true, TestShim.isValueLongRecord(ex));
    }
}
