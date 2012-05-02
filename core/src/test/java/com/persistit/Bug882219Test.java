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

package com.persistit.bug;

import static org.junit.Assert.assertEquals;

import java.io.InterruptedIOException;
import java.util.Timer;
import java.util.TimerTask;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Transaction;
import com.persistit.Transaction.CommitPolicy;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.unit.PersistitUnitTestCase;

/**
 * Interrupting a thread while it is performing a read or write closes the
 * underlying FileChannel. Subsequently Persistit emits errors such as the
 * following discovered on a DP site:
 * 
 * ERROR [JOURNAL_COPIER] 2011-10-26 12:38:08,904 Slf4jAdapter.java (line 98)
 * [JOURNAL_COPIER] ERROR com.persistit.exception.PersistitIOException:
 * java.nio.channels.ClosedChannelException
 * 
 * ERROR [JOURNAL_COPIER] 2011-10-26 12:38:18,909 Slf4jAdapter.java (line 98)
 * [JOURNAL_COPIER] ERROR Exception java.nio.channels.ClosedChannelException
 * while writing volume akiban_data(/var/lib/akiban/akiban_data.v01) page 53,248
 * 
 * ERROR [JOURNAL_COPIER] 2011-10-26 12:38:18,909 Slf4jAdapter.java (line 98)
 * [JOURNAL_COPIER] ERROR
 */

public class Bug882219Test extends PersistitUnitTestCase {

    // Thirty seconds
    final static long TIME = 20L * 1000L * 1000L * 1000L;

    // Flush caches:
    // sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
    //
    @Test
    public void testInterrupts() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "bug882219", true);
        final Thread foregroundThread = Thread.currentThread();

        Timer timer = new Timer("Interrupter");
        timer.schedule(new TimerTask() {
            public void run() {
                foregroundThread.interrupt();
            }
        }, 1000, 100);
        ex.removeAll();
        final Transaction txn = ex.getTransaction();
        final long start = System.nanoTime();
        int errors = 0;
        int commits = 0;
        int transactions = 0;
        try {
            while (errors == 0 && System.nanoTime() - start < TIME) {
                boolean began = false;
                try {
                    txn.begin();
                    began = true;
                    ex.getValue().put(RED_FOX);
                    for (int i = 0; i < 10000; i++) {
                        ex.to(i).store();
                    }
                    txn.commit(CommitPolicy.HARD); // force disk I/O
                    commits++;
                } catch (PersistitInterruptedException e) {
                    // clear interrupted flag and ignore
                    Thread.interrupted();
                } catch (PersistitIOException e) {
                    if (e.getCause() instanceof InterruptedIOException) {
                        // ignore
                    }
                } catch (Exception e) {
                    // of interest
                    e.printStackTrace();
                    errors++;
                    break;
                } finally {
                    if (began) {
                        txn.end();
                        transactions++;
                    }
                }
            }
        } finally {
            timer.cancel();
            // make sure interrupted state is cleared.
            Thread.interrupted();
        }
        System.out.printf("Transactions=%d  Commits=%d  Errors=%d", transactions, commits, errors);
        assertEquals(0, errors);
    }

    @Override
    public void runAllTests() throws Exception {
    }

}
