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

package com.persistit.bug;

import java.util.Timer;
import java.util.TimerTask;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.TestShim;
import com.persistit.Transaction;
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
    final static long TIME = 30L * 1000L * 1000L * 1000L;

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
        while (errors == 0 && System.nanoTime() - start < TIME) {
            try {
                txn.begin();
                try {
                    ex.getValue().put(RED_FOX);
                    for (int i = 0; i < 10000; i++) {
                        ex.to(i).store();
                    }
                    txn.commit(true); // force disk I/O
                } catch (PersistitInterruptedException e) {
                    // clear interrupted flag
                    Thread.interrupted();
                } catch (PersistitIOException e) {
                    if (TestShim.isIOInterruptedException(e.getCause())) {
                        // ignore
                    }
                } catch (Exception e) {
                    // of interest
                    e.printStackTrace();
                    errors++;
                    break;
                }
            } finally {
                txn.end();
            }
        }
        timer.cancel();
        assertEquals(0, errors);
    }

    @Override
    public void runAllTests() throws Exception {
    }

}
