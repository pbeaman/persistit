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

import java.util.concurrent.atomic.AtomicBoolean;

import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import com.persistit.exception.TimeoutException;
import com.persistit.unit.PersistitUnitTestCase;

/**
 * In branch lp:~yshavit/akiban-server/postkarang_fix_MT_1a revno 835, I'm
 * getting many ArrayIndexOutOfBoundsExceptions with index = -1 from within
 * Transaction.end. It could be I'm misusing the Transaction pattern (need to
 * verify this), but if so, I'd expect a more informative exception; and if not,
 * well, then this is an issue.
 * 
 * Oddly enough, this only seems to happen when a breakpoint gets triggered. I
 * put in an
 * "if exception instanceof ArrayIndexOutOfBoundsException, print trace" and it
 * only happened once the breakpoint got triggered. But I don't know if that's
 * because the exception only got thrown at that point, or if System.err was
 * just not being printed until then. On the one hand, I am calling
 * System.err.flush() -- but on the other hand, if I run this not in debugger,
 * the stack trace appears as soon as I do a thread dump -- so I guess something
 * is just getting spinlocked out of having a chance to print the message.
 * 
 * This is consistently reproduceable.
 * 
 * Here's the stack trace:
 * 
 * java.lang.ArrayIndexOutOfBoundsException: -1 at
 * java.util.ArrayList.get(ArrayList.java:324) at
 * com.persistit.LockManager$ResourceTracker.unregister(LockManager.java:58) at
 * com.persistit.LockManager.unregister(LockManager.java:131) at
 * com.persistit.SharedResource.release(SharedResource.java:335) at
 * com.persistit.Transaction.end(Transaction.java:747) at
 * com.akiban.server.service
 * .dxl.BasicDMLFunctions.scanSome(BasicDMLFunctions.java:394) at
 * com.akiban.server
 * .service.dxl.BasicDMLFunctions.scanSome(BasicDMLFunctions.java:328) at
 * com.akiban.server.service.dxl.HookableDMLFunctions.scanSome(
 * 
 * 
 * -----
 * 
 * Great catch. This is indeed the symptom of two bugs, one serious, in
 * Persistit. The not-so-serious bug is that LockManager should range-check the
 * index value and emit a more helpful debug message. The more serious bug is
 * that in Transaction.begin(), there's a call to claim a SharedResource named
 * TransactionResourceA. Informally this is the "big fat global lock" I have
 * spoken about. That call to to claim() has a timeout of 30 seconds. Amazingly,
 * if the time expires, the transaction simply motors on without it. The AIOOBE
 * exception is triggered with the call to end() on that transaction attempts to
 * release the claim on TransactionResourceA and finds it's not there.
 * 
 * The transaction code will be entirely replaced in Kinu, but for now I will
 * patch around the timeout issue. I will lump both fixes under this bug number,
 * since it really covers both issues well.
 * 
 * Also, it turns out this bug was discovered due to an application logic error
 * in which a thread called begin() and then never called end(). That caused the
 * timeout in the first place because no other transaction could then get the
 * TransactionResourceA object in writer mode.
 * 
 * 
 */
public class Bug780811Test extends PersistitUnitTestCase {

    private final AtomicBoolean done = new AtomicBoolean();
    
    @Override
    public void runAllTests() throws Exception {
        // TODO Auto-generated method stub

    }

    public void testTransactionTimeout() throws Exception {
        final Persistit db = _persistit;

        final Thread thread = new Thread(new Runnable() {
            public void run() {
                final Transaction t = db.getTransaction();
                t.setPessimisticRetryThreshold(0);
                try {
                    t.begin();
                    try {
                        for (int i = 0; !done.get() && i < 1000; i++) {
                            Thread.sleep(50);
                        }
                    } catch (InterruptedException e) {

                    }
                    t.commit();
                    t.end();
                } catch (PersistitException e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
        
        Thread.sleep(1000);
        Transaction t = db.getTransaction();
        boolean okay = false;
        Exchange ex = db.getExchange("persistit", "Bug780811Test", true);
        try {
            t.begin();
            for (int i = 0; i < 10; i++) {
                ex.clear().append(i).store();
            }
            t.commit();
        } catch (TimeoutException e) {
            okay = true;
        } finally {
            if (!okay) {
                t.end();
            }
        }
        done.set(true);
        thread.join();
        assertTrue(okay);
    }

}
