/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit;

import static com.persistit.unit.UnitTestProperties.VOLUME_NAME;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.junit.Test;

import com.persistit.exception.PersistitException;

/**
 * https://bugs.launchpad.net/akiban-persistit/+bug/1065677
 * 
 * Doing a long delete in OLB_latest dataset, my Mac crashed, taking my VM with
 * it. The Vm was running akiban. It was via a local psql client.
 * 
 * <pre>
 * INFO 15:14:05,258 Starting services.
 * Exception in thread "main" java.lang.AssertionError
 *  at com.persistit.JournalManager$PageNode.setPrevious(JournalManager.java:1918)
 *  at com.persistit.RecoveryManager.scanLoadPageMap(RecoveryManager.java:1150)
 *  at com.persistit.RecoveryManager.scanOneRecord(RecoveryManager.java:937)
 *  at com.persistit.RecoveryManager.findAndValidateKeystone(RecoveryManager.java:784)
 *  at com.persistit.RecoveryManager.buildRecoveryPlan(RecoveryManager.java:
 * </pre>
 * 
 */

public class Bug1065677Test extends PersistitUnitTestCase {

    private final static String TREE_NAME = "Bug1065677Test";

    private Exchange getExchange() throws PersistitException {
        return _persistit.getExchange(VOLUME_NAME, TREE_NAME, true);
    }

    /**
     * This method tries to recreate the state at which the journal files in bug
     * 1065677 arrived. Plan:
     * 
     * 1. Write a transaction that updates multiple pages.
     * 
     * 2. Flush all buffers so that there are pages to recover and then crash
     * Persistit so there is no checkpoint.
     * 
     * 3. Restart Persistit, but advance the system timestamp to simulate
     * somewhat chaotic ordering of page writes created by reapplying a huge
     * delete operation in the transaction.
     * 
     * 4. Crash once again. This will leave pages from the original epoch in the
     * page map and will add versions of those pages with larger timestamps.
     * This crash simulates the JVM being killed during recovery processing. In
     * the actual case it appears there is no post-recovery checkpoint, merely
     * lots of dirty pages. To simulate this outcome we truncate about 50K of
     * bytes form the end of the journal file as a surrogate for the system
     * having not completed recovery.
     * 
     * 5. Restart the system once again. Now do some normal processing work,
     * simulated here by adding another transaction.
     * 
     * 6. Magic happens here: we now perform a rollover, which writes both the
     * branch map and the page map into a single PM record. Because the PM
     * record is written by two separate loops, some page P found in both the
     * branch map and the page map is written into two separate PM sub-records.
     * As it turns out, the version in the branch map has a smaller timestamp
     * than the one in the page map. This sets up the AssertionError.
     * 
     * 7. Restart Persistit to exploit the bug during scanLoadPageMap.
     * 
     * @throws Exception
     */
    @Test
    public void breakTimestampSequence() throws Exception {
        doTransaction();

        final Configuration config = _persistit.getConfiguration();
        final long lastTimestamp = _persistit.getCurrentTimestamp();
        _persistit.flush();
        _persistit.crash();

        _persistit = new Persistit();
        _persistit.getTimestampAllocator().updateTimestamp(lastTimestamp + 1000);
        _persistit.setConfiguration(config);
        _persistit.initialize();
        _persistit.crash();
        truncate();

        config.setAppendOnly(true);
        _persistit = new Persistit(config);

        doTransaction();

        final JournalManager jman = _persistit.getJournalManager();
        jman.rollover();
        _persistit.close();

        _persistit = new Persistit(config);
        _persistit.close();

    }

    private void doTransaction() throws Exception {
        final Exchange ex = getExchange();
        final Transaction txn = ex.getTransaction();
        txn.begin();
        ex.getValue().put(RED_FOX);
        for (int i = 0; i < 10000; i++) {
            ex.to(i).store();
        }
        txn.commit();
        txn.end();
    }

    private void truncate() throws IOException {
        final JournalManager jman = _persistit.getJournalManager();
        final long lastAddress = jman.getCurrentAddress();
        final File file = jman.addressToFile(lastAddress);
        final RandomAccessFile raf = new RandomAccessFile(file, "rw");
        try {
            final long length = raf.length();
            raf.setLength(length - 50000);
        } finally {
            raf.close();
        }
    }
}
