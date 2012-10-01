/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import org.junit.Test;

import com.persistit.JournalRecord.JE;
import com.persistit.Transaction.CommitPolicy;
import com.persistit.exception.CorruptJournalException;
import com.persistit.exception.CorruptVolumeException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.unit.UnitTestProperties;

public class Bug1041003Test extends PersistitUnitTestCase {

    final static int BLOCKSIZE = 10000000;

    /*
     * This class needs to be in com.persistit rather than com.persistit.unit
     * because it uses some package- private methods in Persistit.
     */

    private final String _volumeName = "persistit";

    @Override
    protected Properties getProperties(final boolean cleanup) {
        final Properties p = UnitTestProperties.getProperties(cleanup);
        p.setProperty("journalsize", Integer.toString(BLOCKSIZE));
        return p;
    }

    private ErrorInjectingFileChannel errorInjectingChannel(final FileChannel channel) {
        final ErrorInjectingFileChannel eimfc = new ErrorInjectingFileChannel();
        ((MediatedFileChannel) channel).injectChannelForTests(eimfc);
        return eimfc;
    }

    /**
     * Simulate IOException on attempt to append to the journal. This simulates
     * bug #878346. Sets an injected IOException on journal file .000000000001
     * then stores a bunch of data until a failure occurs. Clears the injected
     * error, runs one more transaction and then checks the resulting database
     * state for correctness.
     * 
     * @throws Exception
     */
    @Test
    public void leaveTransactionBufferFlipped() throws Exception {
        /*
         * Need first journal file almost full so that an attempt to write a
         * transaction will force a rollover.
         */
        final Exchange exchange = _persistit.getExchange(_volumeName, "Bug1041003Test", true);
        _persistit.flush();
        final JournalManager jman = _persistit.getJournalManager();
        ByteBuffer bb = ByteBuffer.allocate(BLOCKSIZE);
        long size = BLOCKSIZE - JE.OVERHEAD - jman.getCurrentAddress() - 1;
        bb.position((int) (size - JournalRecord.TX.OVERHEAD));
        jman.writeTransactionToJournal(bb, 1, 2, 0);
        final Transaction txn = _persistit.getTransaction();
        final ErrorInjectingFileChannel eifc = errorInjectingChannel(_persistit.getJournalManager().getFileChannel(
                BLOCKSIZE));
        /*
         * Will cause any attempt to write into the second journal file to fail.
         */
        eifc.injectTestIOException(new IOException("injected"), "w");
        try {
            txn.begin();
            try {
                exchange.getValue().put(RED_FOX);
                exchange.to(1).store();
                txn.commit(CommitPolicy.HARD);
            } finally {
                txn.end();
            }
        } catch (final PersistitIOException e) {
            if (e.getMessage().contains("injected")) {
                System.out.println("Expected: " + e);
            } else {
                throw e;
            }
        }
        /*
         * Now remove the disk full condition. Transaction should now succeed.
         */
        eifc.injectTestIOException(null, "w");
        txn.begin();
        try {
            exchange.getValue().put(RED_FOX + RED_FOX);
            /*
             * Bug 1041003 causes the following line to throw an
             * IllegalStateException.
             */
            exchange.to(1).store();
            txn.commit(CommitPolicy.HARD);
        } catch (IllegalStateException e) {
            fail("Bug 1041003 strikes: " + e);
        } finally {
            txn.end();
        }
    }

}
