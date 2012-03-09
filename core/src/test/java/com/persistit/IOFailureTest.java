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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import com.persistit.exception.CorruptJournalException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.unit.UnitTestProperties;

public class IOFailureTest extends PersistitUnitTestCase {

    final static int BLOCKSIZE = 10000000;

    /*
     * This class needs to be in com.persistit rather than com.persistit.unit
     * because it uses some package- private methods in Persistit.
     */

    private String _volumeName = "persistit";

    protected Properties getProperties(final boolean cleanup) {
        final Properties p = UnitTestProperties.getProperties(cleanup);
        p.setProperty("journalsize", Integer.toString(BLOCKSIZE));
        return p;
    }

    private ErrorInjectingFileChannel errorInjectingChannel(final FileChannel channel) {
        final ErrorInjectingFileChannel eimfc = new ErrorInjectingFileChannel();
        ((MediatedFileChannel)channel).setErrorInjectingChannelForTests(eimfc);
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
    public void testJournalUnwritable() throws Exception {
        final Transaction txn = _persistit.getTransaction();
        final ErrorInjectingFileChannel eifc = errorInjectingChannel( _persistit.getJournalManager().getFileChannel(BLOCKSIZE));
        /*
         * Will cause any attempt to write into the second journal file to fail.
         */
        eifc.injectDiskFullLimit(100000);
        int at = 0;
        for (;; at++) {
            try {
                txn.begin();
                try {
                    store1(at);
                    txn.commit(true);
                } finally {
                    txn.end();
                }
            } catch (PersistitIOException e) {
                if (e.getMessage().contains("Disk Full")) {
                    break;
                    // okay
                } else {
                    throw e;
                }
            }
        }
        Thread.sleep(1000);
        /*
         * Now remove the disk full condition. Transaction should now succeed.
         */
        eifc.injectDiskFullLimit(Long.MAX_VALUE);
        txn.begin();
        try {
            store1(at);
            txn.commit(true);
        } finally {
            txn.end();
        }
        final Exchange exchange = _persistit.getExchange(_volumeName, "IOFailureTest", false);
        for (int i = 0; i < at + 5; i++) {
            int count = 0;
            exchange.clear().append(i).append(Key.BEFORE);
            while (exchange.next()) {
                count++;
            }
            assertEquals("Incorrect number of keys in tree", i <= at ? 5000 : 0, count);
        }
    }

    /**
     * Simulate IOException on attempt to read from journal during normal
     * operation. The test sets up the journal is the sole source from which an
     * attempt to read data can pull pages. Then it simulates a disk read
     * failure, proves that the client receives an appropriate Exception, clears
     * the failure condition, and finally proves that the client succeeds.
     * 
     * @throws Exception
     */
    public void testJournalUnreadable() throws Exception {
        final String reason = "Read Failure";
        store1(0);
        final Volume volume = _persistit.getVolume(_volumeName);
        /*
         * Remove all pages from the pool
         */
        volume.getPool().flush(Long.MAX_VALUE);
        volume.getPool().invalidate(volume);
        /*
         * Make sure the pages can't be read back from the journal's write
         * buffer
         */
        _persistit.getJournalManager().force();

        Exchange ex = _persistit.getExchange(_volumeName, "IOFailureTest", false);

        final ErrorInjectingFileChannel eifc = errorInjectingChannel(_persistit.getJournalManager().getFileChannel(0));
        eifc.injectTestIOException(new IOException(reason), "r");
        try {
            ex.clear().append(0).next();
            fail("Should have gotten an IOException");
        } catch (PersistitIOException ioe) {
            assertEquals("Incorrect Exception thrown: " + ioe, reason, ioe.getMessage());
        }
        eifc.injectTestIOException(null, "");
        assertEquals("Expected key not found", true, ex.clear().append(Key.BEFORE).next());

        store1(1);

        /*
         * Remove all pages from the pool
         */
        volume.getPool().flush(Long.MAX_VALUE);
        /*
         * Push all pages back to the Volume file.
         */
        /*
         * Make sure the pages can't be read back from the journal's write
         * buffer
         */
        _persistit.getJournalManager().force();
        /*
         * Inject IOException on journal reads. This should stall the copier
         * process until the error condition is cleared.
         */
        final ErrorInjectingFileChannel mfcj = errorInjectingChannel(_persistit.getJournalManager().getFileChannel(0));
        mfcj.injectTestIOException(new IOException(reason), "r");
        final long start = System.currentTimeMillis();
        new Timer().schedule(new TimerTask() {
            public void run() {
                mfcj.injectTestIOException(null, "");
            }
        }, 2000);
        copyBackEventuallySucceeds(start, reason);
    }

    /**
     * Simulate IOException on reading from a Volume. Inserts a bunch of data,
     * runs copyBack to get it all written to the volume, then clears the buffer
     * pool and attempts to read it back.
     * 
     * @throws Exception
     */
    public void testVolumeUnreadable() throws Exception {
        final String reason = "Read Failure";
        store1(0);

        Exchange ex = _persistit.getExchange(_volumeName, "IOFailureTest", false);
        final Volume volume = _persistit.getVolume(_volumeName);
        /*
         * Remove all pages from the pool
         */
        volume.getPool().flush(Long.MAX_VALUE);

        /*
         * Make sure the pages can't be read back from the journal's write
         * buffer
         */
        _persistit.getJournalManager().force();
        final ErrorInjectingFileChannel mfcj = errorInjectingChannel(_persistit.getJournalManager().getFileChannel(0));
        /*
         * Push all pages back to the Volume file.
         */
        _persistit.copyBackPages();

        volume.getPool().invalidate(volume);
        ex.initCache();
        /*
         * Inject IOException on journal reads. This prevents pages from being
         * read back from the journal. However, because copyBackPages has
         * written all page images back to the Volume, no reads against the
         * journal should occur.
         */
        mfcj.injectTestIOException(new IOException(reason), "r");
        /*
         * This should succeed because the journal has been fully copied,
         * therefore reads are coming from the Volume file itself.
         */
        assertEquals(true, ex.clear().append(Key.BEFORE).next());
        /*
         * Clear out the buffer pool again.
         */
        volume.getPool().invalidate(volume);
        ex.initCache();

        final ErrorInjectingFileChannel mfcv = errorInjectingChannel(volume.getStorage().getChannel());
        mfcv.injectTestIOException(new IOException(reason), "r");

        try {
            ex.clear().append(0).next();
            fail("Should have gotten an IOException");
        } catch (PersistitIOException ioe) {
            assertEquals("Incorrect Exception thrown: " + ioe, reason, ioe.getMessage());
        }
        mfcv.injectTestIOException(null, "");
        assertEquals("Expected key not found", true, ex.clear().append(Key.BEFORE).next());

    }

    public void testVolumeUnwritable() throws Exception {
        final String reason = "Write Failure";
        final Volume volume = _persistit.getVolume(_volumeName);
        final ErrorInjectingFileChannel mfcv = errorInjectingChannel(volume.getStorage().getChannel());
        mfcv.injectTestIOException(new IOException(reason), "w");
        /*
         * Should succeed since writes to volume are delayed
         */
        store1(0);
        /*
         * Remove all pages from the pool
         */
        volume.getPool().flush(Long.MAX_VALUE);
        _persistit.getJournalManager().force();
        /*
         * This method should stall until we clear the injected IOException
         */
        final long start = System.currentTimeMillis();
        new Timer().schedule(new TimerTask() {
            public void run() {
                mfcv.injectTestIOException(null, "");
            }
        }, 2000);
        copyBackEventuallySucceeds(start, reason);
        volume.getPool().invalidate(volume);
    }
    
    public void testJournalEOFonRecovery() throws Exception {
        final Properties properties = _persistit.getProperties();
        final JournalManager jman = _persistit.getJournalManager();
        Exchange exchange = _persistit.getExchange(_volumeName, "RecoveryTest", true);
        exchange.getValue().put(RED_FOX);
        int count = 0;
        long checkpointAddr = 0;
        for (; jman.getCurrentAddress() < jman.getBlockSize() * 1.25;) {
            if (jman.getCurrentAddress() - checkpointAddr > jman.getBlockSize() * 0.8) {
                _persistit.checkpoint();
                checkpointAddr = jman.getCurrentAddress();
            }
            exchange.to(count).store();
            count++;
        }
        for (int i = 0; i < count + 100; i++) {
            assertEquals(i < count, exchange.to(i).isValueDefined());
        }
        final long currentAddress = jman.getCurrentAddress();
        _persistit.close();

        final File file0 = jman.addressToFile(currentAddress - jman.getBlockSize());
        final FileChannel channel0 = new RandomAccessFile(file0, "rw").getChannel();
        final long size0 = channel0.size();
        channel0.truncate(100);

        final File file1 = jman.addressToFile(currentAddress);
        final FileChannel channel1 = new RandomAccessFile(file1, "rw").getChannel();
        final long size1 = channel1.size();
        channel1.truncate(100);

        channel1.close();

        _persistit = new Persistit();
        try {
            _persistit.initialize(properties);
        } catch (CorruptJournalException cje) {
            // expected
        }
        file1.delete();

        _persistit = new Persistit();
        _persistit.initialize(properties);

    }

    private void store1(final int at) throws PersistitException {
        final Exchange exchange = _persistit.getExchange(_volumeName, "IOFailureTest", true);
        final StringBuilder sb = new StringBuilder();

        for (int i = 1; i <= 5000; i++) {
            sb.setLength(0);
            sb.append((char) (i / 20 + 64));
            sb.append((char) (i % 20 + 64));
            exchange.clear().append(at).append(sb);
            exchange.getValue().put("Record #" + at + "_" + i);
            exchange.store();
        }
    }
    
    private void copyBackEventuallySucceeds(final long start, final String reason) throws Exception {
        final long expires = System.currentTimeMillis() + 10000;
        boolean done = false;
        while (System.currentTimeMillis() < expires) {
            try {
                _persistit.copyBackPages();
                done = true;
                break;
            } catch (PersistitIOException ioe) {
                assertEquals("Incorrect Exception thrown: " + ioe, reason, ioe.getMessage());
            }
        }
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(done ? "Copyback took too long" : "Copyback did not complete", done && elapsed >= 2000);
        assertEquals("Copyback did not move base address to end of journal", _persistit.getJournalManager().getCurrentAddress(), _persistit.getJournalManager()
                .getBaseAddress());
    }

}
