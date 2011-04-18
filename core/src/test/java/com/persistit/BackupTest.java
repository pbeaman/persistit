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
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.unit.UnitTestProperties;


public class BackupTest extends PersistitUnitTestCase {
    private final static int TRANSACTION_COUNT = 50000;
    
    protected Properties getProperties(final boolean cleanup) {
        return UnitTestProperties.getBiggerProperties(cleanup);
    }

    @Test
    public void testSimpleBackup() throws Exception {

        final PersistitMap<Integer, String> pmap1 = new PersistitMap<Integer, String>(
                _persistit.getExchange("persistit", "BackupTest", true));
        for (int index = 0; index < 50000; index++) {
            pmap1.put(new Integer(index), "This is the record for index="
                    + index);
        }

        final TreeMap<Integer, String> tmap = new TreeMap<Integer, String>(
                pmap1);
        final File file = File.createTempFile("backup", ".zip");
        file.deleteOnExit();
        final Backup backup1 = new Backup();
        backup1.setMessageStream(System.out);

        backup1.setPersistit(_persistit);
        backup1.setup(1, "backup file=" + file.getAbsolutePath(), "cli", 0, 5);
        backup1.setupTaskWithArgParser(new String[] {
                "file=" + file.getAbsolutePath(), "-z", "-c" });

        backup1.run();
        final Properties properties = _persistit.getProperties();
        _persistit.close();

        _persistit = new Persistit();
        final Backup backup2 = new Backup();
        backup2.setMessageStream(System.out);
        backup2.setPersistit(_persistit);
        backup2.doRestore(file.getAbsolutePath());

        _persistit.initialize(properties);
        _persistit.checkAllVolumes();

        final PersistitMap<Integer, String> pmap2 = new PersistitMap<Integer, String>(
                _persistit.getExchange("persistit", "BackupTest", false));
        final boolean comparison = pmap2.equals(tmap);
        assertTrue(comparison);
    }

    @Test
    public void testBackupWithConcurrentTransactions() throws Exception {
        final TransactionWriter tw = new TransactionWriter();
        final Thread twThread = new Thread(tw, "BackupTest_TW");
        twThread.start();
        
        while (tw.counter.get() < TRANSACTION_COUNT) {
            Thread.sleep(1000);
        }

        final File file = File.createTempFile("backup", ".zip");
        file.deleteOnExit();
        final Backup backup1 = new Backup();
        backup1.setMessageStream(System.out);
        backup1.setPersistit(_persistit);
        backup1.setup(1, "backup file=" + file.getAbsolutePath(), "cli", 0, 5);
        backup1.setupTaskWithArgParser(new String[] {
                "file=" + file.getAbsolutePath(), "-c", "-y" });
        tw.backupStarted.set(true);
        backup1.run();
        tw.stop.set(true);
        twThread.join();
        
        final Properties properties = _persistit.getProperties();
        _persistit.crash();
        UnitTestProperties.cleanUpDirectory(new File(UnitTestProperties.DATA_PATH));
        _persistit = new Persistit();
        final Backup backup2 = new Backup();
        backup2.setMessageStream(System.out);
        backup2.setPersistit(_persistit);
        backup2.doRestore(file.getAbsolutePath());

        _persistit.initialize(properties);
        _persistit.checkAllVolumes();
        final Exchange exchange = _persistit.getExchange("persistit", "BackupTest", false);
        exchange.to(Key.BEFORE);
        int extras = 0;
        while (exchange.next()) {
            int key = exchange.getKey().reset().decodeInt();
            boolean found = tw.commitTransactions.remove(key);
            if (!found) {
                extras++;
            }
        }
        assertTrue(tw.commitTransactions.isEmpty());
    }

    private class TransactionWriter implements Runnable {

        final Set<Integer> commitTransactions = new HashSet<Integer>();
        final AtomicInteger counter = new AtomicInteger();
        final Random random = new Random(1);
        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicBoolean backupStarted = new AtomicBoolean();

        public void run() {
            try {
                final Exchange ex = _persistit.getExchange("persistit",
                        "BackupTest", true);
                final Transaction transaction = ex.getTransaction();
                while (!stop.get()) {
                    final int key = random.nextInt();
                    transaction.begin();
                    ex.to(key);
                    ex.getValue().put("Record for key=" + key);
                    ex.store();
                    transaction.commit();
                    if (!backupStarted.get()) {
                        commitTransactions.add(key);
                    }
                    int count = counter.incrementAndGet();
                    transaction.end();
                    // Once the counter has advanced to TRANSACTION_COUNT, throttle this
                    // thread back to a more "realistic" rate and let the backup
                    // thread proceed.
                    if (count > TRANSACTION_COUNT) {
                        Thread.sleep(10);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void runAllTests() throws Exception {
        testSimpleBackup();
        testBackupWithConcurrentTransactions();
    }

}
