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

import java.io.File;
import java.io.PrintWriter;
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

public class BackupTaskTest extends PersistitUnitTestCase {
    private final static int TRANSACTION_COUNT = 50000;

    @Override
    protected Properties getProperties(final boolean cleanup) {
        return UnitTestProperties.getBiggerProperties(cleanup);
    }

    @Test
    public void testSimpleBackup() throws Exception {

        final PrintWriter writer = new PrintWriter(System.out);
        final PersistitMap<Integer, String> pmap1 = new PersistitMap<Integer, String>(_persistit.getExchange(
                "persistit", "BackupTest", true));
        for (int index = 0; index < 50000; index++) {
            pmap1.put(new Integer(index), "This is the record for index=" + index);
        }

        final TreeMap<Integer, String> tmap = new TreeMap<Integer, String>(pmap1);
        final File file = File.createTempFile("backup", ".zip");
        file.deleteOnExit();

        BackupTask backup1 = (BackupTask) CLI.parseTask(_persistit, "backup -z -c file=" + file.getAbsolutePath());

        backup1.setMessageWriter(writer);
        backup1.setup(1, "backup file=" + file.getAbsolutePath(), "cli", 0, 5);
        backup1.run();

        final Properties properties = _persistit.getProperties();
        _persistit.close();

        _persistit = new Persistit();
        final BackupTask backup2 = new BackupTask();
        backup2.setMessageWriter(writer);
        backup2.setPersistit(_persistit);
        backup2.doRestore(file.getAbsolutePath());

        _persistit.initialize(properties);
        _persistit.checkAllVolumes();

        final PersistitMap<Integer, String> pmap2 = new PersistitMap<Integer, String>(_persistit.getExchange(
                "persistit", "BackupTest", false));
        final boolean comparison = pmap2.equals(tmap);
        assertTrue(comparison);
    }

    @Test
    public void testBackupWithConcurrentTransactions() throws Exception {
        final PrintWriter writer = new PrintWriter(System.out);
        final TransactionWriter tw = new TransactionWriter();
        final Thread twThread = new Thread(tw, "BackupTest_TW");
        twThread.start();

        while (tw.counter.get() < TRANSACTION_COUNT) {
            Thread.sleep(1000);
        }

        final File file = File.createTempFile("backup", ".zip");
        file.deleteOnExit();

        BackupTask backup1 = (BackupTask) CLI.parseTask(_persistit, "backup -y -c file=" + file.getAbsolutePath());

        backup1.setMessageWriter(writer);
        backup1.setPersistit(_persistit);
        backup1.setup(1, "backup file=" + file.getAbsolutePath(), "cli", 0, 5);
        tw.backupStarted.set(true);
        backup1.run();
        tw.stop.set(true);
        twThread.join();

        final Properties properties = _persistit.getProperties();
        _persistit.crash();
        UnitTestProperties.cleanUpDirectory(new File(UnitTestProperties.DATA_PATH));

        _persistit = new Persistit();
        final BackupTask backup2 = new BackupTask();
        backup2.setMessageWriter(writer);
        backup2.setPersistit(_persistit);
        backup2.doRestore(file.getAbsolutePath());
        properties.setProperty("appendonly", "true");

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

        @Override
        public void run() {
            try {
                final Exchange ex = _persistit.getExchange("persistit", "BackupTest", true);
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
                    // Once the counter has advanced to TRANSACTION_COUNT,
                    // throttle this
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
