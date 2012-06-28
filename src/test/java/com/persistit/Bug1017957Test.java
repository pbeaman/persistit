/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
 */

package com.persistit;

import static com.persistit.util.ThreadSequencer.sequence;
import static com.persistit.util.SequencerConstants.*;
import static org.junit.Assert.assertEquals;

import java.io.PrintWriter;

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.util.ThreadSequencer;

/**
 * https://bugs.launchpad.net/akiban-persistit/+bug/1017957
 * 
 * During the past week the 8-hour stress test suite has generated several
 * CorruptVolumeExceptions and other related phenomena. Examples:
 * 
 * Stress6 [main] FAILED: com.persistit.exception.CorruptVolumeException: Volume
 * persistit(/tmp/persistit_tests/persistit) level=0 page=15684
 * initialPage=57164
 * key=<{"stress6",98,5,"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}> walked
 * right more than 50 pages last page visited=81324 at
 * com.persistit.Exchange.corrupt(Exchange.java:3884) at
 * com.persistit.Exchange.searchLevel(Exchange.java:1250) at
 * com.persistit.Exchange.searchTree(Exchange.java:1125) at
 * com.persistit.Exchange.storeInternal(Exchange.java:1443) at
 * com.persistit.Exchange.store(Exchange.java:1294) at
 * com.persistit.Exchange.store(Exchange.java:2534) at
 * com.persistit.stress.unit.Stress6.executeTest(Stress6.java:98) at
 * com.persistit.stress.AbstractStressTest.run(AbstractStressTest.java:93) at
 * java.lang.Thread.run(Thread.java:662)
 * 
 * Stress2txn [main] FAILED: com.persistit.exception.RebalanceException at
 * com.persistit.Buffer.join(Buffer.java:2523) at
 * com.persistit.Exchange.raw_removeKeyRangeInternal(Exchange.java:3367) at
 * com.persistit.Exchange.removeKeyRangeInternal(Exchange.java:3070) at
 * com.persistit.Exchange.removeInternal(Exchange.java:2999) at
 * com.persistit.Exchange.remove(Exchange.java:2927) at
 * com.persistit.stress.unit.Stress2txn.executeTest(Stress2txn.java:231) at
 * com.persistit.stress.AbstractStressTest.run(AbstractStressTest.java:93) at
 * java.lang.Thread.run(Thread.java:662)
 * 
 * Stress2txn [main] FAILED: com.persistit.exception.CorruptVolumeException:
 * LONG_RECORD chain is invalid at page 111919 - invalid page type: Page 111,919
 * in volume persistit(/tmp/persistit_tests/persistit) at index 1,559
 * timestamp=909,787,072 status=vr1 type=Data at
 * com.persistit.LongRecordHelper.corrupt(LongRecordHelper.java:243) at
 * com.persistit.LongRecordHelper.fetchLongRecord(LongRecordHelper.java:103) at
 * com.persistit.Exchange.fetchFixupForLongRecords(Exchange.java:2841) at
 * com.persistit.Exchange.fetchFromValueInternal(Exchange.java:2778) at
 * com.persistit.Exchange.fetchFromBufferInternal(Exchange.java:2747) at
 * com.persistit.Exchange.traverse(Exchange.java:2157) at
 * com.persistit.Exchange.traverse(Exchange.java:1960) at
 * com.persistit.Exchange.traverse(Exchange.java:1897) at
 * com.persistit.Exchange.next(Exchange.java:2330) at
 * com.persistit.stress.unit.Stress2txn.executeTest(Stress2txn.java:188) at
 * com.persistit.stress.AbstractStressTest.run(AbstractStressTest.java:93) at
 * java.lang.Thread.run(Thread.java:662)
 * 
 * Bug mechanism:
 * 
 * An obscure path through
 * {@link Exchange#raw_removeKeyRangeInternal(Key, Key, boolean, boolean)}
 * inserts a key-pointer pair into an index page. It does so after removing all
 * claims on pages and the tree itself. After removing claims, before inserting
 * the key-pointer pair we believe the page itself gets put unto a garbage
 * chain. So after the re-inserion, the index page now has a pointer to a page
 * that will be reused and will contain unrelated data.
 * 
 * @author peter
 * 
 */
public class Bug1017957Test extends PersistitUnitTestCase {

    @Test
    public void induceCorruption() throws Exception {
        ThreadSequencer.enableSequencer(false);
        ThreadSequencer.addSchedules(REMOVE_KEY_1017857_SCHEDULED);
        final Exchange ex = _persistit.getExchange("persistit", "Bug1017957Test", true);
        ((ManagementImpl) _persistit.getManagement()).launch("cliserver port=9999");
        Key key = createUnsafeStructure(ex);

        Thread racer = new Thread(new Runnable() {
            public void run() {
                try {
                    sequence(REMOVE_KEY_1017857_B);
                    Exchange ex = _persistit.getExchange("persistit", "Bug1017957Test", true);
                    removeCoveringRange(ex);
                    sequence(REMOVE_KEY_1017857_C);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        racer.start();
        removeInterestingKey(ex, key);
        IntegrityCheck icheck = new IntegrityCheck(_persistit);
        icheck.setMessageLogVerbosity(Task.LOG_VERBOSE);
        icheck.setMessageWriter(new PrintWriter(System.out));
        icheck.checkVolume(ex.getVolume());
        assertEquals("Corrupt volume", 0, icheck.getFaults().length);
    }

    /**
     * Create a B-Tree with a structure that will induce a deferred index
     * insertion on removal of key. We need an index page that's pretty full
     * such that removing a key and inserting a different one will result in
     * splitting the index page.
     * 
     * @throws Exception
     */
    private Key createUnsafeStructure(final Exchange ex) throws Exception {
        Key result = null;
        final String v = createString(5500); // less than long record
        final String k = createString(1040);
        for (int i = 1000; i < 1019; i++) {
            if (i == 1009) {
                ex.clear().append(i).append(k.substring(0, 20));
                ex.getValue().put("interesting");
                ex.store();
                result = new Key(ex.getKey());
            }
            ex.clear().append(i).append(k);
            ex.getValue().put(v);
            ex.store();
        }
        return result;
    }

    private void removeInterestingKey(final Exchange ex, final Key interestingKey) throws Exception {
        interestingKey.copyTo(ex.getKey());
        ex.remove();
    }

    private void removeCoveringRange(final Exchange ex) throws Exception {
        final Key key1 = new Key(_persistit).append(1005);
        final Key key2 = new Key(_persistit).append(1015);
        ex.removeKeyRange(key1, key2);
    }
}
