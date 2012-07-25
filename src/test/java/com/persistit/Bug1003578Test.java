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

import static com.persistit.util.SequencerConstants.LONG_RECORD_ALLOCATE_B;
import static com.persistit.util.SequencerConstants.LONG_RECORD_ALLOCATE_SCHEDULED;
import static com.persistit.util.ThreadSequencer.addSchedules;
import static com.persistit.util.ThreadSequencer.disableSequencer;
import static com.persistit.util.ThreadSequencer.enableSequencer;
import static com.persistit.util.ThreadSequencer.sequence;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import com.persistit.exception.PersistitException;

/**
 * https://bugs.launchpad.net/akiban-persistit/+bug/1003578
 * 
 * In a recent branch, lp:~pbeaman/akiban-persistit/fix_959456, a new assert was
 * added in JournalManager#writePageToJournal checking an invariant that was
 * suppose to be in place for quite some time. The assert checks that a new
 * entry to the pageMap has a timestamp greater than or equal to the previous
 * entry.
 * 
 * This assert fired in a server-daily run, on LotsOfTablesDT:
 * http://172.16.20.104
 * :8080/view/daily/job/server-daily/com.akiban$akiban-server/1018/
 * java.lang.AssertionError
 * 
 * <pre>
 *  at com.persistit.JournalManager.writePageToJournal(JournalManager.java:1020)
 *  at com.persistit.VolumeStorageV2.writePage(VolumeStorageV2.java:477)
 *  at com.persistit.Buffer.writePage(Buffer.java:538)
 *  at com.persistit.Exchange.storeOverlengthRecord(Exchange.java:3850)
 *  at com.persistit.Exchange.storeInternal(Exchange.java:1382)
 *  at com.persistit.Exchange.store(Exchange.java:1307)
 *  at com.persistit.Exchange.store(Exchange.java:2551)
 *  at com.akiban.server.store.PersistitStoreSchemaManager.saveProtobuf(PersistitStoreSchemaManager.java:869)
 *  at com.akiban.server.store.PersistitStoreSchemaManager.saveAISToStorage(PersistitStoreSchemaManager.java:830)
 *  at com.akiban.server.store.PersistitStoreSchemaManager.commitAISChange(PersistitStoreSchemaManager.java:896)
 *  at com.akiban.server.store.PersistitStoreSchemaManager.createTableDefinition(PersistitStoreSchemaManager.java:187)
 *  at com.akiban.server.service.dxl.BasicDDLFunctions.createTable(BasicDDLFunctions.java:85)
 *  at com.akiban.server.service.dxl.HookableDDLFunctions.createTable(HookableDDLFunctions.java:65)
 *  at com.akiban.server.test.ApiTestBase.createTable(ApiTestBase.java:442)
 *  at com.akiban.server.test.daily.slap.LotsOfTablesDT.createLotsOfTablesTest(LotsOfTablesDT.java:62)
 * </pre>
 * 
 * Which corresponds to this check (line 1020 of r300, 1030 of r302):
 * 
 * <pre>
 * if (oldPageNode != null) {
 *     assert oldPageNode.getTimestamp() &lt;= pageNode.getTimestamp();
 * }
 * </pre>
 * 
 * Bug theory:
 * 
 * Bug is caused by the violation in LongRecordHelper#storeLongRecord():
 * 
 * - Test thread calls storeLongRecord with some timestamp T1.
 * 
 * - CLEANUP_MANAGER processes a CleanupAntiValue and adds a page to the garbage
 * chain with timestamp T2 > T1.
 * 
 * - PAGE_WRITER writes that dirty page with timestamp T2.
 * 
 * - Test thread allocates a new page from the garbage chain, a page that is
 * already dirty and in the PagePap with timestamp T2, but storeOverlengthRecord
 * marks it dirty with timestamp T1.
 * 
 * This analysis works both before and after branch
 * lp:~pbeaman/akiban-persistit/fix_959456. Prior to that branch the guilty
 * method is Exchange#storeOverlengthRecord as shown in the stack trace above.
 */

public class Bug1003578Test extends PersistitUnitTestCase {

    @Test
    public void storeLongRecordFromDeallocatedPages() throws Exception {
        /*
         * Create a tree with a few data pages
         */
        final Exchange ex1 = _persistit.getExchange("persistit", "Bug1003578Test", true);
        final Transaction txn = ex1.getTransaction();
        ex1.getValue().put(RED_FOX);
        for (int i = 0; i < 10000; i++) {
            ex1.to(i).store();
        }
        /*
         * Temporarily suspect the cleanup manager
         */
        _persistit.getCleanupManager().setPollInterval(-1);
        enableSequencer(true);
        addSchedules(LONG_RECORD_ALLOCATE_SCHEDULED);
        /*
         * Now transactionally delete the pages; this will create AntiValues
         */
        txn.begin();
        ex1.clear().removeAll();
        txn.commit();
        txn.end();
        
        /*
         * Traverse the AntiValues. This will enqueue the pages for pruning.
         */
        assertFalse("Should have no visible keys", ex1.to(Key.BEFORE).next());

        final String longString = createString(1000000);
        Thread t = new Thread(new Runnable() {
            public void run() {
                final Exchange ex2 = new Exchange(ex1);
                try {
                    /*
                     * Now create long records
                     */
                    ex2.getValue().put(longString);
                    ex2.to("longrec").store();
                } catch (PersistitException e) {
                    e.printStackTrace();
                }
            }
        });
        t.start();
        /*
         * Clean up the non-edge AntiValues 
         */
        _persistit.getCleanupManager().poll();
        /*
         * Clean up the left-edge AntiValues
         */
        _persistit.getCleanupManager().poll();

        
        sequence(LONG_RECORD_ALLOCATE_B);
        disableSequencer();
        t.join();
        ex1.to("longrec").fetch();
        assertEquals("Should have stored the long record string", longString, ex1.getValue().getString());
    }
}
