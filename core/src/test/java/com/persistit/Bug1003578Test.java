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
import com.persistit.unit.PersistitUnitTestCase;

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
        final Exchange ex = _persistit.getExchange("persistit", "Bug1003578Test", true);
        final Transaction txn = ex.getTransaction();
        ex.getValue().put(RED_FOX);
        for (int i = 0; i < 10000; i++) {
            ex.to(i).store();
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
        ex.clear().removeAll();
        txn.commit();
        txn.end();
        
        /*
         * Traverse the AntiValues. This will enqueue the pages for pruning.
         */
        assertFalse("Should have no visible keys", ex.to(Key.BEFORE).next());

        /*
         * Reenable the CleanupManager
         */
        final String longString = createString(1000000);
        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    /*
                     * Now create long records
                     */
                    ex.getValue().put(longString);
                    ex.to("longrec").store();
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
        ex.to("longrec").fetch();
        assertEquals("Should have stored the long record string", longString, ex.getValue().getString());
    }
}
