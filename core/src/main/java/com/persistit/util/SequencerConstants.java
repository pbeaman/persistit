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

package com.persistit.util;

import static com.persistit.util.ThreadSequencer.allocate;
import static com.persistit.util.ThreadSequencer.array;

/**
 * Constants used in the implementation of internal tests on
 * concurrent behavior.
 * @author peter
 *
 */
public interface SequencerConstants {
    
    /*
     * Used in testing a race condition between Checkpoint and Transaction commit, bug 937877
     */
    int COMMIT_FLUSH_A = allocate("COMMIT_FLUSH_A");
    int COMMIT_FLUSH_B = allocate("COMMIT_FLUSH_B");
    int COMMIT_FLUSH_C = allocate("COMMIT_FLUSH_C");

    int[][] COMMIT_FLUSH_SCHEDULE = new int[][] {
            array(COMMIT_FLUSH_A, COMMIT_FLUSH_B), array(COMMIT_FLUSH_A),
            array(COMMIT_FLUSH_B, COMMIT_FLUSH_C), array(COMMIT_FLUSH_B, COMMIT_FLUSH_C),
    };
    
    /*
     * Used in testing a race condition during recovery between main and JOURNAL_COPIER
     */
    
    int RECOVERY_PRUNING_A = allocate("RECOVERY_PRUNING_A");
    int RECOVERY_PRUNING_B = allocate("RECOVERY_PRUNING_B");

    int[][] RECOVERY_PRUNING_SCHEDULE = new int[][] {
            array(RECOVERY_PRUNING_B), array(RECOVERY_PRUNING_A, RECOVERY_PRUNING_B),
    };
    
    /*
     * Used in testing sequencing between write-write dependencies 
     */
    int WRITE_WRITE_STORE_A = allocate("WRITE_WRITE_STORE_A");
    int WRITE_WRITE_STORE_B = allocate("WRITE_WRITE_STORE_B");
    int WRITE_WRITE_STORE_C = allocate("WRITE_WRITE_STORE_C");

    int[][] WRITE_WRITE_STORE_SCHEDULE = new int[][] {
            array(WRITE_WRITE_STORE_A, WRITE_WRITE_STORE_B), array(WRITE_WRITE_STORE_B),
            array(WRITE_WRITE_STORE_A, WRITE_WRITE_STORE_C), array(WRITE_WRITE_STORE_A, WRITE_WRITE_STORE_C)
    };

    /*
    * Used in testing sequencing between tree creation and removal
    */
    int TREE_CREATE_REMOVE_A = allocate("TREE_CREATE_REMOVE_A");
    int TREE_CREATE_REMOVE_B = allocate("TREE_CREATE_REMOVE_B");
    int TREE_CREATE_REMOVE_C = allocate("TREE_CREATE_REMOVE_C");

    int[][] TREE_CREATE_REMOVE_SCHEDULE = new int[][] {
            array(TREE_CREATE_REMOVE_A, TREE_CREATE_REMOVE_B), array(TREE_CREATE_REMOVE_B),
            array(TREE_CREATE_REMOVE_A, TREE_CREATE_REMOVE_C), array(TREE_CREATE_REMOVE_A, TREE_CREATE_REMOVE_C)
    };

    /*
    * Used in testing sequencing between pageNode reading and invalidation in JournalManager
    */
    int PAGE_MAP_READ_INVALIDATE_A = allocate("PAGE_MAP_READ_INVALIDATE_A");
    int PAGE_MAP_READ_INVALIDATE_B = allocate("PAGE_MAP_READ_INVALIDATE_B");
    int PAGE_MAP_READ_INVALIDATE_C = allocate("PAGE_MAP_READ_INVALIDATE_C");

    int[][] PAGE_MAP_READ_INVALIDATE_SCHEDULE = new int[][] {
            array(PAGE_MAP_READ_INVALIDATE_A, PAGE_MAP_READ_INVALIDATE_B), array(PAGE_MAP_READ_INVALIDATE_B),
            array(PAGE_MAP_READ_INVALIDATE_A, PAGE_MAP_READ_INVALIDATE_C), array(PAGE_MAP_READ_INVALIDATE_A, PAGE_MAP_READ_INVALIDATE_C)
    };
    
    /*
     * Used in testing sequence of timestamp allocation and page deallocation in Bug1003478Test
     * 
     */
    int LONG_RECORD_ALLOCATE_A = allocate("LONG_RECORD_ALLOCATE_A");
    int LONG_RECORD_ALLOCATE_B = allocate("LONG_RECORD_ALLOCATE_B");
    int[][] LONG_RECORD_ALLOCATE_SCHEDULED = new int[][]{array(LONG_RECORD_ALLOCATE_B), array(LONG_RECORD_ALLOCATE_A, LONG_RECORD_ALLOCATE_B)};
}
