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
