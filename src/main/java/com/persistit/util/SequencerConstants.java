/**
 * Copyright 2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.persistit.util;

import static com.persistit.util.ThreadSequencer.allocate;
import static com.persistit.util.ThreadSequencer.array;

/**
 * Constants used in the implementation of internal tests on concurrent
 * behavior.
 * 
 * @author peter
 * 
 */
public interface SequencerConstants {

    /*
     * Used in testing a race condition between Checkpoint and Transaction
     * commit, bug 937877
     */
    int COMMIT_FLUSH_A = allocate("COMMIT_FLUSH_A");
    int COMMIT_FLUSH_B = allocate("COMMIT_FLUSH_B");
    int COMMIT_FLUSH_C = allocate("COMMIT_FLUSH_C");

    int[][] COMMIT_FLUSH_SCHEDULE = new int[][] { array(COMMIT_FLUSH_A, COMMIT_FLUSH_B), array(COMMIT_FLUSH_A),
            array(COMMIT_FLUSH_B, COMMIT_FLUSH_C), array(COMMIT_FLUSH_B, COMMIT_FLUSH_C), };

    /*
     * Used in testing a race condition during recovery between main and
     * JOURNAL_COPIER
     */

    int RECOVERY_PRUNING_A = allocate("RECOVERY_PRUNING_A");
    int RECOVERY_PRUNING_B = allocate("RECOVERY_PRUNING_B");

    int[][] RECOVERY_PRUNING_SCHEDULE = new int[][] { array(RECOVERY_PRUNING_B),
            array(RECOVERY_PRUNING_A, RECOVERY_PRUNING_B), };

    /*
     * Used in testing sequencing between write-write dependencies
     */
    int WRITE_WRITE_STORE_A = allocate("WRITE_WRITE_STORE_A");
    int WRITE_WRITE_STORE_B = allocate("WRITE_WRITE_STORE_B");
    int WRITE_WRITE_STORE_C = allocate("WRITE_WRITE_STORE_C");

    int[][] WRITE_WRITE_STORE_SCHEDULE = new int[][] { array(WRITE_WRITE_STORE_A, WRITE_WRITE_STORE_B),
            array(WRITE_WRITE_STORE_B), array(WRITE_WRITE_STORE_A, WRITE_WRITE_STORE_C),
            array(WRITE_WRITE_STORE_A, WRITE_WRITE_STORE_C) };

    /*
     * Used in testing sequencing between pageNode reading and invalidation in
     * JournalManager
     */
    int PAGE_MAP_READ_INVALIDATE_A = allocate("PAGE_MAP_READ_INVALIDATE_A");
    int PAGE_MAP_READ_INVALIDATE_B = allocate("PAGE_MAP_READ_INVALIDATE_B");
    int PAGE_MAP_READ_INVALIDATE_C = allocate("PAGE_MAP_READ_INVALIDATE_C");

    int[][] PAGE_MAP_READ_INVALIDATE_SCHEDULE = new int[][] {
            array(PAGE_MAP_READ_INVALIDATE_A, PAGE_MAP_READ_INVALIDATE_B), array(PAGE_MAP_READ_INVALIDATE_B),
            array(PAGE_MAP_READ_INVALIDATE_A, PAGE_MAP_READ_INVALIDATE_C),
            array(PAGE_MAP_READ_INVALIDATE_A, PAGE_MAP_READ_INVALIDATE_C) };

    /*
     * Used in testing sequence of timestamp allocation and page deallocation in
     * Bug1003478Test
     */
    int LONG_RECORD_ALLOCATE_A = allocate("LONG_RECORD_ALLOCATE_A");
    int LONG_RECORD_ALLOCATE_B = allocate("LONG_RECORD_ALLOCATE_B");
    int[][] LONG_RECORD_ALLOCATE_SCHEDULED = new int[][] { array(LONG_RECORD_ALLOCATE_B),
            array(LONG_RECORD_ALLOCATE_A, LONG_RECORD_ALLOCATE_B) };

    /*
     * Used in testing delete/deallocate sequence in Bug1022567Test
     */
    int DEALLOCATE_CHAIN_A = allocate("DEALLOCATE_CHAIN_A");
    int DEALLOCATE_CHAIN_B = allocate("DEALLOCATE_CHAIN_B");
    int DEALLOCATE_CHAIN_C = allocate("DEALLOCATE_CHAIN_C");
    int[][] DEALLOCATE_CHAIN_SCHEDULED = new int[][] { array(DEALLOCATE_CHAIN_A, DEALLOCATE_CHAIN_B),
            array(DEALLOCATE_CHAIN_B), array(DEALLOCATE_CHAIN_A, DEALLOCATE_CHAIN_C),
            array(DEALLOCATE_CHAIN_A, DEALLOCATE_CHAIN_C) };

    /*
     * Used in testing delete/deallocate sequence in Bug1064565Test
     */
    int ACCUMULATOR_CHECKPOINT_A = allocate("ACCUMULATOR_CHECKPOINT_A");
    int ACCUMULATOR_CHECKPOINT_B = allocate("ACCUMULATOR_CHECKPOINT_B");
    int ACCUMULATOR_CHECKPOINT_C = allocate("ACCUMULATOR_CHECKPOINT_C");
    int[][] ACCUMULATOR_CHECKPOINT_SCHEDULED = new int[][] { array(ACCUMULATOR_CHECKPOINT_A, ACCUMULATOR_CHECKPOINT_B),
            array(ACCUMULATOR_CHECKPOINT_B), array(ACCUMULATOR_CHECKPOINT_A, ACCUMULATOR_CHECKPOINT_C),
            array(ACCUMULATOR_CHECKPOINT_A, ACCUMULATOR_CHECKPOINT_C) };

}
