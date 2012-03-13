/**
 * Copyright (C) 2012 Akiban Technologies Inc.
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

package com.persistit.util;

import static com.persistit.util.ThreadSequencer.allocate;
import static com.persistit.util.ThreadSequencer.array;

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
            array(WRITE_WRITE_STORE_A, WRITE_WRITE_STORE_C), array(WRITE_WRITE_STORE_A, WRITE_WRITE_STORE_C),
            array(WRITE_WRITE_STORE_B, WRITE_WRITE_STORE_C), array(WRITE_WRITE_STORE_B, WRITE_WRITE_STORE_C)
    };

}
