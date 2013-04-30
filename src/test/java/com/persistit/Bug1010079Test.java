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

package com.persistit;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * https://bugs.launchpad.net/akiban-persistit/+bug/1010079
 * 
 * Various symptoms found while running stress test mixture_txn_1, including
 * CorruptVolumeException, CorruptValueException, and several different asserts.
 * These all stem from new code introduced in
 * lp:~pbeaman/akiban-persistit/fix_1006576_long_record_pruning. This bug is
 * related to 1005206 and 1006576. Diagnosis:
 * 
 * New code added to prune LongRecord MVVs operates on a copy of the original
 * buffer and then atomically refreshes the original buffer from the copy. That
 * operation fails to invalidate the FastIndex of the original buffer in the
 * event a key is removed due to a LongRecord MVV becoming a primordial
 * anti-value.
 * 
 * In addition, pruneLongMvvValues uses a Value object obtained from a
 * ThreadLocal. However, this same Value is already in use by
 * Exchange#storeInternal.
 * 
 * @author peter
 * 
 */
public class Bug1010079Test extends MVCCTestBase {

    @Test
    public void induceCorruption() throws Exception {
        /*
         * 1. Create a page with a long record MVV.
         * 
         * 2. Read the value to set up the LevelCache
         * 
         * 3. Abort the transaction so that long record MVV will become an
         * AntiValue
         * 
         * 4. Prune the Buffer to liberate the long record chain.
         * 
         * 5. Store a value in the page (not yet certain why this is needed to
         * induce the bug, but it is.)
         * 
         * 6. Attempt to the read the first value back in.
         * 
         * Theory of bug is that the read attempt will not see a generation
         * change, will use the LevelCache, and will attempt to read a long
         * record from a page that has now become a data page.
         */

        /*
         * Disable background pruning
         */
        _persistit.getCleanupManager().setPollInterval(-1);

        /*
         * Bump the generation number -- perhaps unnecessary
         */
        for (int i = 0; i < 10; i++) {
            ex1.to(i).store();
            ex1.remove();
        }

        /*
         * Store a single Long MVV
         */
        trx1.begin();
        storeLongMVV(ex1, 1);
        ex1.fetch();
        trx1.rollback();
        trx1.end();

        _persistit.getTransactionIndex().updateActiveTransactionCache();

        ex1.prune();

        ex1.getValue().put(RED_FOX);
        ex1.to(0).store();

        /*
         * This method throws a CorruptVolumeException
         */
        ex1.to(1).fetch();
        assertTrue("Value should have been removed by rollback", !ex1.getValue().isDefined());
    }
}
