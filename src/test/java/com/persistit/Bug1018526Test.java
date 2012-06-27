/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;

public class Bug1018526Test extends PersistitUnitTestCase {

    @Test
    public void tempVolumesAndTreesDoNotGetHandles() throws Exception {
        final Volume volume = _persistit.createTemporaryVolume();
        final Exchange exchange = _persistit.getExchange(volume, "a_temp_tree", true);
        assertEquals("Handle should be 0", 0, volume.getHandle());
        assertEquals("Handle should be 0", 0, exchange.getTree().getHandle());
    }

    @Test
    public void txnOnTempVolumeDoesNotWriteToJournal() throws Exception {
        final Transaction txn = _persistit.getTransaction();
        final JournalManager jman = _persistit.getJournalManager();
        int failed = 0;
        for (int i = 0; i < 10; i++) {
            final long startingAddress = jman.getCurrentAddress();
            txn.begin();
            try {
                final Volume volume = _persistit.createTemporaryVolume();
                final Exchange exchange = _persistit.getExchange(volume, "a_temp_tree", true);
                exchange.clear().append("abc");
                exchange.getValue().put(RED_FOX);
                exchange.store();
                txn.commit();
                if (jman.getCurrentAddress() != startingAddress) {
                    failed++;
                }
            } finally {
                txn.end();
            }
        }
        /*
         * Don't require 0 because a checkpoint could write to the journal in a race
         */
        assertTrue("Transaction on temporary volume should not have written to journal", failed < 3);
    }
}
