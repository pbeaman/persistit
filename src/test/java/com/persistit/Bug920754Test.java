/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.persistit.Accumulator.SumAccumulator;

public class Bug920754Test extends PersistitUnitTestCase {
    /*
     * https://bugs.launchpad.net/akiban-persistit/+bug/920754
     * 
     * While working on my "ApiTestBase turbo" branch, I noticed the volume file
     * growing faster than I expected. I put a debug point in ApiTestBase's
     * 
     * @After method, when it should be compressing journals and such. When I
     * looked in the persistit UI, I saw that there were a few trees still
     * defined but with nothing in them; the AIS (which I looked into via the
     * DXL jmx bean) had only the primordial tables. But _directory contained
     * what looked like "a whole ton" of accumulators, for trees that no longer
     * existed.
     * 
     * To reproduce: - lp:~yshavit/akiban-server/ApiTestBase_turbo -r 1440 -
     * conditional breakpoint at ApiTestBase #274
     * ("System.out.println("flushing");"), to be triggered when size >= 400 *
     * 1024 * 1024. - look in persistit UI at the _directory tree. It should be
     * empty; it's very full.
     * 
     * From within the debugger, I invoked a checkpoint. Judging from write
     * time, the journal file did seem to have been written to; but the
     * accumulators didn't go away.
     */

    @Test
    public void testAccumumulatorTreeIsDeleted() throws Exception {
        final Exchange exchange = _persistit.getExchange("persistit", "Bug920754Test", true);
        final Transaction txn = _persistit.getTransaction();
        final SumAccumulator[] accumulators = new SumAccumulator[10];
        for (int i = 0; i < 10; i++) {
            accumulators[i] = exchange.getTree().getSumAccumulator(1);
        }
        txn.begin();
        for (int i = 0; i < 10; i++) {
            accumulators[i].add(1);
        }
        txn.commit();
        txn.end();
        _persistit.checkpoint();
        final Exchange dir = exchange.getVolume().getStructure().directoryExchange();
        exchange.removeTree();
        int keys = 0;
        dir.to(Key.BEFORE);
        while (dir.next(true)) {
            keys++;
        }
        // _classIndex
        assertEquals("There should be one remaining key in the directory tree", 1, keys);
    }
}
