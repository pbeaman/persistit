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

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.junit.Test;

import com.persistit.policy.SplitPolicy;

/*
 * Got this while loading sample data. Apparently the new PACK split policy 
 * splits a data page at the right edge, leading to this Exception when 
 * inserting an invalid key in the index level.
 * Caused by: com.persistit.exception.CorruptVolumeException: Volume
 * akiban_data(/var/lib/akiban/akiban_data) level=1 page=0
 * previousPage=331207 initialPage=331207 key=<{{right edge}}>
 * oldBuffer=<Page 331207 in Volume
 * akiban_data(/var/lib/akiban/akiban_data) at index 66715
 * status=vdswr1 <Network-Worker-Thread-0>> invalid page address
 *    at com.persistit.Exchange.searchLevel(Exchange.java:1090)
 *    at com.persistit.Exchange.searchTree(Exchange.java:999)
 *    at com.persistit.Exchange.storeInternal(Exchange.java:1342)
 *    at com.persistit.Transaction.applyUpdatesFast(Transaction.java:1958)
 *    at com.persistit.Transaction.doCommit(Transaction.java:1329)
 *    at com.persistit.Transaction.commit(Transaction.java:918)
 *    at com.akiban.server.store.PersistitStore.writeRow(PersistitStore.java:593)
 *    at com.akiban.server.service.dxl.BasicDMLFunctions.writeRow(BasicDMLFunctions.java:687)
 * 
 *  Workaround: specify NICE policy in server.properties:
 * 
 *   persistit.splitpolicy=NICE
 */

public class Bug708592Test extends PersistitUnitTestCase {

    @Test
    public void test1() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "bug708592", true);
        ex.clear();
        ex.setSplitPolicy(SplitPolicy.LEFT_BIAS);
        final BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getClassLoader()
                .getResourceAsStream("Bug780592_data.txt")));
        String line = null;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("{")) {
                ex.getKey().clear();
                final KeyParser parser = new KeyParser(line);
                parser.parseKey(ex.getKey());
                ex.store();
            } else if (line.startsWith("*")) {
                System.out.println(line);
                ex.setSplitPolicy(SplitPolicy.PACK_BIAS);
            }
        }
    }

    @Override
    public void runAllTests() throws Exception {
        test1();
    }

}
