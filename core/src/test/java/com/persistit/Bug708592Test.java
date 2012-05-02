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

package com.persistit.bug;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.KeyParser;
import com.persistit.policy.SplitPolicy;
import com.persistit.unit.PersistitUnitTestCase;

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
