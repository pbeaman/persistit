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

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Transaction;
import com.persistit.unit.PersistitUnitTestCase;

/**
 * 
 * From akiban-server 0.81:
 * 
 * ERROR BTree structure error Volume akiban_txn(/var/lib/akiban/akiban_txn.v0)
 * 
 * level=0 page=35
 * key=<{83,(char)82,(long)20110115200202,(long)20111110094933,(long
 * )1688310,(long)1688310 ,(long)24091208}-> is before left edge
 * 
 * Exchange(Volume=/var/lib/akiban/akiban_txn.v0,Tree=_txn_100000004,,Key=<{83,(
 * char
 * )82,(long)20110115200202,(long)20111110094933,(long)1688310,(long)1688310,
 * (long)24091208}->)
 * 
 * 0: Buffer=<Page 58 in volume akiban_txn(/var/lib/akiban/akiban_txn.v0) at
 * index 88,860 timestamp=1,935,552,865 status=vdwr1 <Network-Worker-Thread-1>
 * type=Data>, keyGeneration=115650,
 * bufferGeneration=559,foundAt=<32:fixup:depth=10:ebc=0:db=37:tail=6496>>
 * 
 * 1: Buffer=<Page 63 in volume akiban_txn(/var/lib/akiban/akiban_txn.v0) at
 * index 88,861 timestamp=1,935,552,865 status=vd
 * type=Index1>,keyGeneration=115650,
 * bufferGeneration=16,foundAt=<36:fixup:depth=9:ebc=0:db=37:tail=16188>>
 */

public class Bug889850Test extends PersistitUnitTestCase {

    @Test
    public void testDeleteKeysFromTransaction() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "bug889850", true);
        ex.getValue().put(RED_FOX);
        final Transaction txn = _persistit.getTransaction();
        txn.begin();
        for (int i = 5000; --i >= 0;) {
            ex.to(i);
            ex.store();
        }
        txn.commit();
        txn.end();
        txn.begin();
        for (int i = 0; i < 5000; i++) {
            ex.to(i);
            ex.remove();
        }
        for (int i = 5000; --i >= 0;) {
            ex.to(i);
            ex.remove();
        }
        txn.commit();
        txn.end();
    }

}
