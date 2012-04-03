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

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Transaction;
import com.persistit.unit.PersistitUnitTestCase;

public class Bug877656Test extends PersistitUnitTestCase {

    public void testForward() throws Exception {
        Transaction txn = _persistit.getTransaction();
        txn.begin();
        Exchange ex = _persistit.getExchange("persistit", "Bug877656Test", true);
        for (int i = 1; i < 10; i++) {
            ex.to(i).store();
        }
        txn.commit();
        txn.end();
        // ----------
        txn.begin();
        for (int i = 1; i < 10; i++) {
            ex.to(i).remove();
        }
        ex.to(0);
        int count = 0;
        for (; count < 10; count++) {
            if (!ex.traverse(Key.GTEQ, true)) {
                break;
            }
        }
        assertEquals("{{before}}", ex.getKey().toString());
        txn.commit();
        txn.end();
        assertEquals(0, count);
    }

    public void testReverse() throws Exception {
        Transaction txn = _persistit.getTransaction();
        txn.begin();
        Exchange ex = _persistit.getExchange("persistit", "Bug877656Test", true);
        for (int i = 1; i < 10; i++) {
            ex.to(i).store();
        }
        txn.commit();
        txn.end();
        // ----------
        txn.begin();
        for (int i = 1; i < 10; i++) {
            ex.to(i).remove();
        }
        ex.to(11);
        int count = 0;
        for (; count < 10; count++) {
            if (!ex.traverse(Key.LTEQ, true)) {
                break;
            }
        }
        assertEquals("{{after}}", ex.getKey().toString());
        txn.commit();
        txn.end();
        assertEquals(0, count);
    }

}
