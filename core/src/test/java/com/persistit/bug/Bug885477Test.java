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
import com.persistit.Key;
import com.persistit.Transaction;
import com.persistit.unit.PersistitUnitTestCase;

/**
 * Suppose a persistit tree has keys (1, 10), (1, 20), (2, 30). If an exchange's
 * key is (1), then traverse(GTEQ, false) should traverse to the first record,
 * resulting in the key still containing (1). Instead, traverse results in an
 * exchange with key (2), as if the EQ part of the direction was ignored.
 */

public class Bug885477Test extends PersistitUnitTestCase {

    @Test
    public void testNotInTransaction() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "bug885477", true);
        ex.clear().append(1).append(10).store();
        ex.clear().append(1).append(20).store();
        ex.clear().append(2).append(30).store();
        
        ex.clear().append(1);
        assertTrue(ex.traverse(Key.GTEQ, false));
        assertEquals(1, ex.getKey().decodeInt());

        ex.clear().append(2);
        assertTrue(ex.traverse(Key.LTEQ, false));
        assertEquals(2, ex.getKey().decodeInt());

        assertTrue(ex.hasChildren());
        assertFalse(ex.isValueDefined());
        ex.append(30);
        assertFalse(ex.hasChildren());
        assertTrue(ex.isValueDefined());
        
    }

    @Test
    public void testInTransaction() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "bug885477", true);
        final Transaction txn = _persistit.getTransaction();
        txn.begin();
        ex.clear().append(1).append(10).store();
        ex.clear().append(1).append(20).store();
        ex.clear().append(2).append(30).store();
        
        ex.clear().append(1);
        assertTrue(ex.traverse(Key.GTEQ, false));
        assertEquals(1, ex.getKey().decodeInt());

        ex.clear().append(2);
        assertTrue(ex.traverse(Key.LTEQ, false));
        assertEquals(2, ex.getKey().decodeInt());
        
        assertTrue(ex.hasChildren());
        assertFalse(ex.isValueDefined());
        
        txn.commit();
        txn.end();
    }

}
