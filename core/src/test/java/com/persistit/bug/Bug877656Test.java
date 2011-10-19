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
        assertEquals("{{after}}", ex.getKey().toString());
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
        assertEquals("{{before}}", ex.getKey().toString());
        txn.commit();
        txn.end();
        assertEquals(0, count);
    }

}
