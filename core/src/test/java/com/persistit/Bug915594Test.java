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

package com.persistit;

import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.unit.PersistitUnitTestCase;

/**
 * I did an update that failed due to constraint violation and the transaction
 * was rolled back.
 * 
 * <pre>
 * test=> drop table ts;
 * DROP TABLE
 * test=> create table ts (id int primary key not null, s varchar(32));
 * CREATE TABLE
 * test=> create unique index ts_s on ts(s);
 * CREATE INDEX
 * test=> insert into ts values(1, 'abc');
 * INSERT 0 1
 * test=> insert into ts values(2, 'xyz');
 * INSERT 0 1
 * test=> select * from ts;
 *  id | s
 * ----+-----
 *   1 | abc
 *   2 | xyz
 * (2 rows)
 * 
 * test=> update ts set s = 'oops';
 * ERROR: Non-unique key for index ts_s: {"oops",(long)2}
 * test=> select * from ts;
 *  id | s
 * ----+-----
 *   1 | abc
 *   2 | xyz
 * (2 rows)
 * </pre>
 * <p />
 * I then restarted the server (uncleanly with ^C and akserver -f).
 * <p />
 * Now the update that failed is there.
 * 
 * <pre>
 * test=> select * from ts;
 *  id | s
 * ----+------
 *   1 | oops
 *   2 | oops
 * (2 rows)
 * </pre>
 * <p />
 * In the Persistit GUI, the MVV was <Aborted> before the shutdown and then had
 * a valid timestamp when it came back up.
 */

public class Bug915594Test extends PersistitUnitTestCase {

    @Test
    public void testAbortedTransactionMustNotRecovered() throws Exception {
        Exchange ex = _persistit.getExchange("persistit", "Bug915594Test", true);
        Transaction txn = ex.getTransaction();
        txn.begin();
        for (int i = 1; i <= 10; i++) {
            ex.getValue().put(RED_FOX);
            ex.clear().append(i).store();
        }
        txn.rollback();
        txn.end();
        _persistit.checkpoint();
        Properties properties = _persistit.getProperties();
        ex = null;
        txn = null;
        _persistit.close();

        _persistit = new Persistit();
        _persistit.initialize(properties);
        Exchange ex2 = _persistit.getExchange("persistit", "Bug915594Test", true);
        assertTrue(!ex2.clear().next(true));
    }
}
