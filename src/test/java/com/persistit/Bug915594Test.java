/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

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
        ex = null;
        txn = null;
        _persistit.close();

        _persistit = new Persistit(_config);
        final Exchange ex2 = _persistit.getExchange("persistit", "Bug915594Test", true);
        assertTrue(!ex2.clear().next(true));
    }
}
