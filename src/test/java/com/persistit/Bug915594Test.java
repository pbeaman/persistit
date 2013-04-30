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
