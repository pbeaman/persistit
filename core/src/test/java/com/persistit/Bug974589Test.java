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

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.unit.UnitTestProperties;

public class Bug974589Test extends PersistitUnitTestCase {
    /*
     * https://bugs.launchpad.net/akiban-persistit/+bug/974589
     * IllegalStateException Thrown from BucketSampler.add During analyze table
     * 
     * Distilled: database is left with row count Accumulator holding a value of
     * 0 when there is actually a row.
     */

    private static final String TREE_NAME = "Bug974589Test";

    private static Exchange getExchange(Persistit persistit) throws PersistitException {
        return persistit.getExchange(UnitTestProperties.VOLUME_NAME, TREE_NAME, true);
    }

    public void testBug980292() throws Exception {
        for (int loop = 1; loop < 5; loop++) {
            _persistit.checkpoint();
            Exchange ex1 = getExchange(_persistit);
            Transaction txn1 = ex1.getTransaction();
            /*
             * Touch an Accumulator, then immediately checkpoint.
             */
            txn1.begin();
            Accumulator acc1 = ex1.getTree().getAccumulator(Accumulator.Type.SUM, 1);
            acc1.update(1, txn1);
            txn1.commit();
            txn1.end();
            ex1 = null;
            txn1 = null;
            acc1 = null;
            _persistit.checkpoint();
            _persistit.close();

            Properties properties = _persistit.getProperties();
            _persistit = new Persistit();
            _persistit.initialize(properties);

            Exchange ex2 = getExchange(_persistit);
            Transaction txn2 = ex2.getTransaction();
            txn2.begin();
            Accumulator acc2 = ex2.getTree().getAccumulator(Accumulator.Type.SUM, 1);
            assertEquals(loop, acc2.getSnapshotValue(txn2));
            txn2.commit();
            txn2.end();
            System.out.printf("Iteration %,d completed\n", loop);
            ex2 = null;
            txn2 = null;
            acc2 = null;
        }
    }
}
