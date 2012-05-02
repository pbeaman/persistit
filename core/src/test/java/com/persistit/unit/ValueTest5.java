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

package com.persistit.unit;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.encoding.CoderManager;
import com.persistit.exception.PersistitException;

public class ValueTest5 extends PersistitUnitTestCase {
    /**
     * Tests JSA 1.1 default serialization. Requires the
     * enableCompatibleConstructors to be true.
     */

    Exchange _exchange;

    public enum Job {
        COOK, BOTTLEWASHER, PROGRAMMER, JANITOR, SALEMAN,
    };

    public static class S implements Serializable {
        private final static long serialVersionUID = 1;
        Job _myJob;
        Job _yourJob;

        S(Job m, Job y) {
            _myJob = m;
            _yourJob = y;
        }

        @Override
        public String toString() {
            return _myJob + ":" + _yourJob;
        }

    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        _exchange = _persistit.getExchange("persistit", getClass().getSimpleName(), true);
    }

    @Override
    public void tearDown() throws Exception {
        _persistit.releaseExchange(_exchange);
        _exchange = null;
        super.tearDown();
    }

    @Test
    public void test1() throws PersistitException {
        System.out.print("test1 ");
        S s = new S(Job.COOK, Job.BOTTLEWASHER);
        _exchange.getValue().put(s);
        _exchange.clear().append("test1").store();
        Object x = _exchange.getValue().get();
        assertEquals("COOK:BOTTLEWASHER", x.toString());

        System.out.println("- done");
    }

    public static void main(String[] args) throws Exception {
        new ValueTest5().initAndRunTest();
    }

    @Override
    public void runAllTests() throws Exception {
        _exchange = _persistit.getExchange("persistit", "ValueTest5", true);
        CoderManager cm = _persistit.getCoderManager();

        test1();

        _persistit.setCoderManager(cm);
    }

}
