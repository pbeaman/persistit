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

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.unit.PersistitUnitTestCase;

public class Bug739533Test extends PersistitUnitTestCase {

    private String _volumeName = "persistit";

    @Override
    public void runAllTests() throws Exception {
        // TODO Auto-generated method stub
    }

    @Test
    public void test739544() throws Exception {

        final Exchange exchange = _persistit.getExchange(_volumeName, "Bug739533", true);

        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 65536; i++) {
            sb.append(".");
        }
        final int maxSimpleValueSize = maxValueSize(exchange);
        final int pageSize = exchange.getVolume().getPageSize();

        System.out.println("maxValueSize=" + maxSimpleValueSize);

        for (int k = 100; k < 1990; k += 100) {
            exchange.removeAll();

            for (int i = 0; i < maxSimpleValueSize - 1; i++) {
                sb.append(".");
            }

            exchange.getValue().put(sb.toString().substring(0, pageSize / 2 - 50));

            for (int j = 0; j < 4; j += 2) {
                exchange.clear().append(j);
                exchange.store();
            }

            exchange.getValue().put(sb.toString().substring(0, maxSimpleValueSize - 10));

            for (int j = 1; j < 3; j += 2) {
                exchange.clear().append(j).append(sb.toString().substring(0, k));
                exchange.store();
            }
        }

    }

    /**
     * This is the old computation. The intent is to pick a record that really
     * is too big so that it will exploit the bug.
     * 
     * @param exchange
     * @return
     */
    int maxValueSize(final Exchange exchange) {
        return exchange.getVolume().getPageSize() - Buffer.INDEX_PAGE_OVERHEAD
                - Key.maxStorableKeySize(exchange.getVolume().getPageSize()) * 2;
    }

}
