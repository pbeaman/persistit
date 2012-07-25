/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
 */

package com.persistit;

import org.junit.Test;

import com.persistit.Exchange;

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
