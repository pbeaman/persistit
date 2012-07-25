/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit.unit;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.persistit.Persistit;
import com.persistit.PersistitUnitTestCase;
import com.persistit.exception.PersistitException;
import com.persistit.logging.Log4JAdapter;

public class Log4JLoggerTest extends PersistitUnitTestCase {

    @Test
    public void test1() throws PersistitException {
        System.out.print("test1 ");
        System.out.println("- done");
    }

    public static void main(final String[] args) throws Exception {
        new Log4JLoggerTest().runAllTests();
    }

    @Override
    public void runAllTests() throws Exception {
        final Logger logger = Logger.getLogger("com.persistit.unit.JDK14LoggerTest");
        // Set up a simple configuration that logs on the console.
        BasicConfigurator.configure();
        Persistit persistit = new Persistit();
        persistit.setPersistitLogger(new Log4JAdapter(logger));
        persistit.initialize(UnitTestProperties.getAlternateProperties(true));
        try {
            test1();
        } finally {
            persistit.close();
        }
    }

}
