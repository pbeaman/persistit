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

package com.persistit.unit;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.persistit.Persistit;
import com.persistit.exception.PersistitException;

/**
 * Create a Persistit instance using Spring Framework.
 * 
 * @author peter
 * 
 */
public class SpringFrameworkConfigurationTest {

    public static class TestClient {
        final Persistit db;

        public TestClient(final Persistit db) {
            this.db = db;
        }

        private void test() {
            System.out.println(db.getVolumes());
            try {
                db.close();
            } catch (final PersistitException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void configurePersistitFromSpring() throws Exception {
        System.setProperty("com.persistit.datapath", UnitTestProperties.DATA_PATH);
        final ApplicationContext context = new ClassPathXmlApplicationContext(
                "com/persistit/unit/SpringFrameworkConfiguraitonTest.xml");

        final Persistit persistit = (Persistit) context.getBean("persistit");
        assertTrue("Persistit should be initialized", persistit.isInitialized());

        final TestClient testClient = (TestClient) context.getBean("testClient");
        testClient.test();
    }

}
