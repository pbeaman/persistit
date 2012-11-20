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

import com.persistit.Persistit;
import com.persistit.Volume;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


/**
 * Simple example using Spring Framework to configure and initialize a Persistit
 * instance.
 * 
 */
public class SpringFrameworkDemo {

    /**
     * A "client" class that will receive a configured Persistit instance by
     * constructor injection.
     * 
     * @author peter
     * 
     */
    public static class TestClient {
        final Persistit db;

        public TestClient(final Persistit db) {
            this.db = db;
        }

        private void test() {
            if (db == null) {
                System.out.println("Problem: no persistit bean was supplied");
                return;
            }
            if (!db.isInitialized()) {
                System.out.println("Problem: persistit was not initialized");
                return;
            }

            final Volume volume = db.getVolume("springdemo");
            if (volume == null) {
                System.out.println("Problem: no volume named \"springdemo\" was created");
                return;
            }
            
            System.out.println("Success: Persistit was initialized and injected into the TestClient class");

        }
    }

    public static void main(final String[] args) throws Exception {
        ApplicationContext context = new ClassPathXmlApplicationContext("Beans.xml");
        final Persistit db = (Persistit) context.getBean("persistit");
        try {

            final TestClient testClient = (TestClient) context.getBean("testClient");
            testClient.test();

        } finally {
            if (db != null) {
                db.close();
            }
        }
    }

}
