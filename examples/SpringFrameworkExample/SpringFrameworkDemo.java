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
