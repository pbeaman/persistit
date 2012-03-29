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

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Persistit;

public class HelloWorld {
    public static void main(String[] args) throws Exception {
        Persistit db = new Persistit();
        try {
            // Reads configuration from persistit.properties, allocates
            // buffers, opens Volume(s), and performs recovery processing
            // if necessary.
            //
            db.initialize();
            //
            // Creates an Exchange, which is a thread-private facade for
            // accessing data in a Persistit Tree. This Exchange will
            // access a Tree called "greetings" in a Volume called
            // "hwdemo". It will create a new Tree by that name
            // if one does not already exist.
            //
            Exchange dbex = db.getExchange("hwdemo", "greetings", true);
            //
            // Set up the Value field of the Exchange.
            //
            dbex.getValue().put("World");
            //
            // Set up the Key field of the Exchange.
            //
            dbex.getKey().append("Hello");
            //
            // Ask Persistit to put this key/value pair into the Tree.
            // Until this point, the changes to the Exchange are local
            // to this thread.
            //
            dbex.store();
            //
            // Prepare to traverse all the keys in the Tree (of which there
            // is currently only one!) and for each key display its value.
            //
            dbex.getKey().to(Key.BEFORE);
            while (dbex.next()) {
                System.out.println(dbex.getKey().indexTo(0).decode() + " "
                        + dbex.getValue().get());
            }
            db.releaseExchange(dbex);
        } finally {
            // Always close Persistit. If the application does not do
            // this, Persistit's background threads will keep the JVM from
            // terminating.
            //
            db.close();
        }
    }
}