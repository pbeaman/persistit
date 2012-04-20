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

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;

import org.junit.Test;

import com.persistit.CLI;
import com.persistit.Management;
import com.persistit.PersistitMap;
import com.persistit.TestShim;
import com.persistit.util.Util;

public class CommandLineTest extends PersistitUnitTestCase {

    @Test
    public void testCliParser() throws Exception {
        assertNotNull(TestShim.parseTask(_persistit, "backup file=somefile -a -y -z"));
        assertNotNull(TestShim.parseTask(_persistit, "save trees=persistit file=somefile"));
        assertNotNull(TestShim.parseTask(_persistit, "load trees=persistit:*{1:2} file=somefile -t -n"));
        assertNull(TestShim.parseTask(_persistit, "open datapath=somefile"));
        try {
            TestShim.parseTask(_persistit, "backup file=somefile -s -y -z wrong=parameter");
            fail();
        } catch (Exception e) {
            // okay
        }
    }

    @Test
    public void testCommands() throws Exception {

        final PersistitMap<Integer, String> pmap = new PersistitMap<Integer, String>(_persistit.getExchange(
                "persistit", "CommandLineTest", true));
        for (int index = 0; index < 500; index++) {
            pmap.put(new Integer(index), "This is the record for index=" + index);
        }

        final Management management = _persistit.getManagement();

        String status = management.launch("icheck trees=persistit:CommandLineTest");
        waitForCompletion(taskId(status));
        final File file = File.createTempFile("CommandLineTest", ".sav");
        file.deleteOnExit();
        status = management.launch("save file=" + file + " trees=persistit:CommandLineTest{200:}");
        waitForCompletion(taskId(status));
        pmap.clear();

        status = management.launch("load file=" + file);
        waitForCompletion(taskId(status));

        assertEquals(300, pmap.size());
        
        status = management.launch("jquery -T -V -v page=1");
        waitForCompletion(taskId(status));

    }

    @Test
    public void testScript() throws Exception {
        final PersistitMap<Integer, String> pmap = new PersistitMap<Integer, String>(_persistit.getExchange(
                "persistit", "CommandLineTest", true));
        for (int index = 0; index < 500; index++) {
            pmap.put(new Integer(index), "This is the record for index=" + index);
        }
        _persistit.close();

        final String datapath = _persistit.getProperty("datapath");
        final String rmiport = _persistit.getProperty("rmiport");
        final StringReader stringReader = new StringReader(String.format("help\nopen datapath=%s rmiport=%s\nicheck -v\n", datapath, rmiport));
        final BufferedReader reader = new BufferedReader(stringReader);
        final StringWriter stringWriter = new StringWriter();
        final PrintWriter writer = new PrintWriter(stringWriter);
        CLI.runScript(null, reader, writer);
        final String result = stringWriter.toString();
        assertTrue(result.contains("data"));
    }
    
    private long taskId(final String status) {
        return Long.parseLong(status);
    }

    private void waitForCompletion(final long taskId) throws Exception {
        for (int waiting = 0; waiting < 60; waiting++) {
            final String status = _persistit.getManagement().execute("task taskId=" + taskId);
            if (!status.isEmpty()) {
                String[] s = status.split(Util.NEW_LINE, 2);
                if (s.length == 2) {
                    System.out.println(s[1]);
                }
            } else {
                return;
            }
            
            Thread.sleep(500);
        }
        throw new IllegalStateException("Task " + taskId + " did not compelete within 10 seconds");
    }

    @Override
    public void runAllTests() throws Exception {

    }

}
