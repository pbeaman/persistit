/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.persistit.ui.help;

import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.net.URL;

import javax.help.CSH;
import javax.help.HelpBroker;
import javax.help.HelpSet;

import com.persistit.ui.JavaHelpAdapter;

/**
 * @author Peter Beaman
 * @version 1.0
 */
public class JavaHelpAdapterImpl implements JavaHelpAdapter {
    String _helpSetName;
    HelpSet _helpSet;
    ActionListener _actionListener;

    public void showHelp(ActionEvent ae) {
        if (_actionListener != null) {
            _actionListener.actionPerformed(ae);
        }
    }

    public void create(Object arg, final ActionEvent ae) {
        _helpSetName = (String) arg;
        Thread thread = new Thread(new Runnable() {
            public void run() {
                // Find the HelpSet file and create the HelpSet object:
                ClassLoader cl = getClass().getClassLoader();
                try {
                    URL hsURL = HelpSet.findHelpSet(cl, _helpSetName);
                    _helpSet = new HelpSet(null, hsURL);
                    // Create a HelpBroker object:
                    HelpBroker hb = _helpSet.createHelpBroker();
                    // Create a "Help" menu item to trigger the help viewer:
                    _actionListener = new CSH.DisplayHelpFromSource(hb);
                    _actionListener.actionPerformed(ae);
                } catch (Exception ee) {
                    // Say what the exception really is
                    System.out.println("HelpSet " + ee.getMessage());
                    System.out
                            .println("HelpSet " + _helpSetName + " not found");
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    public void dispose() {
        Frame[] frames = java.awt.Frame.getFrames();
        for (int index = 0; index < frames.length; index++) {
            Frame frame = frames[index];
            if (frame.getTitle().indexOf("Persistit AdminUI Help") >= 0) {
                frame.dispose();
            }
        }
    }
}
