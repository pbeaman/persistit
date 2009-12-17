/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on May 18, 2005
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
public class JavaHelpAdapterImpl
implements JavaHelpAdapter
{
    String _helpSetName;
    HelpSet _helpSet;
    ActionListener _actionListener;
    
    public void showHelp(ActionEvent ae)
    {
        if (_actionListener != null)
        {
            _actionListener.actionPerformed(ae);
        }
    }
    
    public void create(Object arg, final ActionEvent ae)
    {
        _helpSetName = (String)arg;
        Thread thread = new Thread(new Runnable()
        {
            public void run()
            {
                //  Find the HelpSet file and create the HelpSet object:
                ClassLoader cl = getClass().getClassLoader();
                try
                {
                   URL hsURL = HelpSet.findHelpSet(cl, _helpSetName);
                   _helpSet = new HelpSet(null, hsURL);
                   //  Create a HelpBroker object:
                   HelpBroker hb = _helpSet.createHelpBroker();
                   //  Create a "Help" menu item to trigger the help viewer:
                   _actionListener = new CSH.DisplayHelpFromSource(hb);
                   _actionListener.actionPerformed(ae);
                }
                catch (Exception ee) {
                   // Say what the exception really is
                   System.out.println( "HelpSet " + ee.getMessage());
                   System.out.println("HelpSet "+ _helpSetName +" not found");
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }
    
    public void dispose()
    {
        Frame[] frames = java.awt.Frame.getFrames();
        for (int index = 0; index < frames.length; index++)
        {
            Frame frame = frames[index];
            if (frame.getTitle().indexOf("Persistit AdminUI Help") >= 0)
            {
                frame.dispose();
            }
        }
    }
}
