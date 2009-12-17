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
 * Created on Mar 21, 2005
 */
package com.persistit.ui;

import java.awt.BorderLayout;
import java.awt.Dimension;

import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import com.persistit.Management;

/**
 * @author Peter Beaman
 * @version 1.0
 */
class InspectorDisplayablePanel
extends AbstractInspector
{
    private JTextArea _textArea;
    
    protected void setup(AdminUI ui, InspectorPanel host)
    {
        super.setup(ui, host);
        setLayout(new BorderLayout());
        _textArea = new JTextArea();
        _textArea.setEditable(false);
        ui.registerTextComponent(_textArea);
        JScrollPane scrollPane = 
            new JScrollPane(
                _textArea,
                JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED,
                JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        scrollPane.setPreferredSize(new Dimension(500, 100));
        scrollPane.setBorder(null);
        add(scrollPane, BorderLayout.CENTER);
    }
    
    protected void refreshed()
    {
        Management.LogicalRecord lr = _host.getLogicalRecord();
        if (lr == null)
        {
            nullData();
        }
        else if (_host.isShowValue())
        {
            _textArea.setText(lr.getValueString());
        }
        else
        {
            _textArea.setText(lr.getKeyString());
        }
    }
       
    void nullMessage()
    {
        _textArea.setText(_adminUI.getNullMessage());
    }
    
    protected void waiting()
    {
        _textArea.setText(_adminUI.getWaitingMessage());
    }
    
}
