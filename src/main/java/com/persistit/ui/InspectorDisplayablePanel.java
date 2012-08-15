/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit.ui;

import java.awt.BorderLayout;
import java.awt.Dimension;

import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.ScrollPaneConstants;

import com.persistit.Management;

/**
 * @author Peter Beaman
 * @version 1.0
 */
class InspectorDisplayablePanel extends AbstractInspector {
    private JTextArea _textArea;

    @Override
    protected void setup(final AdminUI ui, final InspectorPanel host) {
        super.setup(ui, host);
        setLayout(new BorderLayout());
        _textArea = new JTextArea();
        _textArea.setEditable(false);
        ui.registerTextComponent(_textArea);
        final JScrollPane scrollPane = new JScrollPane(_textArea, ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED,
                ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        scrollPane.setPreferredSize(new Dimension(500, 100));
        scrollPane.setBorder(null);
        add(scrollPane, BorderLayout.CENTER);
    }

    @Override
    protected void refreshed() {
        final Management.LogicalRecord lr = _host.getLogicalRecord();
        if (lr == null) {
            nullData();
        } else if (_host.isShowValue()) {
            _textArea.setText(lr.getValueString());
        } else {
            _textArea.setText(lr.getKeyString());
        }
    }

    void nullMessage() {
        _textArea.setText(_adminUI.getNullMessage());
    }

    @Override
    protected void waiting() {
        _textArea.setText(_adminUI.getWaitingMessage());
    }

}
