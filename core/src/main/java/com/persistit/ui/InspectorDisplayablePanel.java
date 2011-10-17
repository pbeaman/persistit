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
    protected void setup(AdminUI ui, InspectorPanel host) {
        super.setup(ui, host);
        setLayout(new BorderLayout());
        _textArea = new JTextArea();
        _textArea.setEditable(false);
        ui.registerTextComponent(_textArea);
        JScrollPane scrollPane = new JScrollPane(_textArea, ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED,
                ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        scrollPane.setPreferredSize(new Dimension(500, 100));
        scrollPane.setBorder(null);
        add(scrollPane, BorderLayout.CENTER);
    }

    @Override
    protected void refreshed() {
        Management.LogicalRecord lr = _host.getLogicalRecord();
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
