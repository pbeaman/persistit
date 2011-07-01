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
import java.awt.event.ActionEvent;

import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.ScrollPaneConstants;

import com.persistit.Management;
import com.persistit.util.Util;

/**
 * @author Peter Beaman
 * @version 1.0
 */
class InspectorHexPanel extends AbstractInspector {
    private JTextArea _textArea;

    @Override
    protected void setup(AdminUI ui, InspectorPanel host) {
        super.setup(ui, host);
        setLayout(new BorderLayout());
        _textArea = new JTextArea();
        _textArea.setEditable(false);
        _textArea.setFont(_adminUI.getFixedFont());
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
            _textArea.setText(Util.dump(lr.getValueState()));
        } else {
            _textArea.setText(Util.dump(_host.getLogicalRecord().getKeyState()));
        }
    }

    @Override
    protected void nullData() {
        _textArea.setText(_adminUI.getNullMessage());
    }

    @Override
    protected void waiting() {
        _textArea.setText(_adminUI.getWaitingMessage());
    }

    protected void setDefaultButton() {
        // No default button
    }

    public void actionPerformed(AdminUI.AdminAction action, ActionEvent ae) {
        // no actions.
    }

}
