/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
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
    protected void setup(final AdminUI ui, final InspectorPanel host) {
        super.setup(ui, host);
        setLayout(new BorderLayout());
        _textArea = new JTextArea();
        _textArea.setEditable(false);
        _textArea.setFont(_adminUI.getFixedFont());
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

    public void actionPerformed(final AdminUI.AdminAction action, final ActionEvent ae) {
        // no actions.
    }

}
