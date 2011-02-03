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

import java.awt.FlowLayout;
import java.rmi.RemoteException;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.text.AttributeSet;
import javax.swing.text.BadLocationException;
import javax.swing.text.PlainDocument;

/**
 * @author Peter Beaman
 * @version 1.0
 */
public class TaskSetupPanel extends Box {
    private AdminUI _adminUI;
    private String _taskClassName;
    private String _taskName;
    private Vector _generalParameterDescriptors = new Vector();
    private Vector _taskSpecificParameterDescriptors = new Vector();
    private String _yesMessage;
    private String _noMessage;

    public TaskSetupPanel(AdminUI ui, String taskSpecification)
            throws Exception {
        super(BoxLayout.Y_AXIS);
        _adminUI = ui;
        _yesMessage = ui.getProperty("YesMessage");
        _noMessage = ui.getProperty("NoMessage");

        String generalParameters = ui.getProperty("TaskDescriptor.General");
        StringTokenizer st;

        st = new StringTokenizer(generalParameters, ",");
        while (st.hasMoreTokens()) {
            String parameterSpec = st.nextToken();
            ParameterComponent pd = taskParameterDescription(parameterSpec);
            _generalParameterDescriptors.add(pd);
        }

        st = new StringTokenizer(taskSpecification, ",");
        _taskName = st.nextToken();
        _taskClassName = st.nextToken();

        while (st.hasMoreTokens()) {
            String parameterSpec = st.nextToken();
            ParameterComponent pd = taskParameterDescription(parameterSpec);
            _taskSpecificParameterDescriptors.add(pd);
        }

        setDescriptionString(_taskName);
        setOwnerString(_adminUI.getHostName());
    }

    ParameterComponent taskParameterDescription(String parameterSpec) {
        StringTokenizer st2 = new StringTokenizer(parameterSpec, ":");
        String type = st2.nextToken();
        String caption = st2.nextToken();
        JLabel label = new JLabel(caption);
        label.setFont(_adminUI.getBoldFont());
        label.setForeground(_adminUI.getPersistitAccentColor());
        JPanel labelPanel = offsetPanel(0);
        labelPanel.add(label);
        labelPanel.add(Box.createHorizontalGlue());
        add(labelPanel);

        JPanel panel = offsetPanel(10);
        JComponent component = null;

        if ("STRING".equals(type)) {
            int columns = 50;
            if (st2.hasMoreTokens()) {
                columns = Integer.parseInt(st2.nextToken());
            }
            String dflt = "";
            if (st2.hasMoreElements()) {
                dflt = st2.nextToken();
            }
            JTextField textField = new JTextField(dflt, columns);
            panel.add(textField);
            panel.add(Box.createHorizontalGlue());
            add(panel);
            return new ParameterComponent(type, textField);
        }

        if ("TREES".equals(type)) {
            TreeAndVolumeSelector tavSelector = new TreeAndVolumeSelector();
            tavSelector.setup(_adminUI);
            panel.add(tavSelector);
            add(panel);
            panel.add(Box.createHorizontalGlue());
            return new ParameterComponent(type, tavSelector);
        }

        if ("BOOLEAN".equals(type)) {
            JRadioButton yesButton = new JRadioButton(_yesMessage);
            JRadioButton noButton = new JRadioButton(_noMessage);
            ButtonGroup group = new ButtonGroup();
            group.add(yesButton);
            group.add(noButton);
            String dflt = "false";
            if (st2.hasMoreTokens()) {
                dflt = st2.nextToken();
            }
            if ("true".equals(dflt))
                yesButton.setSelected(true);
            else
                noButton.setSelected(true);
            panel.add(yesButton);
            panel.add(noButton);
            panel.add(Box.createHorizontalGlue());
            add(panel);
            return new ParameterComponent(type, yesButton);
        }

        if ("INTEGER".equals(type)) {
            String dflt = "";
            if (st2.hasMoreTokens()) {
                dflt = st2.nextToken();
            }
            JTextField textField = new JTextField(10);
            textField.setDocument(new PlainDocument() {
                public void insertString(int offs, String str, AttributeSet a)
                        throws BadLocationException {
                    boolean okay = true;
                    for (int index = 0; index < str.length() && okay; index++) {
                        if (!Character.isDigit(str.charAt(index)))
                            okay = false;
                    }
                    if (okay)
                        super.insertString(offs, str, a);
                }
            });
            textField.setText(dflt);
            panel.add(textField);
            panel.add(Box.createHorizontalGlue());
            add(panel);
            return new ParameterComponent(type, textField);
        }

        throw new RuntimeException(
                "Misconfigured admin properties: no such task parameter type "
                        + type);

    }

    JPanel offsetPanel(int size) {
        JPanel panel = new JPanel();
        panel.setLayout(new FlowLayout(FlowLayout.LEFT, 2, 2));
        if (size > 0)
            panel.add(Box.createHorizontalStrut(size));
        return panel;
    }

    private static class ParameterComponent {
        String _type;
        JComponent _component;

        ParameterComponent(String type, JComponent component) {
            _type = type;
            _component = component;
        }

        void setStringValue(String s) {
            if ("STRING".equals(_type) || "INTEGER".equals(_type)) {
                ((JTextField) _component).setText(s);
            }
        }

        String getStringValue() {
            if ("STRING".equals(_type) || "INTEGER".equals(_type)) {
                return ((JTextField) _component).getText();
            }

            if ("BOOLEAN".equals(_type)) {
                return (((JRadioButton) _component).isSelected()) ? Boolean.TRUE
                        .toString() : Boolean.FALSE.toString();
            }

            if ("TREES".equals(_type)) {
                return ((TreeAndVolumeSelector) _component).getTreeListString();
            }
            throw new RuntimeException();
        }

        boolean getBooleanValue() {
            if ("BOOLEAN".equals(_type)) {
                return ((JRadioButton) _component).isSelected();
            }
            throw new RuntimeException();
        }

        int getIntValue() {
            if ("INTEGER".equals(_type)) {
                return Integer.parseInt(((JTextField) _component).getText());
            }
            throw new RuntimeException();
        }
    }

    void refresh(boolean reset) throws RemoteException {
        for (int index = 0; index < _taskSpecificParameterDescriptors.size(); index++) {
            ParameterComponent pc = (ParameterComponent) _taskSpecificParameterDescriptors
                    .get(index);
            if (pc._component instanceof AdminPanel) {
                ((AdminPanel) pc._component).refresh(reset);
            }
        }
    }

    String getTaskName() {
        return _taskName;
    }

    String getTaskClassName() {
        return _taskClassName;
    }

    String[] argStrings() {
        String[] results = new String[_taskSpecificParameterDescriptors.size()];
        for (int index = 0; index < results.length; index++) {
            ParameterComponent pc = (ParameterComponent) _taskSpecificParameterDescriptors
                    .get(index);
            results[index] = pc.getStringValue();
        }
        return results;
    }

    String getDescriptionString() {
        ParameterComponent pc = (ParameterComponent) _generalParameterDescriptors
                .get(0);
        return pc.getStringValue();
    }

    void setDescriptionString(String description) {
        ParameterComponent pc = (ParameterComponent) _generalParameterDescriptors
                .get(0);
        pc.setStringValue(description);
    }

    String getOwnerString() {
        ParameterComponent pc = (ParameterComponent) _generalParameterDescriptors
                .get(1);
        return pc.getStringValue();
    }

    void setOwnerString(String owner) {
        ParameterComponent pc = (ParameterComponent) _generalParameterDescriptors
                .get(1);
        pc.setStringValue(owner);
    }

    boolean isVerboseEnabled() {
        ParameterComponent pc = (ParameterComponent) _generalParameterDescriptors
                .get(2);
        return pc.getBooleanValue();
    }

    long getExpirationTime() {
        ParameterComponent pc = (ParameterComponent) _generalParameterDescriptors
                .get(3);
        return (long) pc.getIntValue() * 1000L;
    }
}
