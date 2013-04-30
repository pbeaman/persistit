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

import java.awt.FlowLayout;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

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
    private final AdminUI _adminUI;
    private final String _commandName;
    private final String _taskName;
    private final List<ParameterComponent> _generalParameterDescriptors = new ArrayList<ParameterComponent>();
    private final List<ParameterComponent> _taskSpecificParameterDescriptors = new ArrayList<ParameterComponent>();
    private final String _yesMessage;
    private final String _noMessage;

    public TaskSetupPanel(final AdminUI ui, final String taskSpecification) throws Exception {
        super(BoxLayout.Y_AXIS);
        _adminUI = ui;
        _yesMessage = ui.getProperty("YesMessage");
        _noMessage = ui.getProperty("NoMessage");

        final String generalParameters = ui.getProperty("TaskDescriptor.General");
        StringTokenizer st;

        st = new StringTokenizer(generalParameters, ",");
        while (st.hasMoreTokens()) {
            final String parameterSpec = st.nextToken();
            final ParameterComponent pd = taskParameterDescription(parameterSpec);
            _generalParameterDescriptors.add(pd);
        }

        st = new StringTokenizer(taskSpecification, ",");
        _taskName = st.nextToken();
        _commandName = st.nextToken();

        while (st.hasMoreTokens()) {
            final String parameterSpec = st.nextToken();
            final ParameterComponent pd = taskParameterDescription(parameterSpec);
            _taskSpecificParameterDescriptors.add(pd);
        }

        setDescriptionString(_taskName);
        setOwnerString(_adminUI.getHostName());
    }

    ParameterComponent taskParameterDescription(final String parameterSpec) {
        final StringTokenizer st2 = new StringTokenizer(parameterSpec, ":");
        final String name = st2.nextToken();
        final String type = st2.nextToken();
        final String caption = st2.nextToken();
        final JLabel label = new JLabel(caption);
        label.setFont(_adminUI.getBoldFont());
        label.setForeground(_adminUI.getPersistitAccentColor());
        final JPanel labelPanel = offsetPanel(0);
        labelPanel.add(label);
        labelPanel.add(Box.createHorizontalGlue());
        add(labelPanel);

        final JPanel panel = offsetPanel(10);

        if ("STRING".equals(type) || "LINE".equals(type)) {
            int columns = 50;
            if (st2.hasMoreTokens()) {
                columns = Integer.parseInt(st2.nextToken());
            }
            String dflt = "";
            if (st2.hasMoreElements()) {
                dflt = st2.nextToken();
            }
            final JTextField textField = new JTextField(dflt, columns);
            panel.add(textField);
            panel.add(Box.createHorizontalGlue());
            add(panel);
            return new ParameterComponent(name, type, textField);
        }

        if ("TREES".equals(type)) {
            final TreeAndVolumeSelector tavSelector = new TreeAndVolumeSelector();
            tavSelector.setup(_adminUI);
            panel.add(tavSelector);
            add(panel);
            panel.add(Box.createHorizontalGlue());
            return new ParameterComponent(name, type, tavSelector);
        }

        if ("BOOLEAN".equals(type)) {
            final JRadioButton yesButton = new JRadioButton(_yesMessage);
            final JRadioButton noButton = new JRadioButton(_noMessage);
            final ButtonGroup group = new ButtonGroup();
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
            return new ParameterComponent(name, type, yesButton);
        }

        if ("INTEGER".equals(type)) {
            String dflt = "";
            if (st2.hasMoreTokens()) {
                dflt = st2.nextToken();
            }
            final JTextField textField = new JTextField(10);
            textField.setDocument(new PlainDocument() {
                @Override
                public void insertString(final int offs, final String str, final AttributeSet a)
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
            return new ParameterComponent(name, type, textField);
        }

        throw new RuntimeException("Misconfigured admin properties: no such task parameter type " + type);

    }

    JPanel offsetPanel(final int size) {
        final JPanel panel = new JPanel();
        panel.setLayout(new FlowLayout(FlowLayout.LEFT, 2, 2));
        if (size > 0)
            panel.add(Box.createHorizontalStrut(size));
        return panel;
    }

    private static class ParameterComponent {
        String _name;
        String _type;
        JComponent _component;

        ParameterComponent(final String name, final String type, final JComponent component) {
            _name = name;
            _type = type;
            _component = component;
        }

        void setStringValue(final String s) {
            if ("STRING".equals(_type) || "INTEGER".equals(_type) || "LINE".equals(_type)) {
                ((JTextField) _component).setText(s);
            }
        }

        String getStringValue() {
            if ("STRING".equals(_type) || "INTEGER".equals(_type) || "LINE".equals(_type)) {
                return ((JTextField) _component).getText();
            }

            if ("BOOLEAN".equals(_type)) {
                return (((JRadioButton) _component).isSelected()) ? Boolean.TRUE.toString() : Boolean.FALSE.toString();
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

    void refresh(final boolean reset) throws RemoteException {
        for (int index = 0; index < _taskSpecificParameterDescriptors.size(); index++) {
            final ParameterComponent pc = _taskSpecificParameterDescriptors.get(index);
            if (pc._component instanceof AdminPanel) {
                ((AdminPanel) pc._component).refresh(reset);
            }
        }
    }

    String getTaskName() {
        return _taskName;
    }

    String getCommandName() {
        return _commandName;
    }

    String getCommandLine() {
        final StringBuilder sb = new StringBuilder(_commandName);
        for (final ParameterComponent pc : _taskSpecificParameterDescriptors) {
            sb.append(' ');
            if ("BOOLEAN".equals(pc._type)) {
                final String flag = pc._name;
                final boolean invert = flag.endsWith("~");
                if (invert ^ pc.getBooleanValue()) {
                    sb.append("-");
                    sb.append(flag.charAt(0));
                }
            } else if ("LINE".equals(pc._type)) {
                sb.setLength(0);
                sb.append(pc.getStringValue());
            } else {
                sb.append(pc._name);
                sb.append('=');
                if ("INTEGER".equals(pc._type)) {
                    sb.append(pc.getIntValue());
                } else {
                    sb.append(pc.getStringValue());
                }
            }
        }
        return sb.toString();
    }

    String[] argStrings() {
        final String[] results = new String[_taskSpecificParameterDescriptors.size()];
        for (int index = 0; index < results.length; index++) {
            final ParameterComponent pc = _taskSpecificParameterDescriptors.get(index);
            results[index] = pc.getStringValue();
        }
        return results;
    }

    String getDescriptionString() {
        final ParameterComponent pc = _generalParameterDescriptors.get(0);
        return pc.getStringValue();
    }

    void setDescriptionString(final String description) {
        final ParameterComponent pc = _generalParameterDescriptors.get(0);
        pc.setStringValue(description);
    }

    String getOwnerString() {
        final ParameterComponent pc = _generalParameterDescriptors.get(1);
        return pc.getStringValue();
    }

    void setOwnerString(final String owner) {
        final ParameterComponent pc = _generalParameterDescriptors.get(1);
        pc.setStringValue(owner);
    }

    boolean isVerboseEnabled() {
        final ParameterComponent pc = _generalParameterDescriptors.get(2);
        return pc.getBooleanValue();
    }

    long getExpirationTime() {
        final ParameterComponent pc = _generalParameterDescriptors.get(3);
        return pc.getIntValue() * 1000L;
    }
}
