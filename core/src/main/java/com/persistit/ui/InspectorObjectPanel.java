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
import java.rmi.RemoteException;

import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.JTree;
import javax.swing.ScrollPaneConstants;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultTreeModel;

import com.persistit.KeyState;
import com.persistit.Management;
import com.persistit.ValueState;

/**
 * @author Peter Beaman
 * @version 1.0
 */
class InspectorObjectPanel extends AbstractInspector {

    private static final long serialVersionUID = -6373326253805580551L;

    private DefaultTreeModel _treeModel;
    private JTree _tree;
    private JTextArea _textArea;

    @Override
    protected void setup(AdminUI ui, InspectorPanel host) {
        super.setup(ui, host);
        _treeModel = new DefaultTreeModel(null);
        _tree = new JTree(_treeModel);
        setLayout(new BorderLayout());
        ui.registerTextComponent(_tree);
        _textArea = new JTextArea(0, 0);
        ui.registerTextComponent(_textArea);

        final JScrollPane treeScrollPane = new JScrollPane(_tree, ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED,
                ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        treeScrollPane.setPreferredSize(new Dimension(500, 100));
        treeScrollPane.setBorder(null);

        final JScrollPane textScrollPane = new JScrollPane(_textArea, ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED,
                ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        textScrollPane.setPreferredSize(new Dimension(500, 0));
        textScrollPane.setBorder(null);

        final JSplitPane splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, treeScrollPane, textScrollPane);
        splitPane.setResizeWeight(1.0);
        splitPane.setOneTouchExpandable(true);
        splitPane.setDividerLocation(Integer.MAX_VALUE);

        _tree.getSelectionModel().addTreeSelectionListener(new TreeSelectionListener() {
            @Override
            public void valueChanged(TreeSelectionEvent tse) {
                final ValueInspectorTreeNode node = (ValueInspectorTreeNode) tse.getPath().getLastPathComponent();
                if (node != null && tse.isAddedPath()) {
                    if (node.getEvalString() == null && textScrollPane.getHeight() != 0) {
                        _textArea.setText(null);
                        node.evalString(new Runnable() {
                            @Override
                            public void run() {
                                _textArea.setText(node.getEvalString());
                            }
                        });
                        ;
                    } else {
                        _textArea.setText(node.getEvalString());
                    }
                }
            }
        });

        add(splitPane, BorderLayout.CENTER);
    }

    @Override
    protected void refreshed() {
        Management management = _adminUI.getManagement();
        Management.LogicalRecord lr = _host.getLogicalRecord();
        if (management == null || lr == null) {
            nullMessage();
        } else if (_host.isShowValue()) {
            ValueState valueState = lr.getValueState();
            try {
                Object[] results = management.decodeValueObjects(valueState, null);
                ValueInspectorTreeNode root = new ValueInspectorTreeNode(null, results, "Value", Object[].class);
                _treeModel.setRoot(root);
                _textArea.setText(null);

            } catch (Exception e) {
                _host.setLogicalRecord(null);
                _adminUI.postException(e);
            }
        } else {
            KeyState keyState = lr.getKeyState();
            try {
                Object[] results = management.decodeKeyObjects(keyState, null);

                ValueInspectorTreeNode root = new ValueInspectorTreeNode(null, results, "KeySegments", Object[].class);
                _treeModel.setRoot(root);
            } catch (Exception e) {
                _host.setLogicalRecord(null);
                _adminUI.postException(e);
            }
        }
    }

    private Object getObject(ValueState vs) {
        return null;
    }

    void nullMessage() {
        _treeModel.setRoot(null);
    }

    @Override
    protected void waiting() {
        _treeModel.setRoot(new ValueInspectorTreeNode(null, _adminUI.getWaitingMessage(), "", String.class));
    }

    class RemoteClassLoader extends ClassLoader {
        RemoteClassLoader() {
            super(RemoteClassLoader.class.getClassLoader());
        }

        @Override
        protected Class loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (!name.startsWith("com.persistit")) {
                return super.loadClass(name, resolve);
            } else {
                Class clazz = findClass(name);
                if (resolve)
                    resolveClass(clazz);
                return clazz;
            }
        }

        @Override
        protected Class findClass(String name) throws ClassNotFoundException {
            Class clazz = null;
            if (_adminUI.getManagement() != null) {
                try {
                    System.out.println("RemoteClassLoader is attempting to load class " + name);
                    clazz = _adminUI.getManagement().getRemoteClass(name);
                } catch (RemoteException re) {
                    _adminUI.postException(re);
                }
            }
            if (clazz == null)
                throw new ClassNotFoundException(name);
            return clazz;
        }
    }
}
