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
    protected void setup(final AdminUI ui, final InspectorPanel host) {
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
            public void valueChanged(final TreeSelectionEvent tse) {
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
        final Management management = _adminUI.getManagement();
        final Management.LogicalRecord lr = _host.getLogicalRecord();
        if (management == null || lr == null) {
            nullMessage();
        } else if (_host.isShowValue()) {
            final ValueState valueState = lr.getValueState();
            try {
                final Object[] results = management.decodeValueObjects(valueState, null);
                final ValueInspectorTreeNode root = new ValueInspectorTreeNode(null, results, "Value", Object[].class);
                _treeModel.setRoot(root);
                _textArea.setText(null);

            } catch (final Exception e) {
                _host.setLogicalRecord(null);
                _adminUI.postException(e);
            }
        } else {
            final KeyState keyState = lr.getKeyState();
            try {
                final Object[] results = management.decodeKeyObjects(keyState, null);

                final ValueInspectorTreeNode root = new ValueInspectorTreeNode(null, results, "KeySegments",
                        Object[].class);
                _treeModel.setRoot(root);
            } catch (final Exception e) {
                _host.setLogicalRecord(null);
                _adminUI.postException(e);
            }
        }
    }

    private Object getObject(final ValueState vs) {
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
        protected Class loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
            if (!name.startsWith("com.persistit")) {
                return super.loadClass(name, resolve);
            } else {
                final Class clazz = findClass(name);
                if (resolve)
                    resolveClass(clazz);
                return clazz;
            }
        }

        @Override
        protected Class findClass(final String name) throws ClassNotFoundException {
            Class clazz = null;
            if (_adminUI.getManagement() != null) {
                try {
                    System.out.println("RemoteClassLoader is attempting to load class " + name);
                    clazz = _adminUI.getManagement().getRemoteClass(name);
                } catch (final RemoteException re) {
                    _adminUI.postException(re);
                }
            }
            if (clazz == null)
                throw new ClassNotFoundException(name);
            return clazz;
        }
    }
}
