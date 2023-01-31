package org.slj.mqtt.tree.ui;

import org.slj.mqtt.tree.MqttTree;
import org.slj.mqtt.tree.MqttTreeNode;

import javax.swing.*;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.io.IOException;
import java.util.Set;

public class MqttTreeViewer  extends JFrame {
    private JTree tree;
    private JTree searchTree;
    private JLabel selectedLabel;
    private JTextField searchBar;
    private JTextField connectionBar;

    private final MqttTree mqttTree;


    public MqttTreeViewer(MqttTree mqttTree) throws IOException {
        this.mqttTree = mqttTree;

        createConnectionBar();
        createSearchBar();

        DefaultMutableTreeNode root = readNodes(mqttTree);
        createTree(root);

        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        this.setTitle("MQTT Tree Browser");
        this.setSize(400, 400);
        this.setVisible(true);
    }

    protected DefaultMutableTreeNode readNodes(MqttTree mqttTree){

        MqttTreeNode node = mqttTree.getRootNode();
        return readTree(null, node);
    }

    protected DefaultMutableTreeNode readTree(DefaultMutableTreeNode parent, MqttTreeNode node){

        DefaultMutableTreeNode current = null;
        if(parent == null){
            parent = new DefaultMutableTreeNode("MQTT Subscriptions");
            current = parent;
        } else {
            current = new DefaultMutableTreeNode(node.getPathSegment());
            parent.add(current);
        }

        current.setUserObject(node);
        Set<String> children = node.getChildPaths();
        for (String child : children){
            MqttTreeNode childNode = node.getChild(child);
            readTree(current, childNode);
        }
        return parent;
    }

    protected void createSearchBar() throws IOException {

        searchBar = new JFormattedTextField();
        searchBar.setText("Filter Subscriptions..");
        searchBar.addKeyListener(new KeyListener() {
            @Override
            public void keyTyped(KeyEvent e) {

            }

            @Override
            public void keyPressed(KeyEvent e) {

            }

            @Override
            public void keyReleased(KeyEvent e) {

            }
        });
        add(searchBar, BorderLayout.PAGE_START);
    }

    protected void createConnectionBar() throws IOException {

        connectionBar = new JFormattedTextField();
        connectionBar.setText("rmi://localhost");
        add(connectionBar, BorderLayout.SOUTH);
    }

    protected void createTree(DefaultMutableTreeNode root) throws IOException {
        //create the tree by passing in the root node
        tree = new JTree(root){
            @Override
            public String convertValueToText(Object value, boolean selected, boolean expanded, boolean leaf, int row, boolean hasFocus) {
                if(value != null){
                    DefaultMutableTreeNode node = (DefaultMutableTreeNode) value;
                    MqttTreeNode treeNode = (MqttTreeNode) node.getUserObject();
                    if(treeNode.isRoot()){
                        return "MQTT Subscriptions";
                    } else {
                        if(treeNode.hasMembers()){
                            return treeNode.getPathSegment() + " (" + treeNode.getMemberCount() + ")";
                        } else {
                            return treeNode.getPathSegment();
                        }
                    }
                } else {
                    return super.convertValueToText(value, selected, expanded, leaf, row, hasFocus);
                }
            }
        };
        ImageIcon folderIcon = MqttViewerUtils.loadIcon("folder.gif");
        ImageIcon leafIcon = MqttViewerUtils.loadIcon("cellphone.gif");
        DefaultTreeCellRenderer renderer = new DefaultTreeCellRenderer();
        renderer.setLeafIcon(leafIcon);
        renderer.setOpenIcon(folderIcon);
        renderer.setClosedIcon(folderIcon);
        tree.setCellRenderer(renderer);
        tree.setShowsRootHandles(true);
        tree.setRootVisible(true);
        add(new JScrollPane(tree));

        selectedLabel = new JLabel();
        add(selectedLabel, BorderLayout.SOUTH);
        tree.getSelectionModel().addTreeSelectionListener(e -> {
            DefaultMutableTreeNode selectedNode = (DefaultMutableTreeNode) tree.getLastSelectedPathComponent();
            if(selectedNode != null){
                MqttTreeNode node = (MqttTreeNode) selectedNode.getUserObject();
                selectedLabel.setText(node.toPath(true));
            }
        });
    }
}