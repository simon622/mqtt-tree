package org.slj.mqtt.tree.ui;

import org.slj.mqtt.tree.*;

import javax.swing.*;
import java.io.IOException;

public class MqttTreeViewerLauncher {


    public static void main(String[] args) throws MqttTreeLimitExceededException, MqttTreeException {

        ISearchableMqttTree<String> tree = new SearchableMqttTree(
                new MqttTree<String>(MqttTree.DEFAULT_SPLIT, true));
        tree.addSubscription("/some/path/with/some/members", "Member1");
        tree.addSubscription("/some/other/with/some/members", "Member1");
        tree.addSubscription("another/topic/without/leading/slash", "Member1");
        tree.addSubscription("another/#", "Member1");
        tree.addSubscription("another/+/with/something/else", "Member1");


        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                try {
                    new MqttTreeViewer(tree);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
