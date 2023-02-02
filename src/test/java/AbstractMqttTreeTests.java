import org.slj.mqtt.tree.MqttTree;

import java.util.concurrent.ThreadLocalRandom;

public abstract class AbstractMqttTreeTests {

    public static String generateRandomTopic(int segments){
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < segments; i++){
            if(i == 0) sb.append("/");
            int r = ThreadLocalRandom.current().nextInt(0, 1000);
            sb.append(r);
            sb.append("/");
        }
        return sb.toString();
    }

    public static String generateTopicMaxLength(int length){
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++){
            sb.append("a");
        }
        return sb.toString();
    }

    protected static MqttTree<String> createTreeDefaultConfig(){
        MqttTree<String> tree = new MqttTree<>(MqttTree.DEFAULT_SPLIT, true);
        tree.withMaxMembersAtLevel(1000000);
        return tree;
    }
}
