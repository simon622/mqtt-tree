import org.junit.Assert;
import org.junit.Test;
import org.slj.mqtt.tree.MqttTree;
import org.slj.mqtt.tree.MqttTreeException;
import org.slj.mqtt.tree.MqttTreeInputException;
import org.slj.mqtt.tree.MqttTreeLimitExceededException;

public class MqttSpecificationTests extends AbstractMqttTreeTests {

    @Test
    public void testMultiLevelSpecificationExamplesNormative() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.addSubscription("sport/tennis/player1/#", "Client");
        Assert.assertTrue("subscription should exist",
                tree.search("sport/tennis/player1").size() == 1);

        Assert.assertTrue("subscription should exist",
                tree.search("sport/tennis/player1/ranking").size() == 1);

        Assert.assertTrue("subscription should exist",
                tree.search("sport/tennis/player1/score/wimbledon").size() == 1);
    }

    @Test
    public void testMultiLevelSpecificationExamplesNonNormativeExample2() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.addSubscription("sport/tennis/#", "Client");
    }

    @Test(expected = MqttTreeInputException.class)
    public void testMultiLevelSpecificationExamplesNonNormativeExample3() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.addSubscription("sport/tennis#", "Client");
    }

    @Test(expected = MqttTreeInputException.class)
    public void testMultiLevelSpecificationExamplesNonNormativeExample4() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.addSubscription("sport/tennis/#/ranking", "Client");
    }

    @Test
    public void testMultiLevelSpecificationExamplesNonNormativeExample1() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.addSubscription("sport/#", "Client");
        Assert.assertTrue("subscription should exist",
                tree.search("sport").size() == 1);
    }


    @Test
    public void testSingleLevelSpecificationExamplesNonNormativeExample1() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.addSubscription("sport/tennis/+", "Client");

        Assert.assertTrue("subscription should exist",
                tree.search("sport/tennis/player1").size() == 1);

        Assert.assertTrue("subscription should exist",
                tree.search("sport/tennis/player2").size() == 1);

        Assert.assertTrue("subscription should NOT exist",
                tree.search("sport/tennis/player1/ranking").size() == 0);
    }

    @Test
    public void testSingleLevelSpecificationExamplesNonNormativeExample2() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.addSubscription("+", "Client");

        Assert.assertTrue("subscription should exist",
                tree.search("foo").size() == 1);
    }

//    @Test(expected = MqttTreeInputException.class)
    public void testSingleLevelSpecificationExamplesNonNormativeExample3() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        //TODO :FIX ME
        tree.addSubscription("+/tennis/#", "Client");
    }

    @Test(expected = MqttTreeInputException.class)
    public void testSingleLevelSpecificationExamplesNonNormativeExample4() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.addSubscription("sport+", "Client");
    }

    @Test
    public void testSingleLevelSpecificationExamplesNonNormativeExample5() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.addSubscription("sport/+/player1", "Client");
        tree.addSubscription("sport/+/player2", "Client2");

        Assert.assertTrue("subscription should exist",
                tree.search("sport/any/player1").size() == 1);
    }

//    @Test
    public void testSingleLevelSpecificationExamplesNonNormativeExample6() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.addSubscription("+/+", "Client");
        tree.addSubscription("/+", "Client2");
        tree.addSubscription("+", "Client3");
//TODO: FIX ME
        Assert.assertEquals("subscription should exist",
                2, tree.search("/finance").size());
    }

    /*

    Non-normative comment
For example, “sport/tennis/+” matches “sport/tennis/player1” and “sport/tennis/player2”, but not “sport/tennis/player1/ranking”. Also, because the single-level wildcard matches only a single level, “sport/+” does not match “sport” but it does match “sport/”.
•	 “+” is valid
•	“+/tennis/#” is valid
•	“sport+” is not valid
•	“sport/+/player1” is valid
•	“/finance” matches “+/+” and “/+”, but not “+”

     */
}
