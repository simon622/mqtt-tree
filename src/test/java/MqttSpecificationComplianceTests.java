import org.junit.Assert;
import org.junit.Test;
import org.slj.mqtt.tree.MqttTree;
import org.slj.mqtt.tree.MqttTreeException;
import org.slj.mqtt.tree.MqttTreeInputException;
import org.slj.mqtt.tree.MqttTreeLimitExceededException;

import java.util.Set;

public class MqttSpecificationComplianceTests extends AbstractMqttTreeTests {

    @Test
    public void testMultiLevelSpecificationExamplesNormative() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.subscribe("sport/tennis/player1/#", "Client");
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
        tree.subscribe("sport/tennis/#", "Client");
    }

    @Test(expected = MqttTreeInputException.class)
    public void testMultiLevelSpecificationExamplesNonNormativeExample3() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.subscribe("sport/tennis#", "Client");
    }

    @Test(expected = MqttTreeInputException.class)
    public void testMultiLevelSpecificationExamplesNonNormativeExample4() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.subscribe("sport/tennis/#/ranking", "Client");
    }

    @Test
    public void testMultiLevelSpecificationExamplesNonNormativeExample1() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.subscribe("sport/#", "Client");
        Assert.assertTrue("subscription should exist",
                tree.search("sport").size() == 1);
    }


    @Test
    public void testSingleLevelSpecificationExamplesNonNormativeExample1() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.subscribe("sport/tennis/+", "Client");

        System.err.println(tree.toTree(System.lineSeparator()));

        Assert.assertTrue("subscription should exist",
                tree.search("sport/tennis/player1").size() == 1);

        Assert.assertTrue("subscription should exist",
                tree.search("sport/tennis/player2").size() == 1);

        Assert.assertTrue("subscription should NOT exist",
                tree.search("sport/tennis/player1/ranking").size() == 0);
    }

    @Test
    public void testSingleLevelSpecificationExamplesNonNormativeExample1b() throws MqttTreeException, MqttTreeLimitExceededException {

        //because the single-level wildcard matches only a single level, “sport/+” does not match “sport” but it does match “sport/”.
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.subscribe("sport/+", "Client1");

        Assert.assertTrue("subscription should NOT exist",
                tree.search("sport").size() == 0);

        Assert.assertTrue("subscription should exist",
                tree.search("sport/").size() == 1);
    }

    @Test
    public void testSingleLevelSpecificationExamplesNonNormativeExample2() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.subscribe("+", "Client");

        Assert.assertTrue("subscription should exist",
                tree.search("foo").size() == 1);
    }

    @Test
    public void testSingleLevelSpecificationExamplesNonNormativeExample3() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.subscribe("+/tennis/#", "Client");

        Assert.assertTrue("subscription should exist",
                tree.search("foo/tennis/player1").size() == 1);

        Assert.assertTrue("subscription should NOT exist",
                tree.search("foo/ten/player1").size() == 0);
    }

    @Test(expected = MqttTreeInputException.class)
    public void testSingleLevelSpecificationExamplesNonNormativeExample4() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.subscribe("sport+", "Client");
    }

    @Test
    public void testSingleLevelSpecificationExamplesNonNormativeExample5() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.subscribe("sport/+/player1", "Client");
        tree.subscribe("sport/+/player2", "Client2");

        Assert.assertTrue("subscription should exist",
                tree.search("sport/any/player1").size() == 1);
    }

    @Test
    public void testSingleLevelSpecificationExamplesNonNormativeExample6() throws MqttTreeException, MqttTreeLimitExceededException {

        //“/finance” matches “+/+” and “/+”, but not “+”
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.subscribe("+", "Client1");
        tree.subscribe("+/+", "Client2");
        tree.subscribe("/+", "Client3");

        Set<String> s = tree.search("/finance");

        Assert.assertEquals("subscription should exist",
                2, s.size());
    }

    @Test
    public void testSingleLevelSpecificationExamplesNonNormativeExample6Edge() throws MqttTreeException, MqttTreeLimitExceededException {

        MqttTree<String> tree = createTreeDefaultConfig();
        tree.subscribe("+/+", "Client");

        System.err.println(tree.getDistinctPaths(true));

        Set<String> s = tree.search("/");
        System.err.println(s);
        Assert.assertEquals("subscription should exist",
                1, s.size());
    }

    @Test(expected = MqttTreeInputException.class)
    public void testSemanticExample1() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.subscribe("", "Client1");
    }

    @Test
    public void testSemanticExample4() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.subscribe("/leading/", "Client1");
        tree.subscribe("leading/", "Not1");
        tree.subscribe("/leading", "Not2");

        Set<String> m = tree.search("/leading/");
        Assert.assertEquals("subscription should exist", 1,
                m.size());

        Assert.assertEquals("subscription should exist",
                m.stream().findFirst().get(), "Client1");
    }

    @Test
    public void testSemanticExample5() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.subscribe("/", "Client1");

        Set<String> m = tree.search("/");
        Assert.assertEquals("subscription should exist", 1,
                m.size());

        Assert.assertEquals("subscription should exist",
                m.stream().findFirst().get(), "Client1");
    }

    @Test
    public void testSemanticExample8() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.subscribe("ACCOUNTS", "Client1");
        tree.subscribe("accounts", "Client2");
        Assert.assertEquals("should be 2 distinct branches", 2, tree.getBranchCount());
        Assert.assertTrue("subscription should exist",
                tree.search("ACCOUNTS").size() == 1);
    }

    @Test
    public void testSemanticExample9() throws MqttTreeException, MqttTreeLimitExceededException {
        MqttTree<String> tree = createTreeDefaultConfig();
        tree.subscribe("Accounts payable", "Client1");
        Assert.assertEquals("should be 1 distinct branches", 1, tree.getBranchCount());
        Assert.assertTrue("subscription should exist",
                tree.search("Accounts payable").size() == 1);

        Assert.assertTrue("subscription should NOT exist",
                tree.search("Accounts").size() == 0);

        Assert.assertTrue("subscription should NOT exist",
                tree.search("payable").size() == 0);

        Assert.assertTrue("subscription should NOT exist",
                tree.search("Accountspayable").size() == 0);
    }
}
