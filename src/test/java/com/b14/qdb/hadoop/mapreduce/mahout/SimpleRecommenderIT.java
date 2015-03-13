package com.b14.qdb.hadoop.mapreduce.mahout;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.common.RandomUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.b14.qdb.Qdb;
import com.b14.qdb.Quasardb;
import com.b14.qdb.QuasardbConfig;
import com.b14.qdb.QuasardbException;
import com.b14.qdb.QuasardbNode;
import com.b14.qdb.hadoop.mahout.QuasardbDataModel;
import com.b14.qdb.hadoop.mahout.QuasardbPreference;

/**
 * A unit test case for {@link QuasardbDataModel} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version master
 * @since 1.3.0
 */
public class SimpleRecommenderIT {
    private static final int PORT = 2836;
    private static final String HOST = "127.0.0.1";
    private static final QuasardbConfig config = new QuasardbConfig();
    
    private Quasardb qdbInstance = null;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Qdb.DAEMON.start();
        QuasardbNode quasardbNode = new QuasardbNode(HOST, PORT);
        config.addNode(quasardbNode);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Qdb.DAEMON.stop();
    }

    @Before
    public void setUp() throws Exception {
        qdbInstance = new Quasardb(config);
        try {
            qdbInstance.connect();
        } catch (QuasardbException e) {
            e.printStackTrace();
            fail("qdbInstance not initialized.");
        }
        
        String line;
        try {
            InputStream fis = getClass().getClassLoader().getResourceAsStream("intro.csv");
            InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
            BufferedReader br = new BufferedReader(isr);
            int i = 0;
            while ((line = br.readLine()) != null) {
                StringTokenizer st = new StringTokenizer(line, ",");
                QuasardbPreference pref = new QuasardbPreference(Long.parseLong(st.nextToken()), Long.parseLong(st.nextToken()), Float.parseFloat(st.nextToken()), (new Date()).getTime());
                qdbInstance.put("recommender.user_" + i, pref);
                i++;
            }
        } catch (Exception e) {
            fail("Shouldn't raise an Exception while loading data into quasardb");
        }
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (qdbInstance != null) {
                qdbInstance.purgeAll();
                qdbInstance.close();
            }
        } catch (QuasardbException e) {
        }
    }
   
    /**
     * Test for {@link QuasardbDataModel}.<br>
     * <br>
     * Mahout doesn't represent one recommender. It contains numerous of plug-in components. 
     * Developer can variate this components to build the best recommender for specified domain area. 
     */
    @Test
    public void testSimpleRecommender() {
        try {
            // Implementation stores, provides access to all the preference, user, and item data needed in the computation
            DataModel model = new QuasardbDataModel(HOST, PORT);
            
            // Notion of how similar two users are
            UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
            
            // Notion of a group of users that are most similar to a given user
            // This neighborhood contains of 2 users
            // It's also possible to define theshold instead of concrete number of users (if we do not know how many users might be neighbors)
            UserNeighborhood neighborhood = new NearestNUserNeighborhood(2, similarity, model);
            
            Recommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
            
            int userId = 1;
            int recommendationsCount = 2;
            List<RecommendedItem> recommendations = recommender.recommend(userId, recommendationsCount);
            
            assertTrue("There should be recommendations.", !recommendations.isEmpty());
            assertTrue("There should be 2 recommendations.", recommendations.size() == 2);

            for (RecommendedItem recommendation : recommendations) {
                System.out.println(recommendation);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("Shouldn't raise an Exception => " + e.getMessage());
        }
    }
    
    @Test
    public void testScoreSimpleRecommender() {
        try {
            // Forces the same random choices each time.
            // Training and test datasets may differ at each run
            RandomUtils.useTestSeed();
            DataModel model = new QuasardbDataModel(HOST, PORT);
            RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
            RecommenderBuilder builder = new RecommenderBuilder() {
                public Recommender buildRecommender(DataModel model) throws TasteException {
                    UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
                    UserNeighborhood neighborhood = new NearestNUserNeighborhood(2, similarity, model);
                    return new GenericUserBasedRecommender(model, neighborhood, similarity);
                }
            };

            // The RecommenderEvaluator splits the data into a training and test set, builds a new training DataModel and Recommender to test, 
            // and compares its estimated preferences to the actual test data.
            // 
            // Train recommender with 70% of data; test with 30%.
            // 1.0 = using 100% of data. It is good for quick testing recommender on data subsets
            double score = evaluator.evaluate(builder, null, model, 0.7, 1.0);

            // Score indicating how well the Recommender performed
            System.out.println(score);

            // A result of 1.0 from this implementation means that, on average, the recommender estimates a preference that deviates from the actual preference by 1.0.
            RecommenderIRStatsEvaluator evaluatorIRStats = new GenericRecommenderIRStatsEvaluator ();
            IRStatistics stats = evaluatorIRStats.evaluate(builder, null, model, null, 2, GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1.0);
            // Percent of good recommendations
            assertTrue("Precision should be 0.75", stats.getPrecision() == 0.75);
            System.out.println(stats.getPrecision());
            
            // Percent of good recommendations appear in results
            assertTrue("Percentage of good recommendations should be 1.0", stats.getRecall() == 1.0);
            System.out.println(stats.getRecall());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Shouldn't raise an Exception => " + e.getMessage());
        }
    }
}
