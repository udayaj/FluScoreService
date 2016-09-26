package fluscoreservice;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.configuration.*;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import java.net.URLDecoder;

import edu.cmu.lti.lexical_db.ILexicalDatabase;
import edu.cmu.lti.lexical_db.NictWordNet;
import edu.cmu.lti.ws4j.RelatednessCalculator;
import edu.cmu.lti.ws4j.impl.Resnik;
import edu.cmu.lti.ws4j.impl.WuPalmer;
import edu.cmu.lti.ws4j.util.WS4JConfiguration;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ScoreService implements Daemon {

    //database parameters
    private static String DB_HOST = "";
    private static String DB_PORT = "";
    private static String DB_USER = "";
    private static String DB_PASSWORD = "";
    private static String DB_NAME = "";
    private static int ROWS_READ = 50;

    //file paths
    private static String WORDLIST_PATH = "";
    private static String BLACKLIST_PATH = "";
    private static String EXCLUDE_SEMANTICS_PATH = "";

    //score options
    private static boolean BLACK_LIST_ENABLED = true;
    private static boolean TAGS_SEARCH_ENABLED = true;
    private static boolean COMBINED_SCORES_ENABLED = true;
    private static float THRESHOLD_SCORE = 0.93f;

    //threading variables
    private static int CLIENT_NUM = 0;
    private static int NUM_OF_CLIENTS = 1;
    //private static final int delay = 60000;//60 seconds    
    //private static boolean sleep = true;
    protected static int MAX = 200;
    private static boolean stop_execution = false;

    protected static int last_id = 0;
    private static int NUM_THREADS = 25;//25
    //private static int num_consumer_waitings = 0;
    //private static int counter = 0;

    //resources loaded
    private static Connection db = null;
    protected List<MsgData> list = new ArrayList<>();
    private static String arrBlackList[][];
    private static String arrTagList[][];
    private static String arrWordList[][];
    private static String arrExcludeSemanticsList[];

    //WordNet
    private static ILexicalDatabase lexicalDB = new NictWordNet();
    private static RelatednessCalculator[] rcs = {
        new WuPalmer(lexicalDB), new Resnik(lexicalDB)
    };

    class Producer extends Thread {

        public Producer(String name) {
            super(name);
        }

        public void run() {
            while (true) {
                List<MsgData> list2 = getMessages();

                while (list2.isEmpty()) {
                    try {
                        //System.out.println("Producer - going to sleep ...");
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        System.err.println("Producer INTERRUPTED - SLEEP");
                    }
                    list2 = getMessages();
                }

                if (stop_execution) {
                    System.out.println("Producer going to stop : Thread: " + this.getName());
                    break;
                }

                synchronized (list) {
                    //while (!list.isEmpty() || (NUM_THREADS != num_consumer_waitings)) {
                    while (!list.isEmpty()) {
                        try {
                            //System.out.println("Producer WAITING");
                            list.wait();   // Limit the size
                        } catch (InterruptedException ex) {
                            System.err.println("Producer INTERRUPTED - Wait");
                        }
                    }

                    try {
                        Thread.sleep(4000);
                    } catch (InterruptedException ex) {
                        System.err.println("Producer INTERRUPTED - SLEEP");
                    }

                    list2 = getMessages();
                    list.addAll(list2);
                    //num_consumer_waitings = 0;

                    list.notifyAll();  // must own the lock

                    if (stop_execution) {
                        System.out.println("Producer going to stop : Thread: " + this.getName());
                        break;
                    }
                }
            }
        }

        // Data coellection method
        List<MsgData> getMessages() {
            List<MsgData> list2 = new ArrayList<>();

            try {
                String tweetsQuery;
                //AND \"NoTags\" = false                

                if (NUM_OF_CLIENTS == 1) {
                    tweetsQuery = "SELECT \"Id\", \"MsgText\", \"KeyWords\", \"Longitude\", \"Latitude\", \"CreatedTime\", \"PlaceName\", \"PlacePolygon\", \"RetweetCount\", \"UserLocation\" FROM public.\"NormalizedMsg\" WHERE (\"IsProcessed\" = true AND \"IsScoreCalculated\" = false) ORDER BY \"Id\" ASC LIMIT " + ROWS_READ;
                } else {
                    tweetsQuery = "SELECT \"Id\", \"MsgText\", \"KeyWords\", \"Longitude\", \"Latitude\", \"CreatedTime\", \"PlaceName\", \"PlacePolygon\", \"RetweetCount\", \"UserLocation\" FROM public.\"NormalizedMsg\" WHERE (\"IsProcessed\" = true AND \"IsScoreCalculated\" = false) AND \"Id\"%" + NUM_OF_CLIENTS + "=" + CLIENT_NUM + " ORDER BY \"Id\" ASC LIMIT " + ROWS_READ;
                }

                Statement st = db.createStatement();
                ResultSet rs = st.executeQuery(tweetsQuery);

                while (rs.next()) {
                    int Id = rs.getInt(1);
                    String Msg = rs.getString(2).trim();
                    String KeyWords = URLDecoder.decode(rs.getString(3).trim().replace("''", ""), "utf-8");
                    float Longitude = rs.getFloat(4);
                    float Latitude = rs.getFloat(5);
                    Date CreatedTime = new Date(rs.getTimestamp(6).getTime());
                    String PlaceName = rs.getString(7);
                    String PlacePolygon = rs.getString(8);
                    int RetweetCount = rs.getInt(9);
                    String UserLocation = rs.getString(10);
                    float TagScore = 0.0f;
                    float NormalizedScore = 0.0f;
                    float CombinedScore = 0.0f;
                    boolean IsQualified = false;
                    boolean IsBlackListed = false;

                    MsgData d = new MsgData(Id, Msg, KeyWords, Longitude, Latitude, CreatedTime, PlaceName, PlacePolygon, RetweetCount, UserLocation, TagScore, NormalizedScore, CombinedScore, IsQualified, IsBlackListed);
                    list2.add(d);
                }

            } catch (java.sql.SQLException ex) {
                System.err.println("select query failed failed....");
                System.err.println(ex);
                System.exit(0);
            } catch (UnsupportedEncodingException ex) {
                System.err.println("Unsupported Encoding Exception in decoding tags");
                System.err.println(ex);
                System.exit(0);
            }

            return list2;
        }
    }

    class Consumer extends Thread {
        StopWords stopWords = new StopWords();
        
        public Consumer(String name) {
            super(name);
        }

        public void run() {
            while (true) {
                MsgData d = null;
                synchronized (list) {
//                    if (list.isEmpty()) {
//                        num_consumer_waitings = num_consumer_waitings + 1;
//                        if (num_consumer_waitings == NUM_THREADS) {
//                            list.notifyAll();
//                        }
//                    }

                    while (list.isEmpty()) {
                        try {
                            //System.out.println("CONSUMER WAITING " + this.getName());
                            list.wait();  // must own the lock
                        } catch (InterruptedException ex) {
                            System.err.println("CONSUMER INTERRUPTED");
                        }
                    }
                    d = list.remove(0);
                    //System.out.println("Get:"+d.MsgId);
                    list.notifyAll();

                    if (stop_execution) {
                        System.out.println("Consumer going to stop : Thread: " + this.getName());
                        break;
                    }
                }

                //start the checking process
                boolean blackListed = false;

                if (BLACK_LIST_ENABLED) {
                    blackListed = isBlackListed(d);
                }

                if (!blackListed) {
                    float normalScore = calculateNormalizedScore(d);
                    d.NormalizedScore = normalScore;
                    d.IsBlackListed = false;
                    d.IsQualified = false;
                    float tagScore = 0.0f;
                    if (TAGS_SEARCH_ENABLED && !d.KeyWords.isEmpty()) {
                        tagScore = calculateTagScore(d);

                        d.TagScore = tagScore;
                        d.CombinedScore = normalScore + tagScore;

                    } else {
                        d.TagScore = 0.0f;
                        d.CombinedScore = 0.0f;
                    }

                    //qialification criteria   IsQualified = true/false
                    //if ((normalScore >= THRESHOLD_SCORE) || (d.CombinedScore >= (1.5 * THRESHOLD_SCORE))) {
                    if (normalScore >= THRESHOLD_SCORE) {                        
                        calculateExcludeSemanticScores(d);
                        if(d.RES < d.RES_Flu){
                            d.IsQualified = true;
                        }else{
                            d.IsQualified = false;
                        }
                    }

                } else {
                    //blacklisted criteria   IsBlackListed=true, TagScore=0, NormalScore =0, Combined Score=0, IsQualified = false
                    d.IsBlackListed = true;
                    d.NormalizedScore = 0.0f;
                    d.TagScore = 0.0f;
                    d.CombinedScore = 0.0f;
                    d.IsQualified = false;
                }

                //System.out.println("Thread:" + this.getName());
                //insert the record
                //if (!isRecordExist(d.MsgId)) {
                insertScoredMsgRecord(d);
                updateNormalizedMsgRecord(d.MsgId);
                //}

                //counter++;
                //System.out.println("Thred: " + this.getName() + "  Processed: " + counter);
                if (stop_execution) {
                    System.out.println("Consumer going to stop : Thread: " + this.getName());
                    break;
                }
            }
        }

        boolean isBlackListed(MsgData d) {
            int len = arrBlackList.length;
            for (int i = 0; i < len; i++) {
                Pattern p = Pattern.compile("\\b" + arrBlackList[i][0] + "(d|s|es|ies|ied|ed|ate|ated|ion|ize|ized|ive|ity|ious|ization|ing){0,1}\\b", Pattern.CASE_INSENSITIVE);
                Matcher m = p.matcher(d.Msg);
                if (m.find()) {
                    return true;
                }
            }
            return false;
        }

        float calculateNormalizedScore(MsgData d) {
            float sum = 0.0f;
            int len = arrWordList.length;
            for (int i = 0; i < len; i++) {
                Pattern p = Pattern.compile("\\b" + arrWordList[i][0] + "(d|s|es|ies|ied|ed|ate|ated|ion|ize|ized|ive|ity|ious|ization|ing){0,1}\\b", Pattern.CASE_INSENSITIVE);
                Matcher m = p.matcher(d.Msg);
                if (m.find()) {
                    sum = sum + Float.parseFloat(arrWordList[i][1]);
                }
            }
            return sum;
        }

        float calculateTagScore(MsgData d) {
            String tags = d.KeyWords.replaceAll("''", "");
            float sum = 0.0f;
            int len = arrTagList.length;
            for (int i = 0; i < len; i++) {
                Pattern p = Pattern.compile("\\b" + arrTagList[i][0] + "\\b", Pattern.CASE_INSENSITIVE);
                Matcher m = p.matcher(tags);
                if (m.find()) {
                    sum = sum + Float.parseFloat(arrTagList[i][1]);
                }
            }
            return sum;
        }

        MsgData calculateExcludeSemanticScores(MsgData d) {
            WS4JConfiguration.getInstance().setMFS(true);

            String baseKeyWord = arrExcludeSemanticsList[0];//Influenza

            ArrayList<String> tokens = getTokens(d.Msg);

            if (tokens.size() > 0) {
                //WUP
                float sum_wup_flu = 0.0f;
                for (String token : tokens) {
                    float temp_wup_flu = (float) rcs[0].calcRelatednessOfWords(token, baseKeyWord);
                    if (temp_wup_flu > 0) {
                        sum_wup_flu = sum_wup_flu + temp_wup_flu;
                    }
                }
                d.WUP_Flu = sum_wup_flu;

                for (int i = 1; i < arrExcludeSemanticsList.length; i++) {
                    float sum_wup = 0.0f;

                    for (String token : tokens) {
                        float temp_wup = (float) rcs[0].calcRelatednessOfWords(token, arrExcludeSemanticsList[i]);
                        if (temp_wup > 0) {
                            sum_wup = sum_wup + temp_wup;
                        }
                    }

                    d.WUP = (d.WUP < sum_wup) ? sum_wup : d.WUP;
                }

                //RES
                float sum_res_flu = 0.0f;
                for (String token : tokens) {
                    float temp_res_flu = (float) rcs[1].calcRelatednessOfWords(token, baseKeyWord);
                    if (temp_res_flu > 0) {
                        sum_res_flu = sum_res_flu + temp_res_flu;
                    }
                }
                d.RES_Flu = sum_res_flu;

                for (int i = 1; i < arrExcludeSemanticsList.length; i++) {
                    float sum_res = 0.0f;

                    for (String token : tokens) {
                        float temp_res = (float) rcs[1].calcRelatednessOfWords(token, arrExcludeSemanticsList[i]);
                        if (temp_res > 0) {
                            sum_res = sum_res + temp_res;
                        }
                    }

                    d.RES = (d.RES < sum_res) ? sum_res : d.RES;
                }
            }

            return d;
        }

        ArrayList<String> getTokens(String msg) {
            ArrayList<String> list = new ArrayList<>();

            Pattern p = Pattern.compile("(\\@|\\||\\(|\\)|\\-|/|\\\\|\\+|%|\\.|=|\\*)+", Pattern.CASE_INSENSITIVE);
            Matcher m = p.matcher(msg);
            msg = m.replaceAll(" ");
            String arr[] = msg.split(" ");

            //remove stop words
            if (arr.length > 0) {
                for (String word : arr) {
                    if (!stopWords.isStopword(word)) {
                        list.add(word);
                    }
                }
            }

            return list;
        }

        boolean isRecordExist(int Id) {
            boolean exist = false;

            try {

                String tweetsQuery = "SELECT count(*) AS Total FROM public.\"ScoredMsg\" WHERE (\"NormalizedId\" =" + Id + ")";

                Statement st = db.createStatement();
                ResultSet rs = st.executeQuery(tweetsQuery);

                while (rs.next()) {
                    int count = rs.getInt("Total");
                    if (count > 0) {
                        exist = true;
                    }
                    break;
                }
            } catch (java.sql.SQLException ex) {
                System.err.println("duplicate count query failed failed....");
                System.err.println(ex);
                System.exit(0);
            }

            return exist;
        }

        void insertScoredMsgRecord(MsgData data) {
            try {
                String insertQuery = "INSERT INTO public.\"ScoredMsg\"(\"NormalizedId\", \"Longitude\", \"Latitude\", \"CreatedTime\", \"PlaceName\", \"PlacePolygon\", \"UserLocation\", \"NormalizedScore\", "
                        + "\"TagScore\", \"CombinedScore\", \"IsQualified\", \"IsBlackListed\", \"WUP\", \"RES\", \"WUP_Flu\", \"RES_Flu\") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?);";
                PreparedStatement preparedStatement = db.prepareStatement(insertQuery);
                preparedStatement.setInt(1, data.MsgId);
                preparedStatement.setFloat(2, data.Longitude);
                preparedStatement.setFloat(3, data.Latitude);
                preparedStatement.setTimestamp(4, new java.sql.Timestamp(data.CreatedTime.getTime()));
                preparedStatement.setString(5, data.PlaceName);
                preparedStatement.setString(6, data.PlacePolygon);
                preparedStatement.setString(7, data.UserLocation);
                preparedStatement.setFloat(8, data.NormalizedScore);
                preparedStatement.setFloat(9, data.TagScore);
                preparedStatement.setFloat(10, data.CombinedScore);
                preparedStatement.setBoolean(11, data.IsQualified);
                preparedStatement.setBoolean(12, data.IsBlackListed);

                preparedStatement.setFloat(13, data.WUP);
                preparedStatement.setFloat(14, data.RES);
                preparedStatement.setFloat(15, data.WUP_Flu);
                preparedStatement.setFloat(16, data.RES_Flu);

                preparedStatement.executeUpdate();
            } catch (SQLException ex) {
                System.err.println("Error in SQL Query execution, ScoredMsg record insertion failed.");
                System.err.println(ex);
                System.exit(0);
            }
        }

        void updateNormalizedMsgRecord(int MsgId) {
            try {
                String updateQuery = "Update public.\"NormalizedMsg\" SET \"IsScoreCalculated\" = ? WHERE \"Id\" = ?;";
                PreparedStatement preparedStatement = db.prepareStatement(updateQuery);
                preparedStatement.setBoolean(1, true);//IsScoreCalculated
                preparedStatement.setLong(2, MsgId);//Id            
                preparedStatement.executeUpdate();

            } catch (SQLException ex) {
                System.err.println("Error in SQL Query execution, NormalizedMsg setting IsScoreCalculated = true failed.");
                System.err.println(ex);
                System.exit(0);
            }
        }

    }

    public static void loadParams() throws InterruptedException {
        System.out.println("Score Service started at " + new Date().toString());

        try {
            DefaultConfigurationBuilder builder = new DefaultConfigurationBuilder();
            File f = new File("config/config.xml");
            builder.setFile(f);
            CombinedConfiguration config = builder.getConfiguration(true);

            DB_HOST = config.getString("database.host");
            DB_PORT = config.getString("database.port");
            DB_USER = config.getString("database.user");
            DB_PASSWORD = config.getString("database.password");
            DB_NAME = config.getString("database.name");
            ROWS_READ = config.getInt("score-service.rows-read", ROWS_READ);
            MAX = config.getInt("score-service.list-max", MAX);

            WORDLIST_PATH = config.getString("score-service.wordlist-path", WORDLIST_PATH);
            BLACKLIST_PATH = config.getString("score-service.blacklist-path", BLACKLIST_PATH);
            EXCLUDE_SEMANTICS_PATH = config.getString("score-service.exclude-semantics-path", EXCLUDE_SEMANTICS_PATH);

            BLACK_LIST_ENABLED = config.getBoolean("score-service.blacklist-enabled", BLACK_LIST_ENABLED);
            TAGS_SEARCH_ENABLED = config.getBoolean("score-service.tags-search-enabled", TAGS_SEARCH_ENABLED);
            COMBINED_SCORES_ENABLED = config.getBoolean("score-service.combine-scores-enabled", COMBINED_SCORES_ENABLED);
            THRESHOLD_SCORE = config.getFloat("score-service.threshold-score", THRESHOLD_SCORE);

            CLIENT_NUM = config.getInt("score-service.client-num", CLIENT_NUM);
            NUM_OF_CLIENTS = config.getInt("score-service.num-of-clients", NUM_OF_CLIENTS);

            System.out.println("DB_HOST: " + DB_HOST);
            System.out.println("DB_PORT: " + DB_PORT);
            System.out.println("DB_USER: " + DB_USER);
            System.out.println("DB_PASSWORD: " + DB_PASSWORD);
            System.out.println("DB_NAME: " + DB_NAME);

            //XMLConfiguration config = new XMLConfiguration("config\\config.xml");
            //System.out.println("consumer key: " + config.getString("database.url"));     
            Class.forName("org.postgresql.Driver");
            String dbUrl = "jdbc:postgresql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME;

            db = DriverManager.getConnection(dbUrl, DB_USER, DB_PASSWORD);
        } catch (org.apache.commons.configuration.ConfigurationException cex) {
            System.err.println("Error occurred while reading configurations....");
            System.err.println(cex);
            System.exit(0);
        } catch (ClassNotFoundException ex) {
            System.err.println("Database Driver not found....");
            System.err.println(ex);
            System.exit(0);
        } catch (SQLException ex) {
            System.err.println("Database Connection failed....");
            System.err.println(ex);
            System.exit(0);
        }

        //load word lists
        loadBlackList();
        loadWordListAndTagList();
        loadExcludeSemanticsList();

    }

    public static void loadWordListAndTagList() {
        File wordlist_file;
        ArrayList<String[]> wordlist = new ArrayList<String[]>();
        ArrayList<String[]> taglist = new ArrayList<String[]>();
        try {
            wordlist_file = new File(WORDLIST_PATH);
            BufferedReader br = new BufferedReader(new FileReader(wordlist_file));

            String line = null;
            while ((line = br.readLine()) != null) {

                String arrTemp[] = line.split("\t");//0 word, 1 tag, 2 score
                if (arrTemp.length != 3) {
                    System.out.println(line);
                }

                wordlist.add(new String[]{arrTemp[0], arrTemp[2]});

                if (!arrTemp[1].equalsIgnoreCase("NA")) {
                    taglist.add(new String[]{arrTemp[1], arrTemp[2]});
                }
            }
            br.close();

            if (wordlist.size() > 0) {
                arrWordList = new String[wordlist.size()][2];
                for (int i = 0; i < wordlist.size(); i++) {
                    arrWordList[i] = wordlist.get(i);
                }
            }

            if (taglist.size() > 0) {
                arrTagList = new String[taglist.size()][2];
                for (int i = 0; i < taglist.size(); i++) {
                    arrTagList[i] = taglist.get(i);
                }
            }

            System.out.println("arrWordList: " + arrWordList.length);
            System.out.println("arrTagList: " + arrTagList.length);
        } catch (IOException ex) {
            System.err.println("WordList file reading error...");
            System.err.println(ex);
            System.exit(0);
        }
    }

    public static void loadBlackList() {
        File blacklist_file;
        ArrayList<String[]> blacklist = new ArrayList<String[]>();

        try {
            blacklist_file = new File(BLACKLIST_PATH);
            BufferedReader br = new BufferedReader(new FileReader(blacklist_file));

            String line = null;
            while ((line = br.readLine()) != null) {
                String arrTemp[] = line.split("\t");//0 word, 1 score
                blacklist.add(arrTemp);
            }
            br.close();

            if (blacklist.size() > 0) {
                arrBlackList = new String[blacklist.size()][2];
                for (int i = 0; i < blacklist.size(); i++) {
                    arrBlackList[i] = blacklist.get(i);
                }
            }
            System.out.println("arrBlackList: " + arrBlackList.length);
        } catch (IOException ex) {
            System.err.println("BlackList file reading error....");
            System.err.println(ex);
            System.exit(0);
        }

    }

    public static void loadExcludeSemanticsList() {
        File excludeSemantics_file;
        ArrayList<String> excludeSemanticsList = new ArrayList<String>();

        try {
            excludeSemantics_file = new File(EXCLUDE_SEMANTICS_PATH);
            BufferedReader br = new BufferedReader(new FileReader(excludeSemantics_file));

            String line = null;
            while ((line = br.readLine()) != null) {
                String temp = line.trim();
                excludeSemanticsList.add(temp);
            }
            br.close();

            if (excludeSemanticsList.size() > 0) {
                arrExcludeSemanticsList = new String[excludeSemanticsList.size()];
                for (int i = 0; i < excludeSemanticsList.size(); i++) {
                    arrExcludeSemanticsList[i] = excludeSemanticsList.get(i);
                }
            }
            System.out.println("arrExcludeSemanticsList: " + arrExcludeSemanticsList.length);
        } catch (IOException ex) {
            System.err.println("ExcludeSemantics file reading error....");
            System.err.println(ex);
            System.exit(0);
        }
    }

    public static void main(String[] args) throws Exception {
        ScoreService sc = new ScoreService();
        sc.start();
    }

    @Override
    public void init(DaemonContext dc) throws DaemonInitException, Exception {
        System.out.println("initializing ...");
    }

    @Override
    public void start() throws Exception {
        System.out.println("starting ...");
        loadParams();

        // Start producers and consumers
        int numProducers = 1;
        int numConsumers = NUM_THREADS;

        for (int i = 0; i < numProducers; i++) {
            new Producer("Pro " + Integer.toString(i)).start();
        }

        for (int i = 0; i < numConsumers; i++) {
            new Consumer(Integer.toString(i)).start();
        }

        System.out.println("Threads started !");
    }

    @Override
    public void stop() throws Exception {
        System.out.println("Stopping ...");

        stop_execution = true;
        synchronized (list) {
            //stop_execution = true;            
            list.notifyAll();
            System.out.println("Client No. " + CLIENT_NUM + " was stopped.");
        }
    }

    @Override
    public void destroy() {
        System.out.println("Stopped !");
    }
}
