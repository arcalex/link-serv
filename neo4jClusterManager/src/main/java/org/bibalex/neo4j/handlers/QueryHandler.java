package org.bibalex.neo4j.handlers;

import org.bibalex.neo4j.helpers.Neo4jHelper;
import org.bibalex.neo4j.models.QueryResult;
import org.neo4j.driver.*;
import org.neo4j.driver.types.Path;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.neo4j.driver.Values.parameters;

public abstract class QueryHandler {

    HashSet<QueryResult> queryResultHashSet;
    Map<Integer, QueryResult> queryResultMap;
    String[] neo4jInstances;

    Map<Long, ArrayList<String>> nodeIdQueryKeys; //for nodes
    Map<Long, Integer> nodeIdResultId; //for nodes
    Map<Integer, Path> resultIdEdge; // for edges
    Map<Integer, String> resultIdQueryKey; //for edges
    AtomicInteger count; //results count

    String unwindKey = ""; //ex: row
    String unwindProperty = ""; //ex: url
    HashSet<String> unwindProperties; //ex: test1,test2,test3
    Map<String, String> unwindUsage; //ex: outlinks -> (outlinks:Node{url:row.url})

    Map<String, String> sourceTargetRelationship;
    Map<String, String> sourceTargetRelationshipName;
    HashSet<String> relationshipKeys;
    String originalRelationShipQuery = "";
    Map<String, String> keyAsValue;
    Neo4jHelper neo4jHelper;

    private ExecutorService executorService;

    public QueryHandler(ExecutorService executorService, Neo4jHelper neo4jHelper) {
        this.neo4jInstances = PropertiesHandler.getClusterProperty("neo4jInstances").split(",");
        queryResultHashSet = new HashSet<>();
        queryResultMap = new HashMap<>();

        nodeIdQueryKeys = new HashMap<>();
        nodeIdResultId = new HashMap<>();
        resultIdEdge = new HashMap<>();
        resultIdQueryKey = new HashMap<>();
        count = new AtomicInteger();
        unwindProperties = new HashSet();
        unwindUsage = new HashMap<>();
        sourceTargetRelationship = new HashMap<>();
        sourceTargetRelationshipName = new HashMap<>();
        relationshipKeys = new HashSet<>();
        keyAsValue = new HashMap<>();

        this.executorService = executorService;
        this.neo4jHelper = neo4jHelper;
    }

    public void addNodeIdQueryKey(Long nodeId, String queryKey) {
        ArrayList<String> queryKeys = nodeIdQueryKeys.get(nodeId);
        if (queryKeys == null)
            queryKeys = new ArrayList<>();
        queryKeys.add(queryKey);
        nodeIdQueryKeys.put(nodeId, queryKeys);
    }

    public void addNodeIdResultId(Long nodeId, Integer resultId) {
        nodeIdResultId.put(nodeId, resultId);
    }

    public void addResultIdEdge(Integer resultId, Path path) {
        resultIdEdge.put(resultId, path);
    }

    public void addResultIdQueryKey(Integer resultId, String queryKey) {
        resultIdQueryKey.put(resultId, queryKey);
    }

    public void removeFromUnwindProperties(String key) {
        unwindProperties.remove(key);
    }

    public void removeFromRelationshipKeys(String key) {
        relationshipKeys.remove(key);
    }

    public String getUnwindKey() {
        return unwindKey;
    }

    public Map<String, String> getUnwindUsage() {
        return unwindUsage;
    }

    public String getUnwindProperty() {
        return unwindProperty;
    }

    public HashSet<String> getRelationshipKeys() {
        return relationshipKeys;
    }

    public String getOriginalRelationShipQuery() {
        return originalRelationShipQuery;
    }

    abstract void getNeo4jResult(String query, int shard);

    abstract void handleResultVirtualNodes(String query, Value parametersValue, int shard);

    String getRelationShipName(String key, String value){
        return sourceTargetRelationshipName.get(key+","+value);
    }

    String addReturnStatementsToQuery(String query, HashSet<String> keys) {

        for (String key : keys) {
            query = query + ", labels(" + key + "), " + key + "." + PropertiesHandler.getConfigProperty("idProperty");
        }
        for (String key : unwindUsage.keySet()) {
            query = query + ", labels(" + key + "), " + key + "." + PropertiesHandler.getConfigProperty("idProperty");
        }
        for (String key : relationshipKeys) {
            query = query + ", " + key;
        }
        if (checkUnwind(query))
            query = query + "," + unwindKey;
        query += ";";

        return query;
    }

    void executeQueriesInShards(String query, Value parametersValue, boolean virtual) {
        ArrayList<Future> futures = new ArrayList<>();
        for (int i = 0; i < neo4jInstances.length; i++) {
            int iterator = i;
            Future<?> f = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName()+" Start. Instance = " + neo4jInstances[iterator]);
                    if(virtual)
                        handleResultVirtualNodes(query, parametersValue, iterator);
                    else
                        getNeo4jResult(query, iterator);
                }
            });
            futures.add(f);
        }
        boolean done = false;
        while (!done){
            done = true;
            for(Future f : futures){
                done = done && f.isDone();
            }
        }
    }

    void handleVirtualNodes(){
        ArrayList<Long> nodeIds = new ArrayList();
        for (Map.Entry<Long, Integer> entry : nodeIdResultId.entrySet()) {
            nodeIds.add(entry.getKey());
        }
        if (nodeIds.size() != 0) {
            String query = "match(n:"+ PropertiesHandler.getConfigProperty("clusterLabel") +
                    ") where n." + PropertiesHandler.getConfigProperty("idProperty") + " in $ids return n";
            Value parametersValue = parameters("ids", nodeIds);
            executeQueriesInShards(query, parametersValue, true);
        }
    }

    void handleUnwind(String query){
        getUnwindKey(query);
        addToUnwindProperties(query);
    }

    boolean checkUnwind(String query) {
        return query.matches("(?i).*?unwind.*?");
    }

    void convertHashMapToHashSet() {
        for (Map.Entry<Integer, QueryResult> entry : queryResultMap.entrySet()) {
            queryResultHashSet.add(entry.getValue());
        }
    }

    String handleRelationship(String relationshipQuery) {
        originalRelationShipQuery = relationshipQuery.contains("{") ? relationshipQuery : originalRelationShipQuery;
        relationshipQuery = relationshipQuery.replaceAll("\\:[a-zA-Z]+\\{.*?\\}\\)", ")");
        return relationshipQuery;
    }

    private void getUnwindKey(String query) {
        unwindKey = query.split("(?i)unwind\\s*.*?(\\(|\\[).*?(\\)|\\])\\s*as\\s*")[1].split("\\s")[0];
    }

    private void addToUnwindProperties(String query) {
        Pattern pattern = Pattern.compile("(?i)(?<=unwind\\s[a-z]*(\\(|\\[)\").*?(?=\"\\s*,.*?(\\)|\\])\\s*as\\s*)", Pattern.DOTALL);
        Matcher matcher = pattern.matcher(query);
        if (matcher.find()) {
            HashSet<String> temp = new HashSet<>(Arrays.asList(matcher.group().split("\\|")));
            temp.forEach(str -> {unwindProperties.add("\""+str+"\"");});
        }
    }

//    public static void main(String[] args) {
////        String query = "UNWIND [{url: \"test3\"}, {url: \"test2\"}] as row as test";
////        String query2 = "merge (test:test{})";
////
////        String [] slitq = query2.split("\\:|\\)");
////        String listString = StringUtils.substringBetween(query, "UNWIND", "as");
////        String unwind = "{url: \"test3\"}";
////        String [] splitUnwind = unwind.split("\\{|\\}");
////
////        String str = "ZZZZL <%= dsn %> AFFF <%= AFG %>";
//////        Pattern pattern = Pattern.compile("(?i)(?<=unwind\\s\\[).*?(?=\\]\\s*as)", Pattern.DOTALL);
//////        Matcher matcher = pattern.matcher(query);
//////        while (matcher.find()) {
//////            System.out.println(matcher.group());
//////        }
////        String [] querySplit = query.split("(?i)unwind.*?as");
////        String relationshipQuery = "Optional MATCH (parent)-[r1:HAS_VERSION]->(version)";
////        relationshipQuery = relationshipQuery.replaceAll("\\:[a-zA-Z]+\\{.*?\\}\\)",")");
////        boolean test = relationshipQuery.matches(".*?\\-\\[.*?\\]\\-\\>.*?");
//////        String url = "http://168.62.61.181:8080/1973-10-06T14:00:00Z,http://aardvark";
//////        String [] urlsplit = url.split("/", 4);
////
////        String returnString;
////        returnString = query.split("(?i)return")[1];
////
////        Driver driver = GraphDatabase.driver("bolt://localhost:7687");
////        Session session = driver.session();
////        Result result = session.run(query);
////        Record record = result.next();
////        System.out.println(test);
////        String queryKey = "labels(n)";
////        String [] queryKeyPart = queryKey.split(".");

//
//        String query = "UNWIND [{url: \"com,go,abcnews,//https:/meta/rss?text=byline:\\\"Dr. Sejal Parekh\\\"&limit=30&type=BlogEntry%20Story&sort=date\"}] AS row" +
//                " MERGE (parent:Node {url:\"net,alarabiya,www,//http:/robots.txt\"}) MERGE (parent)-[:HAS_VERSION]->(version:VersionNode {timestamp:\"2020-03-20T02:38:32Z\"})" +
//                " MERGE (outlinks:Node {url:row.url}) MERGE (version)-[:LINKED_TO]->(outlinks) RETURN parent.url;";
//        String query = "UNWIND [{url:\"com,go,abcnews,//https:/meta/rss?text=byline:\"Dr. Sejal Parekh\"&limit=30&type=BlogEntry%20Story&sort=date\"}] AS row MERGE (parent:Node {url:\"net,alarabiya,www,//http:/robots.txt\"}) MERGE (parent)-[:HAS_VERSION]->(version:VersionNode {timestamp:\"2020-03-20T03:43:24Z\"})  MERGE (outlinks:Node {url:row.url}) MERGE (version)-[:LINKED_TO]->(outlinks) RETURN parent.url;";
////
//        String query =
//                "Merge (parent:Node {url: $url }) " +
//                "merge (parent)-[r1:HAS_VERSION]->(version:VersionNode{timestamp: $version }) " +
//                "with parent, version " +
//                "UNWIND split($outlinks , '|') as outlink " +
//                "merge (outlinkNode:Node{url:outlink}) " +
//                "merge (version)-[r2:LINKED_TO]->(outlinkNode) " +
//                "RETURN parent.url;";
//        String query =
//                "MERGE (parent:Node {identifier: $url }) MERGE (parent)-[:HAS_VERSION]->(version:VersionNode {timestamp: $version }) WITH parent, version UNWIND split($outlinks , '|') AS outlink  MERGE (outlinkNode:Node {identifier:outlink}) MERGE (version)-[:LINKED_TO]->(outlinkNode)";
////        String query =
//                "UNWIND [{url:\"test7\"}, {url:\"test2\"}] as row " +
//                "Merge (parent:Node {url:\"test15\"}) " +
//                "merge (parent)-[r1:HAS_VERSION]->(version:VersionNode{timestamp:\"20200320023831\"}) " +
//                "merge (outlinks:Node{url:row.url}) " +
//                "merge (version)-[r2:LINKED_TO]->(outlinks) " +
//                "RETURN parent.identifier;";

//        String query =
//                "Merge (parent:Node {url:\" com,theguardian,amp,//https:/sport/2020/mar/20/no-crowd-no-atmosphere-only-footy-as-afl-season-makes-muted-bow\"}) " +
//                "merge (parent)-[r1:HAS_VERSION]->(version:VersionNode{timestamp:\"2020-03-20T02:38:32Z\"}) " +
//                "RETURN parent.url;";

//        String query = "MATCH (n:Node{url:\"test3\"})-[:HAS_VERSION]->(v:VersionNode)" +
//                " WHERE v.timestamp = \"1973-10-06T14:00:00Z\"" +
//                " RETURN n.url, v.timestamp, ID(n);";
//
//        String query = "MATCH (parent1:Node {url: \"com,linkonlineworld,Passport,//http:/Registration.aspx?WebSiteId=15&amp;ThemeName=Masrawy&amp;Culture=ar-EG&amp;ReturnURL=http%3a%2f%2fwww.masrawy.com%2fDefault.aspx&amp;ReturnRoles=False\"})-[:HAS_VERSION]->(version1:VersionNode {timestamp: \"2021-03-20T02:38:31Z\"})-[r:LINKED_TO]->(parent2) " +
//                "OPTIONAL MATCH (parent2)-[:HAS_VERSION]->(version2:VersionNode {timestamp:\"2021-03-20T02:38:31Z\"}) RETURN ID(parent1), ID(parent2), parent2.url, ID(version1), ID(r), ID(version2), version2.timestamp;";
//        String query = "MATCH (parent:Node{url:\"test3\"})-[:HAS_VERSION]->(v:VersionNode) RETURN DATETIME(v.timestamp).YEAR AS key, COUNT(v) AS count;";
//        String query = "MATCH (parent:Node{url:\"com,weblations,www,//http:/html/clients/vodo.htm\"})-[:HAS_VERSION]->(v:VersionNode)" +
//                " RETURN SUBSTRING(v.timestamp,0 ,4) AS key, COUNT(v) AS count";
//        PropertiesHandler.initializeProperties();
//        ExecutorService es = newFixedThreadPool(20);
//        Neo4jHelper neo4jHelper =  new Neo4jHelper();
//        Map<String, Object> params = new HashMap<>();
//        params.put("url","test5");
//        params.put("version","20200101010101");
//        params.put("outlinks","test6|test7|test8");
//
////
//        WriteHandler queryHandler = new WriteHandler(es,neo4jHelper);
//        queryHandler.executeWrite(query, params);
////
//        ReadHandler readHandler =  new ReadHandler(es, neo4jHelper);
//        readHandler.executeRead(query);
//
//        System.out.println("done");


//        String test = "$this is $test string $to test $splitters ";
//        Pattern pattern = Pattern.compile("\\$\\w+");
//
//        Matcher matcher = pattern.matcher(test);
//        while (matcher.find())
//        {
//            System.out.println(matcher.group().split("\\$")[1]);
//        }
//        String [] testArr = test.split("\\$.*?\\s+");
//        for(String str : testArr){
//            System.out.println(str);
//        }
//    }
}
