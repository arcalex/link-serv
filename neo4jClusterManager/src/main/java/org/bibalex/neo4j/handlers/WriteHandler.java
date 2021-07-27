package org.bibalex.neo4j.handlers;

import org.bibalex.neo4j.helpers.Neo4jHelper;
import org.bibalex.neo4j.models.QueryResult;
import org.bibalex.neo4j.models.QueryResultWrite;
import org.neo4j.driver.*;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.util.Pair;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class WriteHandler extends QueryHandler {

    private Map<String, String> mergeKeys;
    private Map<String, Long> createdKeyWithID;
    private Map<String, Map<Long, Integer>> foundKeyWithIDAndShard;
    private String withString;
    private String idProperty;
    private String clusterLabel;
    private String virtualLabel;

    public WriteHandler(ExecutorService executorService, Neo4jHelper neo4jHelper) {
        super(executorService, neo4jHelper);
        mergeKeys = new HashMap<>();
        createdKeyWithID = new HashMap<>();
        foundKeyWithIDAndShard = new HashMap<>();
        withString = "";
        idProperty = PropertiesHandler.getConfigProperty("idProperty");
        clusterLabel = PropertiesHandler.getConfigProperty("clusterLabel");
        virtualLabel = PropertiesHandler.getConfigProperty("virtualLabel");
    }

    public HashSet<QueryResult> executeWrite(String query, Map<String, Object> parameters) {

        Pattern pattern = Pattern.compile("\\$\\w+");
        Matcher matcher = pattern.matcher(query);
        while (matcher.find())
        {
            String param = matcher.group().split("\\$")[1];
            if(parameters.get(param).getClass().equals(String.class))
                query = query.replaceAll("\\$"+param, "\"" + parameters.get(param).toString() + "\"");
            else
                query = query.replaceAll("\\$"+param, parameters.get(param).toString());
        }

        if (checkUnwind(query)) {
            handleUnwind(query);
        }
        query = getMergeKeys(query);
        query = convertMergeWithMatch(query);
        query = query.replace(";", "");
        if (!query.toLowerCase().contains("return"))
            query += " return ";
        query = addReturnStatementsToQuery(query, new HashSet<>(mergeKeys.keySet()));
        executeQueriesInShards(query, null, false);
        handleVirtualNodes();
        createNodesAndRelationships();
        return queryResultHashSet;
    }

    public void addToFoundKeyWithShardAndID(String key, long id, int index) {
        if (mergeKeys.get(key) != null) {
            mergeKeys.remove(key);
        }
        Map<Long, Integer> idAndShard = foundKeyWithIDAndShard.get(key);
        if (idAndShard == null)
            idAndShard = new HashMap<>();
        idAndShard.put(id, index);
        foundKeyWithIDAndShard.put(key, idAndShard);
    }

    @Override
    void getNeo4jResult(String query, int shard) {
        Result result = neo4jHelper.runQuery(query, null, shard);
        while (result.hasNext()) {
            count.incrementAndGet();
            Record record = result.next();
            List<Pair<String, Value>> properties = record.fields();
            queryResultMap.put(count.intValue(), new QueryResultWrite(properties, this, record, shard));
        }
    }

    @Override
    void handleResultVirtualNodes(String query, Value parametersValue, int shard) {
        Result result = neo4jHelper.runQuery(query, parametersValue, shard);
        while (result.hasNext()) {
            Node node = result.next().get("n").asNode();
            removeFromRelationshipKeys(nodeIdQueryKeys.get(node.get(idProperty)).get(0));
        }
    }

    private String getMergeKeys(String query) {

        String[] queryByMerge = query.split("(?i)merge\\s+");
        for (String queryPart : queryByMerge) {
            if (!queryPart.isEmpty()) {
                queryPart = queryPart.replaceFirst("\\(", "");
                //get outlinks with its query usage
                if (!unwindKey.equals("") && queryPart.contains(unwindKey) && !queryPart.matches(".*?" + unwindKey + "\\s*")) {
                    unwindUsage.put(queryPart.split("\\:|\\)")[0], "(" + queryPart);
//                    unwindProperty = queryPart.split(unwindKey + ".")[1].split("}")[0];
                }
                //handle relationship
                else if (queryPart.matches(".*?\\-\\[.*?\\]\\-\\>.*?")) {
                    String replacedQuery = handleRelationship(queryPart);
                    String source = queryPart.split("\\)")[0];
                    String target = queryPart.split(".*?\\-\\[.*?\\]\\-\\>\\(")[1].split("\\:|\\)")[0];
                    String relationshipName = queryPart.split("\\[|\\:|\\]")[2];

                    sourceTargetRelationship.put(source, target);
                    sourceTargetRelationshipName.put(source + "," + target, relationshipName);
                    relationshipKeys.add(source);
                    relationshipKeys.add(target);

                    if (mergeKeys.get(target) == null && queryPart.contains("{"))
                        mergeKeys.put(target, "(" + queryPart.split("\\(", 2)[1].split("RETURN")[0].split("\\)")[0] + ")");
                    query = query.replace(queryPart, replacedQuery);
                }
                //make sure that isn't unwind part
                else if (!checkUnwind(queryPart)) {
                    mergeKeys.put(queryPart.split("\\:|\\)")[0], "(" + queryPart);
                }
            }
        }
        return query;
    }

    private String convertMergeWithMatch(String query) {
        return query.replaceAll("(?i)merge\\s+", "optional match ");
    }

    private void createNodesAndRelationships() {
        String createQuery = "";
        FileHandler fileHandler = new FileHandler();
        long lastNodeId = fileHandler.getLastNodeId();

        int index = getIndexOfWritingShard();
        for (Map.Entry<String, String> entry : mergeKeys.entrySet()) {
            lastNodeId++;
            createQuery += " create " + entry.getValue() + " set " + entry.getKey() +
                    "."+ idProperty + "= " +
                    lastNodeId + "," + entry.getKey() + ":" + clusterLabel;
            createdKeyWithID.put(entry.getKey(), lastNodeId);
        }
        AtomicLong finalUnwindElementCount = new AtomicLong(lastNodeId + 1);
        setWithString();

        if(unwindProperties.size()!=0) {
            createQuery += withString + " UNWIND [";
            for (String str : unwindProperties) {
                createQuery += "{" + idProperty + ": " + finalUnwindElementCount + ",  url: " + str + "}, ";
                finalUnwindElementCount.getAndIncrement();
            }
            createQuery = createQuery.substring(0, createQuery.length() - 2) + "] AS " + unwindKey;
            lastNodeId = finalUnwindElementCount.decrementAndGet();

            Map.Entry<String, String> entry = unwindUsage.entrySet().iterator().next();

            createQuery += " merge " + entry.getValue().replace(unwindKey, unwindKey+".url") + " set " + entry.getKey() + "." + idProperty +
                    " = " + unwindKey + "." + idProperty + "," + entry.getKey() + ":" + clusterLabel;
            createdKeyWithID.put(entry.getKey(), (long) 0);
        }

        String relationshipStr = createRelationship(index);
        createQuery += relationshipStr;

        if (!createQuery.isEmpty())
            neo4jHelper.runQuery(createQuery, null, index);
        fileHandler.setLastNodeId(lastNodeId);
    }

    private int getIndexOfWritingShard() {
        int indexOfEmptyShard = Integer.valueOf(PropertiesHandler.getClusterProperty("indexOfEmptyShard"));
        int maximumNumberOfNodesInShard = Integer.valueOf(PropertiesHandler.getClusterProperty("maximumNumberOfNodesInShard"));
        int nodesCountThreshold = Integer.valueOf(PropertiesHandler.getClusterProperty("nodesCountThreshold"));

        for (int i = indexOfEmptyShard; i < neo4jInstances.length; i++) {
            Result result = neo4jHelper.runQuery(PropertiesHandler.getConfigProperty("getNodesCountQuery"), null, i);

            int count = result.next().get("COUNT(n)").asInt();

            if ((i == neo4jInstances.length - 1) || (count < maximumNumberOfNodesInShard &&
                    !(count >= maximumNumberOfNodesInShard - nodesCountThreshold))) {
                return i;
            }
        }
        return -1;
    }

    private String createRelationship(int index) {
        String createQuery = "";
        setWithString();

        for (Map.Entry<String, String> entry : sourceTargetRelationship.entrySet()) {

            Set<String> keySet = foundKeyWithIDAndShard.keySet().stream().filter(s -> s.startsWith(entry.getKey())).collect(Collectors.toSet());
            Set<String> valueSet = foundKeyWithIDAndShard.keySet().stream().filter(s -> s.startsWith(entry.getValue())).collect(Collectors.toSet());
            Set<String> createTargetSet = createdKeyWithID.keySet().stream().filter(s -> s.startsWith(entry.getValue())).collect(Collectors.toSet());

            if (keySet.isEmpty() && valueSet.isEmpty()) {
                createQuery += createRelationshipWithTarget(entry, createTargetSet);
            } else if (!keySet.isEmpty() && valueSet.isEmpty()) {
                createQuery += createRelationshipValueCreated(entry, index, createTargetSet);
            } else if (keySet.isEmpty() && !valueSet.isEmpty()) {
                createQuery += createRelationshipKeyCreated(entry, index, createTargetSet);
            } else {
                createQuery += mergeRelationship(entry, index, createTargetSet);
            }
        }
        return createQuery;
    }

    private String createRelationshipValueCreated(Map.Entry<String, String> entry, int index, Set<String> createTargetSet) {
        Map<Long, Integer> idShard = foundKeyWithIDAndShard.get(entry.getKey());
        long id = idShard.keySet().iterator().next();
        int shard = idShard.values().iterator().next();

        String matchQuery = "match (" + entry.getKey() + ":" + clusterLabel + 
                "{" + idProperty + ": " + id + "})";
        if (shard == index) {
            return withString + " " + matchQuery + createRelationshipWithTarget(entry, createTargetSet);
        } else {
            for (String key : createTargetSet) {
                matchQuery += createRelationshipQuery(entry) + key + ":" + virtualLabel + ":" +
                        clusterLabel + "{" + idProperty +
                        ":\"V" + createdKeyWithID.get(key) + "\"})";
            }
            neo4jHelper.runQuery(matchQuery, null, shard);
            return "";
        }
    }

    private String createRelationshipKeyCreated(Map.Entry<String, String> entry, int index, Set<String> createTargetSet) {
        String createQuery = "";
        Map<Long, Integer> idShard = foundKeyWithIDAndShard.get(entry.getValue());
        for (Map.Entry<Long, Integer> idShardEntry : idShard.entrySet()) {
            long id = idShardEntry.getKey();
            int shard = idShardEntry.getValue();

            if (shard == index) {
                createQuery += withString + " match (" + entry.getValue() + id + ":" + clusterLabel +
                        "{" + idProperty+ ": " + id + "}) ";
                createQuery += createRelationshipQuery(entry) + entry.getValue() + id + ") ";
            } else {
                createQuery += createRelationshipQuery(entry) + entry.getValue() + id;
                createQuery += ":" +virtualLabel 
                        + ":" + clusterLabel+ "{" + idProperty + ":\"V" + id + "\"}) ";
            }
        }
        createQuery += createRelationshipWithTarget(entry, createTargetSet);
        return createQuery;
    }

    private String createRelationshipWithTarget(Map.Entry<String, String> entry, Set<String> createTargetSet){
        String createQuery = "";
        for (String key : createTargetSet) {
            createQuery += createRelationshipQuery(entry) + key + ") ";
        }
        return createQuery;
    }

    private String mergeRelationship(Map.Entry<String, String> entry, int index, Set<String> createTargetSet) {
        Map<Long, Integer> keyIdShard = foundKeyWithIDAndShard.get(entry.getKey());
        long keyId = keyIdShard.keySet().iterator().next();
        int keyShard = keyIdShard.values().iterator().next();

        Map<Long, Integer> valueIdShard = foundKeyWithIDAndShard.get(entry.getValue());
        String matchKey = "match (" + entry.getKey() + ":" + clusterLabel +
                "{" +idProperty+ ": " + keyId + "}) ";
        String unwindStr = "UNWIND [";

        for (Map.Entry<Long, Integer> idShardEntry : valueIdShard.entrySet()) {
            long valueId = idShardEntry.getKey();
            int valueShard = idShardEntry.getValue();

            unwindStr += keyShard == valueShard ? "{" + idProperty + ": " + valueId + "}, " : "{" + idProperty + ":\"V" + valueId + "\"}, ";
        }
        unwindStr = unwindStr.substring(0, unwindStr.length()-2) + "] AS row ";

        String createQueryAnotherShard = unwindStr + matchKey +
                "merge (" + entry.getValue() + ":"+clusterLabel+"{"+idProperty+":row."+idProperty+"}) on create set " + entry.getValue() + ":"+virtualLabel+
                createRelationshipQuery(entry) + entry.getValue() + ")";
        neo4jHelper.runQuery(createQueryAnotherShard, null, keyShard);
        if(createTargetSet.size() !=0)
            return createRelationshipValueCreated(entry, index, createTargetSet);
        else
            return "";
    }

    private String createRelationshipQuery(Map.Entry<String, String> entry) {
        return " merge(" + entry.getKey() + ")-[:" + getRelationShipName(entry.getKey(), entry.getValue()) + "]->(";
    }

    private void setWithString() {
        if(createdKeyWithID.size() != 0) {
            withString = " with ";
            for (String key : createdKeyWithID.keySet()) {
                withString += key + ", ";
            }
            withString = withString.endsWith(", ") ? withString.substring(0, withString.length() - 2) : withString;
        }
    }
}
