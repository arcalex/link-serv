package org.bibalex.neo4j.handlers;

import org.bibalex.neo4j.helpers.Neo4jHelper;
import org.bibalex.neo4j.models.QueryResult;
import org.bibalex.neo4j.models.QueryResultRead;
import org.neo4j.driver.*;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.util.Pair;

import java.util.*;
import java.util.concurrent.ExecutorService;

public class ReadHandler extends QueryHandler {

    public ReadHandler(ExecutorService executorService, Neo4jHelper neo4jHelper) {
        super(executorService, neo4jHelper);
    }

    public HashSet<QueryResult> executeRead(String query) {
        query = getKeysFromQuery(query);
        executeQueriesInShards(query, null, false);
        handleVirtualNodes();
        convertHashMapToHashSet();
        return queryResultHashSet;
    }

    @Override
    void getNeo4jResult(String query, int shard) {
        Result result = neo4jHelper.runQuery(query, null, shard);
        while (result.hasNext()) {
            count.incrementAndGet();
            Record record = result.next();
            List<Pair<String, Value>> properties = record.fields();
            queryResultMap.put(count.intValue(), new QueryResultRead(properties, this, count.intValue(), record));
        }
    }

    @Override
    void handleResultVirtualNodes(String query, Value parametersValue, int shard) {

        Result result = neo4jHelper.runQuery(query, parametersValue, shard);
        while (result.hasNext()) {
            Pair<String, Value> resultFields = result.next().fields().get(0);
            Node node = resultFields.value().asNode();
            long nodeId = node.get(PropertiesHandler.getConfigProperty("idProperty")).asLong();
            ArrayList<String> queryKeys = nodeIdQueryKeys.get(nodeId);
            for (String queryKey : queryKeys) {
                int resultId = nodeIdResultId.get(nodeId);
                QueryResult queryResult = queryResultMap.get(resultId);

                if (queryKey.startsWith("STARTEDGE_") || queryKey.startsWith("ENDEDGE_")) {
                    Path path = resultIdEdge.get(resultId);
                    if (queryKey.startsWith("STARTEDGE_"))
                        queryResult.handlePath(queryKey, path, node, null, ReadHandler.this, resultId);
                    else
                        queryResult.handlePath(queryKey, path, null, node, ReadHandler.this, resultId);
                    queryResultMap.put(resultId, queryResult);
                } else if (queryKey.contains(".")) {
                    queryResult.results.put(queryKey, node.get(queryKey.split("\\.")[1]).asObject());
                } else {
                    queryResult.handleNode(queryKey, node, ReadHandler.this, resultId);
                    queryResultMap.put(resultId, queryResult);
                }
            }
        }
    }

    private String getKeysFromQuery(String query) {
        HashSet<String> keys = new HashSet<>();
        query = query.replace(";", "");
        String returnString = query.split("(?i)return\\s+")[1].replaceAll("\\s+", "");
        String[] returnKeys;
        if (returnString.startsWith("SUBSTRING")) {
            int i = returnString.lastIndexOf(",");
            returnKeys = new String[]{returnString.substring(0, i), returnString.substring(i)};
        } else
            returnKeys = returnString.split(",");
        for (String returnKey : returnKeys) {
            if (returnKey.toLowerCase().contains("as")) {
                String[] keyValue = returnKey.split("AS");
                keyAsValue.put(keyValue[0], keyValue[1]);
                String key = keyValue[0].split("\\(")[1];
                String anotherKey = key.contains(".") ? key.split("\\.")[0] : key.split("\\)")[0];
                keys.add(anotherKey);
            } else if (returnKey.contains(".")) {
                keys.add(returnKey.split("\\.")[0]);
            } else if (returnKey.toLowerCase().startsWith("id(") && !returnKey.toLowerCase().startsWith("id(r")) {
                keys.add(returnKey.split("\\(")[1].split("\\)")[0]);
            }
        }
        return addReturnStatementsToQuery(query, keys);
    }
}
