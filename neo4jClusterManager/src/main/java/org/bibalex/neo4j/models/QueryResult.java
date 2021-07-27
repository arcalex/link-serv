package org.bibalex.neo4j.models;

import org.bibalex.neo4j.handlers.PropertiesHandler;
import org.bibalex.neo4j.handlers.QueryHandler;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;

import java.util.*;

public class QueryResult {

    public Map<String, Object> results;

    public Object handleNode(String queryKey, Node node, QueryHandler queryHandler, int resultId) {
        Map<String, Object> nodeMap = new HashMap<>();
        nodeMap.put("identity", node.get(PropertiesHandler.getConfigProperty("idProperty")));
        nodeMap.put("lables", node.labels());
        Map<String, Object> nodeProperties = new HashMap<>();
        for (String key : node.keys()) {
            if (key == PropertiesHandler.getConfigProperty("idProperty")
                    && checkIfNodeIsVirtual(node.get(key).asString(), queryKey, queryHandler, resultId)) {
                return null;
            }
            nodeProperties.put(key, node.get(key).asObject());
        }
        nodeMap.put("properties", nodeProperties);
        return nodeMap;
    }

    public Object handlePath(String queryKey, Path path, Node startNode, Node endNode, QueryHandler queryHandler, int resultId) {
        boolean hasVirtual = false;
        if (startNode == null && endNode == null) {

            String startNodeId = path.start().get(PropertiesHandler.getConfigProperty("idProperty")).asString();
            String endNodeId = path.end().get(PropertiesHandler.getConfigProperty("idProperty")).asString();
            if (checkIfNodeIsVirtual(startNodeId, "STARTEDGE_" + queryKey, queryHandler, resultId))
                hasVirtual = true;
            if (checkIfNodeIsVirtual(endNodeId, "ENDEDGE_" + queryKey, queryHandler, resultId))
                hasVirtual = true;
            if (hasVirtual) {
                queryHandler.addResultIdEdge(resultId, path);
                queryHandler.addResultIdQueryKey(resultId, queryKey);
                return null;
            }
        }

        Object start = handleNode(null, startNode == null ? path.start() : startNode, queryHandler, resultId);
        Object end = handleNode(null, endNode == null ? path.end() : endNode, queryHandler, resultId);

        ArrayList<Object> segments = new ArrayList<>();

        for (Relationship relationship : path.relationships()) {
            Map<String, Object> relationshipMap = new HashMap<>();
            Map<String, Object> segment = new HashMap<>();
            segment.put("start", start);
            relationshipMap.put("identity", relationship.id());
            relationshipMap.put("start", relationship.startNodeId());
            relationshipMap.put("end", relationship.endNodeId());
            relationshipMap.put("type", relationship.type());

            Map<String, Object> relationshipProperties = new HashMap<>();
            for (String key : relationship.keys()) {
                relationshipProperties.put(key, relationship.get(key).asObject());
            }
            relationshipMap.put("properties", relationshipProperties);

            segment.put("relationship", relationshipMap);
            segment.put("end", end);
            segments.add(segment);
        }

        Map<String, Object> pathMap = new HashMap<>();
        pathMap.put("start", start);
        pathMap.put("end", end);
        pathMap.put("segments", segments);
        return pathMap;
    }

    boolean checkIfNodeIsVirtual(String nodeId, String queryKey, QueryHandler queryHandler, int resultId) {
        if (nodeId.startsWith("V")) {
            queryHandler.addNodeIdQueryKey(Long.valueOf(nodeId.replace("V", "")), queryKey);
            if(resultId != -1)
                queryHandler.addNodeIdResultId(Long.valueOf(nodeId.replace("V", "")), resultId);
            return true;
        }
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        QueryResult queryResult = (QueryResult) obj;
        return this.results.equals(queryResult.results);
    }

    @Override
    public int hashCode() {
        return Objects.hash(results);
    }
}
