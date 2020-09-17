package org.bibalex.linkserv.handlers;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bibalex.linkserv.models.Edge;
import org.bibalex.linkserv.models.Node;
import org.neo4j.driver.v1.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.neo4j.driver.v1.Values.parameters;

public class Neo4jHandler {

    private static final Logger LOGGER = LogManager.getLogger(Neo4jHandler.class);
    private String versionNodeLabel;
    private String parentNodeLabel;
    private String linkRelationshipType;
    private Session session;

    public Neo4jHandler() {
        this.versionNodeLabel = PropertiesHandler.getProperty("versionNodeLabel");
        this.parentNodeLabel = PropertiesHandler.getProperty("parentNodeLabel");
        this.linkRelationshipType = PropertiesHandler.getProperty("linkRelationshipType");
    }

    public Session getSession() {
        if (session == null || !session.isOpen()) {
            Driver driver = GraphDatabase.driver(PropertiesHandler.getProperty("uri"));
            session = driver.session();
        }

        return session;
    }

    // get root node matching range of timestamps
    public ArrayList<Node> getRootNodes(String url, String startTimestamp, String endTimestamp) {
        Value parameterValues;
        String query;
        ArrayList<Node> rootNodes = new ArrayList<>();
        Node rootNode;

        if (endTimestamp.isEmpty()) {
            LOGGER.info("Getting Root Node of URL: " + url + " with Timestamp: " + startTimestamp);
            parameterValues = parameters("version", startTimestamp, "url", url);

            query = "CALL linkserv." + PropertiesHandler.getProperty("getRootNodeProcedure") + "($url, $version);";
        } else {
            LOGGER.info("Getting root nodes in range: [" + startTimestamp + ", " + endTimestamp + "]");
            parameterValues = parameters("url", url,
                    "startTimestamp", startTimestamp,
                    "endTimestamp", endTimestamp);

            query = "CALL linkserv." + PropertiesHandler.getProperty("getRootNodesProcedure") +
                    "($url, $startTimestamp, $endTimestamp);";
        }

        Result result = getSession().run(query, parameterValues);
        while (result.hasNext()) {
            Record rootNodeRecord = result.next();
            System.out.println("Neo4j Handler: " + rootNodeRecord.get("nodeId"));
            rootNode = new Node(convertValueToString(rootNodeRecord.get("nodeId")),
                    versionNodeLabel,
                    convertValueToString(rootNodeRecord.get("parentName")),
                    convertValueToString(rootNodeRecord.get("versionName")));
            rootNodes.add(rootNode);
        }
        return rootNodes;
    }

    // get closest version to rootNodeVersion, we'll just assume they're the same for now
    public ArrayList<Object> getOutlinkNodes(String nodeName, String nodeVersion) {

        LOGGER.info("Getting Outlinks of Node of URL: " + nodeName + " with Timestamp: " + nodeVersion);

        ArrayList<Object> outlinkEntities = new ArrayList();
        Value parameterValues = parameters("name", nodeName, "version", nodeVersion);

        String query = "CALL linkserv." + PropertiesHandler.getProperty("getOutlinkNodesProcedure") + "($name, $version);";

        StatementResult result = getSession().run(query, parameterValues);

        while (result.hasNext()) {

            Record resultRecord = result.next();
            Boolean isParent = convertValueToString(resultRecord.get("outlinkVersion")).equalsIgnoreCase("NULL");

            Node outlinkNode = new Node(isParent ? convertValueToString(resultRecord.get("parentId")) :
                    convertValueToString(resultRecord.get("outlinkVersionId")),
                    isParent ? parentNodeLabel : versionNodeLabel,
                    convertValueToString(resultRecord.get("outlinkName")),
                    isParent ? "" : convertValueToString(resultRecord.get("outlinkVersion")));

            Edge outlinkEdge = new Edge(convertValueToString(resultRecord.get("relationshipId")),
                    linkRelationshipType,
                    convertValueToString(resultRecord.get("parentVersionId")),
                    isParent ? convertValueToString(resultRecord.get("parentId")) :
                            convertValueToString(resultRecord.get("outlinkVersionId")));

            outlinkEntities.add(outlinkNode);
            outlinkEntities.add(outlinkEdge);
        }
        return outlinkEntities;
    }

    private String convertValueToString(Value value) {
        return String.valueOf(value).replace("\"", "");
    }

    public boolean addNodesAndRelationships(Map<String, Node> graphNodes, ArrayList<Edge> graphEdges) {

        LOGGER.info("Update Graph: Adding Nodes and Edges");
        LOGGER.debug(graphNodes);

        Map<String, ArrayList<String>> nodeWithOutlinks = new HashMap<>();

        for (Edge edge : graphEdges) {
            String sourceNodeId = edge.getSource();
            ArrayList<String> outlinks = nodeWithOutlinks.get(sourceNodeId);
            if (outlinks == null)
                outlinks = new ArrayList<>();
            outlinks.add((graphNodes.get(edge.getTarget())).getUrl());
            graphNodes.remove(edge.getTarget());
            nodeWithOutlinks.put(sourceNodeId, outlinks);
        }
        boolean result = true;
        for (Map.Entry<String, ArrayList<String>> entry : nodeWithOutlinks.entrySet()) {
            if (!result) {
                LOGGER.info("Could not Update Graph");
                return false;
            }
            result = addOneNodeWithItsOutlinks(entry, graphNodes);
        }
        // if there are nodes not connected via edges
        for (Map.Entry<String, Node> entry : graphNodes.entrySet()) {
            if (!result) {
                LOGGER.info("Could not Update Graph");
                return false;
            }
            result = addNodewithItsVersion(entry);
        }
        LOGGER.info("Graph Updated Successfully");
        return true;
    }

    private boolean addOneNodeWithItsOutlinks(Map.Entry<String, ArrayList<String>> entry, Map<String, Node> graphNodes) {
        String url = (graphNodes.get(entry.getKey())).getUrl();
        String timestamp = (graphNodes.get(entry.getKey())).getTimestamp();
        graphNodes.remove(entry.getKey());
        return neo4jAddNodeWithOutlinks(url, timestamp, entry.getValue());
    }

    private boolean addNodewithItsVersion(Map.Entry<String, Node> entry) {
        String url = entry.getValue().getUrl();
        String timestamp = entry.getValue().getTimestamp();
        // would be useless to add node without version or edges
        if (timestamp != null) {
            return neo4jAddNodeWithOutlinks(url, timestamp, new ArrayList<String>());
        }
        return true;
    }

    private boolean neo4jAddNodeWithOutlinks(String url, String timestamp, ArrayList<String> outlinks) {
        Value parameters = parameters("url", url, "timestamp", timestamp, "outlinks", convertArrayToJSONArray(outlinks));
        String query = "CALL linkserv." + PropertiesHandler.getProperty("addNodesAndRelationshipsProcedure")
                + "($url,$timestamp,$outlinks)";

        Result result = getSession().run(query, parameters);
        if (result.hasNext()) {
            LOGGER.info("Node with url: " + url + " and timestamp: " + timestamp + " added Successfully");
            return true;
        } else {
            LOGGER.info("Could not add Node with url: " + url + " and timestamp: " + timestamp);
            return false;
        }
    }

    private List<Map<String, Object>> convertArrayToJSONArray(ArrayList<String> outlinks) {
        List<Map<String, Object>> list = new ArrayList<>();
        for (String url : outlinks) {
            Map<String, Object> stringObjectMap = new HashMap<>();
            stringObjectMap.put("url", url);
            list.add(stringObjectMap);
        }
        return list;
    }
}
