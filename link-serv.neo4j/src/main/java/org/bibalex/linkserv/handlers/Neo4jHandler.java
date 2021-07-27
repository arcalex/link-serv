package org.bibalex.linkserv.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bibalex.linkserv.models.Edge;
import org.bibalex.linkserv.models.HistogramEntry;
import org.bibalex.linkserv.models.Node;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.neo4j.driver.Values.parameters;

public class Neo4jHandler {

    private static final Logger LOGGER = LogManager.getLogger(Neo4jHandler.class);
    private String versionNodeLabel;
    private String parentNodeLabel;
    private String linkRelationshipType;
    private Session session;
    private String query;
    private Value parameterValues;
    private Driver driver;

    public Neo4jHandler() {
        this.versionNodeLabel = PropertiesHandler.getProperty("versionNodeLabel");
        this.parentNodeLabel = PropertiesHandler.getProperty("parentNodeLabel");
        this.linkRelationshipType = PropertiesHandler.getProperty("linkRelationshipType");
    }

    public Session getSession() {
        if (session == null || !session.isOpen()) {
            if (driver != null) {
                driver.close();
            }
            driver = GraphDatabase.driver(PropertiesHandler.getProperty("uri"));
            session = driver.session();
        }
        return session;
    }

    // get root node matching exact timestamp
    public ArrayList<Node> getRootNode(String url, String timestamp) {
        LOGGER.info("Getting root node of URL: " + url + " with timestamp: " + timestamp);
        parameterValues = parameters("version", timestamp, "url", url);

        query = "CALL linkserv." + PropertiesHandler.getProperty("getRootNodeProcedure") + "($url, $version);";

        return runGetNodeQuery(query, parameterValues);
    }

    // get root node matching range of timestamps
    public ArrayList<Node> getRootNodes(String url, String startTimestamp, String endTimestamp) {
        LOGGER.info("Getting root nodes of URL: " +
                url + " within the time range: [" + startTimestamp + ", " + endTimestamp + "]");
        parameterValues = parameters("url", url,
                "startTimestamp", startTimestamp,
                "endTimestamp", endTimestamp);

        query = "CALL linkserv." + PropertiesHandler.getProperty("getRootNodesProcedure") +
                "($url, $startTimestamp, $endTimestamp);";

        return runGetNodeQuery(query, parameterValues);
    }

    public ArrayList<Node> getVersions(String url, String dateTime) {
        LOGGER.info("Getting versions of URL: " + url + " on: " + dateTime);
        parameterValues = parameters("url", url, "dateTime", dateTime);
        query = "CALL linkserv." + PropertiesHandler.getProperty("getVersionsProcedure") +
                "($url, $dateTime);";
        return runGetNodeQuery(query, parameterValues);
    }

    public ArrayList<Node> getLatestVersion(String url) {
        LOGGER.info("Getting the latest version of URL: " + url);
        parameterValues = parameters("url", url);
        query = "CALL linkserv." + PropertiesHandler.getProperty("getLatestVersionProcedure") +
                "($url);";

        return runGetNodeQuery(query, parameterValues);
    }

    private ArrayList<Node> runGetNodeQuery(String query, Value parameterValues) {
        ArrayList<Node> resultNodes = new ArrayList<>();
        Node resultNode;

        Result result = getSession().run(query, parameterValues);
        while (result.hasNext()) {
            Record rootNodeRecord = result.next();
            resultNode = new Node(convertValueToString(rootNodeRecord.get("nodeId")),
                    versionNodeLabel,
                    convertValueToString(rootNodeRecord.get("nodeURL")),
                    convertValueToString(rootNodeRecord.get("versionName")));
            resultNodes.add(resultNode);
        }
        return resultNodes;
    }

    // get closest version to rootNodeVersion, we'll just assume they're the same for now
    public ArrayList<Object> getOutlinkNodes(String nodeName, String nodeVersion) {

        LOGGER.info("Getting outlinks of URL: " + nodeName + " with timestamp: " + nodeVersion);

        ArrayList<Object> outlinkEntities = new ArrayList();
        parameterValues = parameters("name", nodeName, "version", nodeVersion);

        query = "CALL linkserv." + PropertiesHandler.getProperty("getOutlinkNodesProcedure") + "($name, $version);";

        Result result = getSession().run(query, parameterValues);

        while (result.hasNext()) {

            Record resultRecord = result.next();
            Boolean isParent = convertValueToString(resultRecord.get("outlinkVersion")).equalsIgnoreCase("NULL");

            Node outlinkNode = new Node(convertValueToString(resultRecord.get("outlinkParentId")),
                    isParent ? parentNodeLabel : versionNodeLabel,
                    convertValueToString(resultRecord.get("outlinkName")),
                    isParent ? "" : convertValueToString(resultRecord.get("outlinkVersion")));

            Edge outlinkEdge = new Edge(convertValueToString(resultRecord.get("relationshipId")),
                    linkRelationshipType,
                    convertValueToString(resultRecord.get("parentId")),
                    convertValueToString(resultRecord.get("outlinkParentId")));

            outlinkEntities.add(outlinkNode);
            outlinkEntities.add(outlinkEdge);
        }
        return outlinkEntities;
    }

    public boolean addNodesAndRelationships(Map<String, Node> graphNodes, ArrayList<Edge> graphEdges, String sessionId) {

        LOGGER.info("Update Graph: Adding nodes and edges");
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
        JSONArray jsonGraph = new JSONArray();
        for (Map.Entry<String, ArrayList<String>> entry : nodeWithOutlinks.entrySet()) {
            if (!result) {
                LOGGER.info("Could not update graph");
                return false;
            }
            jsonGraph = addOneNodeWithItsOutlinks(entry, graphNodes, jsonGraph);
        }
        // if there are nodes not connected via edges
        for (Map.Entry<String, Node> entry : graphNodes.entrySet()) {
            if (!result) {
                LOGGER.info("Could not update graph");
                return false;
            }
            jsonGraph = addNodewithItsVersion(entry, jsonGraph);
        }
        result = neo4jAddNodesWithOutlinks(jsonGraph, sessionId);
        LOGGER.info("Graph has been successfully updated");
        return true;
    }

    public ArrayList<HistogramEntry> getVersionCountYearly(String url) {
        LOGGER.info("Getting version count per year for URL: " + url);
        parameterValues = parameters("url", url);
        query = "CALL linkserv." + PropertiesHandler.getProperty("getVersionCountYearlyProcedure") + "($url)";
        return getNeo4jResultAndCovertToHistogram(parameterValues, query);
    }

    public ArrayList<HistogramEntry> getVersionCountMonthly(String url, int year) {
        LOGGER.info("Getting version count per month for URL: " + url + " in the year: " + year);
        parameterValues = parameters("url", url, "year", year);
        query = "CALL linkserv." + PropertiesHandler.getProperty("getVersionCountMonthlyProcedure") + "($url,$year)";
        return getNeo4jResultAndCovertToHistogram(parameterValues, query);
    }

    public ArrayList<HistogramEntry> getVersionCountDaily(String url, int year, int month) {
        LOGGER.info("Getting version count per day for URL: " + url + " in year: " + year + " and month: " + month);
        parameterValues = parameters("url", url, "year", year, "month", month);
        query = "CALL linkserv." + PropertiesHandler.getProperty("getVersionCountDailyProcedure") + "($url,$year,$month)";
        return getNeo4jResultAndCovertToHistogram(parameterValues, query);
    }

    private String convertValueToString(Value value) {
        return String.valueOf(value).replace("\"", "");
    }

    private JSONArray addOneNodeWithItsOutlinks(Map.Entry<String, ArrayList<String>> entry, Map<String, Node> graphNodes, JSONArray jsonGraph) {
        String url = (graphNodes.get(entry.getKey())).getUrl();
        String timestamp = (graphNodes.get(entry.getKey())).getTimestamp();
        graphNodes.remove(entry.getKey());
        JSONObject jsonNodeWithOutlinks = new JSONObject();
        jsonNodeWithOutlinks.put(PropertiesHandler.getProperty("nodeKey"), url);
        jsonNodeWithOutlinks.put(PropertiesHandler.getProperty("versionKey"), timestamp);
        jsonNodeWithOutlinks.put(PropertiesHandler.getProperty("outlinksKey"), entry.getValue());
        jsonGraph.put(jsonNodeWithOutlinks);
        return jsonGraph;
    }

    private JSONArray addNodewithItsVersion(Map.Entry<String, Node> entry , JSONArray jsonGraph) {
        String url = entry.getValue().getUrl();
        String timestamp = entry.getValue().getTimestamp();
        // as it is useless to add node without version and edges
        if (timestamp != null) {
            JSONObject jsonNodeWithOutlinks = new JSONObject();
            jsonNodeWithOutlinks.put(PropertiesHandler.getProperty("nodeKey"), url);
            jsonNodeWithOutlinks.put(PropertiesHandler.getProperty("versionKey"), timestamp);
            jsonNodeWithOutlinks.put(PropertiesHandler.getProperty("outlinksKey"), new ArrayList<>());
            jsonGraph.put(jsonNodeWithOutlinks);
        }
        return jsonGraph;
    }

    private boolean neo4jAddNodesWithOutlinks(JSONArray jsonGraph, String sessionId) {
        LOGGER.info("Adding Nodes and edges");
        Value parameters = parameters("jsonGraph", jsonGraph.toString(),
                "sessionId", sessionId);
        String query = "CALL linkserv." + PropertiesHandler.getProperty("addNodesAndRelationshipsProcedure")
                + "($jsonGraph,$sessionId)";

        Result result = getSession().run(query, parameters);
        if (result.hasNext()) {
            LOGGER.info("Nodes has been successfully added");
            return true;
        } else {
            LOGGER.info("Could not add nodes");
            return false;
        }
    }

    private ArrayList<HistogramEntry> getNeo4jResultAndCovertToHistogram(Value parameters, String query) {
        ArrayList<HistogramEntry> histogramEntries = new ArrayList<>();
        Result result = getSession().run(query, parameters);

        while (result.hasNext()) {
            Record histogramRecord = result.next();
            HistogramEntry histogramEntry = new HistogramEntry(histogramRecord.get("key").asInt(),
                    histogramRecord.get("count").asInt());
            histogramEntries.add(histogramEntry);
        }
        return histogramEntries;
    }
}
