package org.bibalex.linkserv.handlers;

import org.bibalex.linkserv.models.Edge;
import org.bibalex.linkserv.models.HistogramEntry;
import org.bibalex.linkserv.models.Node;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class JSONHandler {

    private static final int DEFAULT_ATTRIBUTE_VALUE = 1;
    private static final Logger LOGGER = LogManager.getLogger(JSONHandler.class);
    int versionNodesCount;
    private Neo4jHandler neo4jHandler;
    private Map<String, Node> graphNodes;
    private ArrayList<Edge> graphEdges;
    private boolean multipleURLs;
    private ArrayList<String> getGraphResults;

    public JSONHandler(boolean multipleURLs) {
        this.neo4jHandler = new Neo4jHandler();
        this.graphNodes = new HashMap<>();
        this.graphEdges = new ArrayList<>();
        this.multipleURLs = multipleURLs;
        this.versionNodesCount = 0;
    }

    public boolean addNodesAndEdgesFromJSONLine(String jsonLine, String url, String timestamp) {

        JSONObject jsonData = new JSONObject(jsonLine);
        if (!jsonData.isNull(PropertiesHandler.getProperty("addNodeKey"))) {
            LOGGER.info("Adding Node through Line: " + jsonLine);
            return handleNode(jsonData, url, timestamp);
        } else {
            LOGGER.info("Adding Edge through Line: " + jsonLine);
            handleEdge(jsonData);
            return true;
        }
    }

    private boolean handleNode(JSONObject jsonData, String url, String timestamp) {

        JSONObject jsonNode = jsonData.getJSONObject(PropertiesHandler.getProperty("addNodeKey"));
        String nodeId = jsonNode.keys().next();
        JSONObject JsonNodeProperties = jsonNode.getJSONObject(nodeId);
        if (JsonNodeProperties.isNull(PropertiesHandler.getProperty("versionKey"))) {
            Node node = new Node(nodeId, PropertiesHandler.getProperty("parentNodeLabel"),
                    JsonNodeProperties.getString(PropertiesHandler.getProperty("nameKey")), null);
            graphNodes.put(nodeId, node);
            LOGGER.info("Parent Node Added: " + nodeId);
        } else {
            Node node = new Node(nodeId, PropertiesHandler.getProperty("versionNodeLabel"),
                    JsonNodeProperties.getString(PropertiesHandler.getProperty("nameKey")),
                    JsonNodeProperties.getString(PropertiesHandler.getProperty("versionKey")));
            versionNodesCount++;
            if (!multipleURLs) {
                if (versionNodesCount > 1 || !node.getTimestamp().equals(timestamp) || !node.getUrl().equals(url))
                    return false;
            }
            graphNodes.put(nodeId, node);
            LOGGER.info("Version Node Added: " + nodeId);
        }
        return true;
    }

    private void handleEdge(JSONObject jsonData) {

        JSONObject jsonEdge = jsonData.getJSONObject(PropertiesHandler.getProperty("addEdgeKey"));
        String edgeId = jsonEdge.keys().next();
        JSONObject JsonEdgeProperties = jsonEdge.getJSONObject(edgeId);
        Edge edge = new Edge(edgeId, PropertiesHandler.getProperty("linkRelationshipType"),
                JsonEdgeProperties.getString(PropertiesHandler.getProperty("sourceKey")),
                JsonEdgeProperties.getString(PropertiesHandler.getProperty("targetKey")));
        graphEdges.add(edge);
    }

    public ArrayList<String> getGraph(String url, String timestamp, Integer depth) {
        return runGetGraphResults(neo4jHandler.getRootNode(url, timestamp), depth);
    }

    public ArrayList<String> getGraph(String url, String startTimestamp, String endTimestamp, Integer depth) {
        return runGetGraphResults(neo4jHandler.getRootNodes(url, startTimestamp, endTimestamp), depth);
    }

    private boolean validatePresenceOfAttributes(Node rootNode, int depth) {
        if (rootNode == null || depth < 1) {
            return false;
        }
        return true;
    }

    private ArrayList<String> runGetGraphResults(ArrayList<Node> rootNodes, int depth) {
        getGraphResults = new ArrayList<>();
        String nodeVersion = "";
        ArrayList<String> nodesNames = new ArrayList<>();

        for (Node rootNode : rootNodes) {
            if (!validatePresenceOfAttributes(rootNode, depth)) {
                getGraphResults.add(new JSONObject().toString());
                LOGGER.info("No Results Found or Invalid Depth");
                return getGraphResults;
            }

            // get root node timestamp to later on implement approximation
            nodeVersion = rootNode.getTimestamp();
            nodesNames.add(rootNode.getUrl());
            getGraphResults.add(addNodeToResults(rootNode));

            for (int i = 0; i < depth; i++) {
                nodesNames = getOutlinkNodes(nodesNames, nodeVersion);
            }
        }

        return getGraphResults;
    }

    private ArrayList<String> getOutlinkNodes(ArrayList<String> nodesNames, String nodeVersion) {

        ArrayList<String> outlinkNodes = new ArrayList<>();
        for (String nodeName : nodesNames) {
            ArrayList<Object> outlinkData = neo4jHandler.getOutlinkNodes(nodeName, nodeVersion);
            for (Object nodeMap : outlinkData) {
                if (nodeMap.getClass() == Node.class) {
                    getGraphResults.add(addNodeToResults((Node) nodeMap));
                    outlinkNodes.add(((Node) nodeMap).getUrl());
                } else
                    getGraphResults.add(addEdgeToResults((Edge) nodeMap));
            }
            return results;
        }
    }

    private String addNodeToResults(Node node) {

        JSONObject outlinkNodeData = new JSONObject();
        outlinkNodeData.put(PropertiesHandler.getProperty("nameKey"), node.getUrl());
        outlinkNodeData.put(PropertiesHandler.getProperty("versionKey"), node.getTimestamp());
        outlinkNodeData.put(PropertiesHandler.getProperty("typeKey"), node.getType());
        outlinkNodeData = setDefaultNodeAttributes(outlinkNodeData);

        JSONObject fullNode = new JSONObject();
        String nodeID = node.getId();
        fullNode.put(nodeID, outlinkNodeData);

        JSONObject nodeJSON = new JSONObject();
        nodeJSON.put(PropertiesHandler.getProperty("addNodeKey"), fullNode);

        return nodeJSON.toString();
    }

    private String addEdgeToResults(Edge edge) {

        JSONObject outlinkEdgeData = new JSONObject();

        outlinkEdgeData.put(PropertiesHandler.getProperty("sourceKey"), edge.getSource());
        outlinkEdgeData.put(PropertiesHandler.getProperty("targetKey"), edge.getTarget());
        outlinkEdgeData.put(PropertiesHandler.getProperty("typeKey"), edge.getType());
        outlinkEdgeData.put("directed", true);
        outlinkEdgeData.put("r", DEFAULT_ATTRIBUTE_VALUE);
        outlinkEdgeData.put("g", DEFAULT_ATTRIBUTE_VALUE);
        outlinkEdgeData.put("b", DEFAULT_ATTRIBUTE_VALUE);
        outlinkEdgeData.put("weight", DEFAULT_ATTRIBUTE_VALUE);

        JSONObject fullEdge = new JSONObject();
        fullEdge.put(edge.getId(), outlinkEdgeData);

        JSONObject edgeJSON = new JSONObject();
        edgeJSON.put(PropertiesHandler.getProperty("addEdgeKey"), fullEdge);

        return edgeJSON.toString();
    }

    private JSONObject setDefaultNodeAttributes(JSONObject nodeData) {

        String[] attributes = PropertiesHandler.getProperty("nodeAttributes").split(",");

        for (String attribute : attributes) {
            nodeData.put(attribute, DEFAULT_ATTRIBUTE_VALUE);
        }
        return nodeData;
    }

    public String getVersionCountYearly(String url) {

        ArrayList<HistogramEntry> histogramEntries = neo4jHandler.getVersionCountYearly(url);
        return convertHistogramArrayToJson(histogramEntries);
    }

    public String getVersionCountMonthly(String url, int year) {

        ArrayList<HistogramEntry> histogramEntries = neo4jHandler.getVersionCountMonthly(url, year);
        return convertHistogramArrayToJson(histogramEntries);
    }

    public String getVersionCountDaily(String url, int year, int month) {

        ArrayList<HistogramEntry> histogramEntries = neo4jHandler.getVersionCountDaily(url, year, month);
        return convertHistogramArrayToJson(histogramEntries);
    }

    private String convertHistogramArrayToJson(ArrayList<HistogramEntry> histogramEntries) {
        JSONObject histogramJson = new JSONObject();
        for (HistogramEntry histogramEntry : histogramEntries) {
            histogramJson.put(String.valueOf(histogramEntry.getKey()), histogramEntry.getCount());
        }
        return histogramJson.toString();
    }

    public Map<String, Node> getGraphNodes() {
        return graphNodes;
    }

    public void setGraphNodes(Map<String, Node> graphNodes) {
        this.graphNodes = graphNodes;
    }

    public ArrayList<Edge> getGraphEdges() {
        return graphEdges;
    }

    public void setGraphEdges(ArrayList<Edge> graphEdges) {
        this.graphEdges = graphEdges;
    }

}
