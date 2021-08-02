package org.bibalex.linkserv.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bibalex.linkserv.models.Edge;
import org.bibalex.linkserv.models.HistogramEntry;
import org.bibalex.linkserv.models.Node;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;

import static java.util.Objects.isNull;

public class JSONHandler {

    private static final int DEFAULT_ATTRIBUTE_VALUE = 0;
    private static final Logger LOGGER = LogManager.getLogger(JSONHandler.class);
    private ArangoDBHandler arangoDBHandler;
    int versionNodesCount;
    private Map<String, Node> graphNodes;
    private ArrayList<Edge> graphEdges;
    private boolean multipleIdentifiers;
    private ArrayList<String> getGraphResults;
    private HashSet<String> getGraphResultsHashSet;
    private int latestVersionDepth;

    public void initialize(boolean multipleIdentifiersVariable) {
        graphNodes = new HashMap<>();
        graphEdges = new ArrayList<>();
        multipleIdentifiers = multipleIdentifiersVariable;
        versionNodesCount = 0;
        arangoDBHandler = new ArangoDBHandler();
    }

    public boolean addNodesAndEdgesFromJSONLine(String jsonLine, String identifier, String timestamp) {
        JSONObject jsonData = new JSONObject(jsonLine);
        //WHY "NOT ISNULL?" AND NOT HASKEY?
        if (!jsonData.isNull(PropertiesHandler.getProperty("addNodeKey"))) {
            //LOGGER.info("Adding node through line: " + jsonLine);
            return handleNode(jsonData, identifier, timestamp);
        } else {
            //LOGGER.info("Adding edge through line: " + jsonLine);
            handleEdge(jsonData);
            return true;
        }
    }

    private boolean handleNode(JSONObject jsonData, String identifier, String timestamp) {

        JSONObject jsonNode = jsonData.getJSONObject(PropertiesHandler.getProperty("addNodeKey"));
        String nodeId = jsonNode.keys().next();
        JSONObject JsonNodeProperties = jsonNode.getJSONObject(nodeId);
        if (JsonNodeProperties.isNull(PropertiesHandler.getProperty("versionKey"))) {
            Node node = new Node(nodeId, PropertiesHandler.getProperty("parentNodeLabel"),
                    JsonNodeProperties.getString(PropertiesHandler.getProperty("nameKey")), null);
            graphNodes.put(nodeId, node);
//            LOGGER.info("Node added: " + nodeId);
        } else {
            versionNodesCount++;
            Node node = new Node(nodeId, PropertiesHandler.getProperty("versionNodeLabel"),
                    JsonNodeProperties.getString(PropertiesHandler.getProperty("nameKey")),
                    JsonNodeProperties.getString(PropertiesHandler.getProperty("versionKey")));
            if (!multipleIdentifiers) {
                if (versionNodesCount > 1 || !node.getTimestamp().equals(timestamp) || !node.getIdentifier().equals(identifier))
                    return false;
            }
            graphNodes.put(nodeId, node);
//            LOGGER.info("VersionNode added: " + nodeId);
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

    public ArrayList<String> getGraph(String identifier, String timestamp, Integer depth) {
        return runGetGraphResults(arangoDBHandler.getRootNode(identifier, timestamp), depth, null, null);
    }

    public ArrayList<String> getGraph(String identifier, String startTimestamp, String endTimestamp, Integer depth) {
        return runGetGraphResults(arangoDBHandler.getRootNodes(identifier, startTimestamp, endTimestamp), depth, startTimestamp, endTimestamp);
    }

    public ArrayList<String> getVersions(String identifier, String dateTime) {
        ArrayList<Node> versionNodes = arangoDBHandler.getVersions(identifier, dateTime);
        ArrayList<String> nodeVersions = new ArrayList<>();

        for (Node versionNode : versionNodes) {
            nodeVersions.add(versionNode.getTimestamp());
        }
        return nodeVersions;
    }

    public ArrayList<String> getLatestVersion(String identifier) {

        latestVersionDepth = Integer.parseInt(PropertiesHandler.getProperty("latestVersionDepth"));
        ArrayList<Node> latestVersionNodes = arangoDBHandler.getLatestVersion(identifier);
        if (latestVersionNodes.isEmpty())
            return new ArrayList<>();
        Node latestVersionNode = latestVersionNodes.get(0);
        return getGraph(latestVersionNode.getIdentifier(), latestVersionNode.getTimestamp(), latestVersionDepth);
    }

    public String getVersionCountsYearly(String identifier) {

        ArrayList<HistogramEntry> histogramEntries = arangoDBHandler.getVersionCountsYearly(identifier);
        LOGGER.info("Response received");
        return convertHistogramArrayToJson(histogramEntries);
    }

    public String getVersionCountsMonthly(String identifier, int year) {

        ArrayList<HistogramEntry> histogramEntries = arangoDBHandler.getVersionCountsMonthly(identifier, year);
        return convertHistogramArrayToJson(histogramEntries);
    }

    public String getVersionCountsDaily(String identifier, int year, int month) {

        ArrayList<HistogramEntry> histogramEntries = arangoDBHandler.getVersionCountsDaily(identifier, year, month);
        return convertHistogramArrayToJson(histogramEntries);
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

    private ArrayList<String> runGetGraphResults(ArrayList<Node> rootNodes, int depth, String startTimestamp, String endTimestamp) {
        getGraphResultsHashSet = new HashSet<>();
        getGraphResults = new ArrayList<>();
        ArrayList<Object> outlinks;

        for (Node rootNode : rootNodes) {
            if (isNull(rootNode)) {
                getGraphResults.add(String.valueOf(new JSONObject()));
                LOGGER.info("No results found");
                return getGraphResults;
            }
            getGraphResultsHashSet.add(String.valueOf((new JSONObject(rootNode))));
            outlinks = arangoDBHandler.getOutlinks(rootNode, depth, startTimestamp, endTimestamp);
            for (Object outlink : outlinks){
                getGraphResultsHashSet.add(String.valueOf(new JSONObject(outlink)));
            }
        }
        return convertObjectStringToJSONString(getGraphResultsHashSet);
    }

    private ArrayList<String> convertObjectStringToJSONString(HashSet<String> graphResults) {
        ObjectMapper objectMapper = new ObjectMapper();
        HashSet<String> nodesHashSet = new HashSet<>();
        HashSet<String> edgesHashSet = new HashSet<>();
        try {
            for (String nodeMap : graphResults) {
                if (nodeMap.contains(PropertiesHandler.getProperty("nameKey"))) {
                    Node node = objectMapper.readValue(nodeMap, Node.class);
                    nodesHashSet.add(addNodeToResults(node));
                } else {
                    Edge edge = objectMapper.readValue(nodeMap, Edge.class);
                    edgesHashSet.add(addEdgeToResults(edge));
                }
            }
            getGraphResults.addAll(nodesHashSet);
            getGraphResults.addAll(edgesHashSet);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return getGraphResults;
    }

    private String addNodeToResults(Node node) {

        JSONObject outlinkNodeData = new JSONObject();
        outlinkNodeData.put(PropertiesHandler.getProperty("nameKey"), node.getIdentifier());
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
        outlinkEdgeData.put("weight", 1);
        JSONObject fullEdge = new JSONObject();
        fullEdge.put(edge.getId(), outlinkEdgeData);

        JSONObject edgeJSON = new JSONObject();

        edgeJSON.put(PropertiesHandler.getProperty("addEdgeKey"), fullEdge);
        return edgeJSON.toString();
    }

    private JSONObject setDefaultNodeAttributes(JSONObject nodeData) {

        String[] colorAttributes = PropertiesHandler.getProperty("colorAttributes").split(",");
        String[] spatialCoordinates = PropertiesHandler.getProperty("spatialCoordinates").split(",");

        for (String colorAttribute : colorAttributes) {
            Random rand = new Random();
            nodeData.put(colorAttribute, rand.nextFloat());
        }

        for (String spatialCoordinate : spatialCoordinates) {
            Random rand = new Random();
            nodeData.put(spatialCoordinate, rand.nextInt(500));
        }

        return nodeData;
    }

    private String convertHistogramArrayToJson(ArrayList<HistogramEntry> histogramEntries) {
        JSONObject histogramJson = new JSONObject();
        for (HistogramEntry histogramEntry : histogramEntries) {
            histogramJson.put(String.valueOf(histogramEntry.getKey()), histogramEntry.getCount());
        }
        return histogramJson.toString();
    }
}