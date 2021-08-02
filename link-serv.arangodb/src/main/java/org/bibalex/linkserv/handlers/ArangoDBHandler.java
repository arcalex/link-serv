package org.bibalex.linkserv.handlers;

import com.arangodb.*;
import com.arangodb.entity.*;
import com.arangodb.util.MapBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bibalex.linkserv.models.Edge;
import org.bibalex.linkserv.models.HistogramEntry;
import org.bibalex.linkserv.models.Node;
import org.json.JSONObject;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.util.Objects.isNull;
import static org.bibalex.linkserv.handlers.PropertiesHandler.getProperty;
import static org.bibalex.linkserv.handlers.PropertiesHandler.initializeProperties;

public class ArangoDBHandler {
    private static final Logger LOGGER = LogManager.getLogger(ArangoDBHandler.class);
    private String nodesCollectionName;
    private String edgesCollectionName;
    private String versionNodeLabel;
    private String parentNodeLabel;
    private String nameKey;
    private String versionKey;
    private String nodeLabelAttribute;
    private String keyAttribute;
    private String requestIdAttribute;
    private ArangoDBConnectionHandler arangoDBConnectionHandler;
    private ArangoCollection nodesCollection;
    private String query;
    private MapBuilder paramMapBuilder;
    private HashMap<String, String> nodeIDs;
    private static final String staticStartTimestamp = "10101000000";

    public ArangoDBHandler() {
        initializeProperties();
        this.nodesCollectionName = getProperty("nodesCollection");
        this.edgesCollectionName = getProperty("edgesCollection");
        this.parentNodeLabel = getProperty("parentNodeLabel");
        this.versionNodeLabel = getProperty("versionNodeLabel");
        this.nameKey = getProperty("nameKey");
        this.versionKey = getProperty("versionKey");
        this.nodeLabelAttribute = getProperty("nodeLabelAttribute");
        this.keyAttribute = getProperty("keyAttribute");
        this.requestIdAttribute = getProperty("requestIdAttribute");
        this.arangoDBConnectionHandler = ArangoDBConnectionHandler.getInstance();
        this.nodeIDs = new HashMap<>();
        this.nodesCollection = arangoDBConnectionHandler.nodesCollection;
    }

    //get the root node of the exact given timestamp
    public ArrayList<Node> getRootNode(String identifier, String timestamp) {
        paramMapBuilder = new MapBuilder().
                put("@nodesCollection", nodesCollectionName).
                put("nameKey", nameKey).
                put("identifier", identifier).
                put("versionKey", versionKey).
                put("timestamp", timestamp);

        LOGGER.info("Getting root node of Identifier: " + identifier + " with timestamp: " + timestamp);
        ArrayList<Node> rootNodeArray = new ArrayList<>();
        query = "FOR node IN @@nodesCollection FILTER node.@nameKey == @identifier \n" +
                "AND node.@versionKey == TO_STRING(@timestamp) RETURN node";
        ArangoCursor<Object> rootNodeArangoCursor = arangoDBConnectionHandler.executeLinkDataQuery(query, paramMapBuilder);
        if (rootNodeArangoCursor.hasNext()) {
            rootNodeArray.add(createNewNode((HashMap) rootNodeArangoCursor.next()));
        }
        try {
            rootNodeArangoCursor.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rootNodeArray;
    }

    //get the root nodes within a given time range
    public ArrayList<Node> getRootNodes(String identifier, String startTimestamp, String endTimestamp) {
        //TO-DO: refactor
        startTimestamp = startTimestamp.isEmpty() ? staticStartTimestamp : startTimestamp;
        endTimestamp = endTimestamp.isEmpty() ? getCurrentTime() : endTimestamp;

        paramMapBuilder = new MapBuilder().
                put("@nodesCollection", nodesCollectionName).
                put("nameKey", nameKey).
                put("identifier", identifier).
                put("versionKey", versionKey).
                put("startTimestamp", startTimestamp).
                put("endTimestamp", endTimestamp);

        LOGGER.info("Getting root nodes of identifier: " + identifier + " within the time range: [" + startTimestamp + ", " + endTimestamp + "]");
        ArrayList<Node> rootNodesArray = new ArrayList<>();

        query = "FOR node IN @@nodesCollection FILTER node.@nameKey == @identifier \n" +
                "AND TO_NUMBER(node.@versionKey) IN TO_NUMBER(@startTimestamp)..TO_NUMBER(@endTimestamp) RETURN node";
        ArangoCursor<Object> rootNodeArangoCursor = arangoDBConnectionHandler.executeLinkDataQuery(query, paramMapBuilder);

        while (rootNodeArangoCursor.hasNext()) {
            rootNodesArray.add(createNewNode((HashMap) rootNodeArangoCursor.next()));
        }
        try {
            rootNodeArangoCursor.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rootNodesArray;
    }

    private String getCurrentTime() {
        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        DateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        simpleDateFormat.setTimeZone(timeZone);
        return simpleDateFormat.format(new Date());
    }

    public ArrayList<Object> getOutlinks(Node rootNode, Integer depth, String startTimestamp, String endTimestamp) {
        //getOutlinks
        String nodesPrefix = nodesCollectionName + "/";
        ArrayList<Object> outlinks = new ArrayList<>();
        ArrayList<HashMap> outlinksPerStep;
        ArrayList<Node> nodes = new ArrayList<>();
        ArrayList<Node> returnedNodes = new ArrayList<>();
        String rootNodeId = nodesPrefix + rootNode.getId();
        nodes.add(rootNode);
        LOGGER.info("Getting outlinks of node: " + rootNodeId);
        /** 1- Get direct outlinks
         * 2- for every node, check if there is an existing version with the same version as the root node's,
         * return it if found and modify the edge accordingly
         */
        for (int i = 0; (i < depth && !(nodes.isEmpty())); i++) {
            for (Node node : nodes) {
                if(node.getType().equals(versionNodeLabel)){
                    outlinksPerStep = getOutlinksPerStep(node);
                    ArrayList<Node> existingVersions;
                    Edge outlinkEdge;
                    for (HashMap outlinkObject : outlinksPerStep) {
                        Object returnedOutlinkNode = outlinkObject.get("node");
                        String outlinkEdgeId = String.valueOf(outlinkObject.get("edgeID"));

                        existingVersions = getExistingVersions((HashMap) returnedOutlinkNode, rootNode, startTimestamp, endTimestamp);

                        if(existingVersions.isEmpty()) {
                            Node outlinkNode = createNewNode((HashMap) returnedOutlinkNode);
                            outlinks.add(outlinkNode);
                            outlinkEdge = new Edge(outlinkEdgeId, edgesCollectionName, node.getId(), outlinkNode.getId());
                            outlinks.add(outlinkEdge);
                        }
                        else {
                            outlinks.addAll(existingVersions);
                            for(Node existingVersion : existingVersions) {
                                outlinkEdge = new Edge(outlinkEdgeId, edgesCollectionName, node.getId(), existingVersion.getId());
                                outlinks.add(outlinkEdge);
                            }
                        }
                        returnedNodes.addAll(existingVersions);
                    }
                }
            }
            nodes.clear();
            nodes.addAll(returnedNodes);
            returnedNodes.clear();
        }
        return outlinks;
    }

    private ArrayList<HashMap> getOutlinksPerStep(Node node) {
        ArrayList<HashMap> outlinks = new ArrayList<>();
        paramMapBuilder = new MapBuilder().
                put("@nodesCollection", nodesCollectionName).
                put("nodeID", nodesCollectionName + "/" + node.getId()).
                put("@edgesCollection", edgesCollectionName);

        query = "WITH @@nodesCollection FOR node, edge IN 1..1 OUTBOUND @nodeID @@edgesCollection \n" +
                "RETURN {node: node, edgeID: edge._key}";

        ArangoCursor<Object> outlinksArangoCursor = arangoDBConnectionHandler.executeLinkDataQuery(query, paramMapBuilder);
        while (outlinksArangoCursor.hasNext()) {
            outlinks.add((HashMap) outlinksArangoCursor.next());
        }
        try {
            outlinksArangoCursor.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return outlinks;
    }

    private ArrayList<Node> getExistingVersions(HashMap node, Node rootNode, String startTimestamp, String endTimestamp) {
        ArrayList<Node> existingVersions = new ArrayList<>();


        if(startTimestamp == null && endTimestamp == null) {
            paramMapBuilder = new MapBuilder().
                    put("@nodesCollection", nodesCollectionName).
                    put("nameKey", nameKey).
                    put("identifier", node.get(nameKey)).
                    put("versionKey", versionKey).
                    put("timestamp", rootNode.getTimestamp());
            query = "FOR node IN @@nodesCollection FILTER node.@nameKey == @identifier \n" +
                    "AND node.@versionKey == TO_STRING(@timestamp) \n" +
                    "RETURN node";
        }
        else {
            paramMapBuilder = new MapBuilder().
                    put("@nodesCollection", nodesCollectionName).
                    put("nameKey", nameKey).
                    put("identifier", node.get(nameKey)).
                    put("versionKey", versionKey).
                    put("startTimestamp", startTimestamp).
                    put("endTimestamp", endTimestamp);

            query = "FOR node IN @@nodesCollection FILTER node.@nameKey == @identifier \n" +
                    "AND TO_NUMBER(node.@versionKey) IN TO_NUMBER(@startTimestamp)..TO_NUMBER(@endTimestamp) \n" +
                    "RETURN node";
        }

        ArangoCursor<Object> existingVersionArangoCursor = arangoDBConnectionHandler.executeLinkDataQuery(query, paramMapBuilder);
        while (existingVersionArangoCursor.hasNext()) {
            HashMap<String, String> existingVersion = (HashMap) existingVersionArangoCursor.next();
            try {
                existingVersionArangoCursor.close();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
            existingVersions.add(createNewNode(existingVersion));
        }
        return existingVersions;
    }

    public ArrayList<Node> getVersions(String identifier, String dateTime) {
        ArrayList<Node> nodeVersions = new ArrayList<>();
        LOGGER.info("Getting versions of identifier: " + identifier + " on " + dateTime);

        paramMapBuilder = new MapBuilder().
                put("@nodesCollection", nodesCollectionName).
                put("nameKey", nameKey).
                put("identifier", identifier).
                put("versionKey", versionKey).
                put("dateTime", dateTime + "%");

        query = "FOR node IN @@nodesCollection FILTER node.@nameKey == @identifier \n" +
                "AND LIKE(node.@versionKey, @dateTime) \n" +
                "RETURN node";

        ArangoCursor<Object> nodeVersionsCursor = arangoDBConnectionHandler.executeLinkDataQuery(query, paramMapBuilder);
        while (nodeVersionsCursor.hasNext()) {
            nodeVersions.add(createNewNode((HashMap<String, String>) nodeVersionsCursor.next()));
        }
        try {
            nodeVersionsCursor.close();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return nodeVersions;
    }

    public ArrayList<Node> getLatestVersion(String identifier) {
        ArrayList<Node> latestRootNodeArray = new ArrayList<>();

        LOGGER.info("Getting the latest version of identifier: " + identifier);

        paramMapBuilder = new MapBuilder().
                put("@nodesCollection", nodesCollectionName).
                put("nameKey", nameKey).
                put("identifier", identifier).
                put("versionKey", versionKey).
                put("limit", 1);

        query = "FOR node IN @@nodesCollection FILTER node.@nameKey == @identifier \n" +
                "SORT node.@versionKey DESC LIMIT @limit RETURN node";

        ArangoCursor<Object> rootNodeArangoCursor = arangoDBConnectionHandler.executeLinkDataQuery(query, paramMapBuilder);

        if (rootNodeArangoCursor.hasNext()) {
            latestRootNodeArray.add(createNewNode((HashMap) rootNodeArangoCursor.next()));
        }
        try {
            rootNodeArangoCursor.close();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return latestRootNodeArray;
    }

    private Node createNewNode(HashMap<String, String> recordHashMap) {
        String nodeLabel = recordHashMap.get(nodeLabelAttribute).replace("\"", "");
        String nodeVersion = nodeLabel == parentNodeLabel ? "" : recordHashMap.get(versionKey);
        return (new Node(String.valueOf(recordHashMap.get(keyAttribute)),
                nodeLabel,
                String.valueOf(recordHashMap.get(nameKey)).replace("\"", ""),
                nodeVersion));
    }

    public ArrayList<HistogramEntry> getVersionCountsYearly(String identifier) {
        ArrayList<HistogramEntry> versionCount = new ArrayList<>();
        LOGGER.info("Getting version count per year for the identifier: " + identifier);
        paramMapBuilder = new MapBuilder().
                put("@nodesCollection", nodesCollectionName).
                put("nameKey", nameKey).
                put("identifier", identifier).
                put("nodeLabelAttribute", nodeLabelAttribute).
                put("versionNodeLabel", versionNodeLabel).
                put("versionKey", versionKey);

        query = "FOR node IN @@nodesCollection FILTER node.@nameKey == @identifier \n" +
                "AND node.@nodeLabelAttribute == @versionNodeLabel \n" +
                "COLLECT year = SUBSTRING(node.@versionKey,0,4) INTO versionsPerYear \n" +
                "RETURN {key: year, count: COUNT(versionsPerYear[*])}";
        ArangoCursor<Object> versionCountArangoCursor = arangoDBConnectionHandler.executeLinkDataQuery(query, paramMapBuilder);
        if (versionCountArangoCursor.hasNext())
            versionCount = getVersionCountResults(versionCountArangoCursor);

        try {
            versionCountArangoCursor.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return versionCount;
    }

    public ArrayList<HistogramEntry> getVersionCountsMonthly(String identifier, int year) {
        ArrayList<HistogramEntry> versionCount = new ArrayList<>();
        LOGGER.info("Getting version count per month for the identifier: " + identifier + " in " + year);
        paramMapBuilder = new MapBuilder().
                put("@nodesCollection", nodesCollectionName).
                put("nameKey", nameKey).
                put("identifier", identifier).
                put("nodeLabelAttribute", nodeLabelAttribute).
                put("versionNodeLabel", versionNodeLabel).
                put("versionKey", versionKey).
                put("year", year);

        query = "FOR node IN @@nodesCollection FILTER node.@nameKey == @identifier \n" +
                "AND node.@nodeLabelAttribute == @versionNodeLabel \n" +
                "AND TO_NUMBER(SUBSTRING(node.@versionKey,0,4)) == @year \n" +
                "COLLECT month = SUBSTRING(node.@versionKey,4,2) INTO versionsPerMonth \n" +
                "RETURN {key: month, count: COUNT(versionsPerMonth[*])}";

        ArangoCursor<Object> versionCountArangoCursor = arangoDBConnectionHandler.executeLinkDataQuery(query, paramMapBuilder);
        if (versionCountArangoCursor.hasNext())
            versionCount = getVersionCountResults(versionCountArangoCursor);
        try {
            versionCountArangoCursor.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return versionCount;
    }

    public ArrayList<HistogramEntry> getVersionCountsDaily(String identifier, int year, int month) {
        ArrayList<HistogramEntry> versionCount = new ArrayList<>();
        LOGGER.info("Getting version count per day for the identifier: \"" + identifier + "\" in " + year + "-" + month);
        paramMapBuilder = new MapBuilder().
                put("@nodesCollection", nodesCollectionName).
                put("nameKey", nameKey).
                put("identifier", identifier).
                put("nodeLabelAttribute", nodeLabelAttribute).
                put("versionNodeLabel", versionNodeLabel).
                put("versionKey", versionKey).
                put("year", year).
                put("month", month);

        query = "FOR node IN @@nodesCollection FILTER node.@nameKey == @identifier \n" +
                "AND node.@nodeLabelAttribute == @versionNodeLabel \n" +
                "AND TO_NUMBER(SUBSTRING(node.@versionKey,0,4)) == @year \n" +
                "AND TO_NUMBER(SUBSTRING(node.@versionKey,4,2)) == @month \n" +
                "COLLECT day = SUBSTRING(node.@versionKey,6,2) INTO versionsPerDay \n" +
                "RETURN {key: day, count: COUNT(versionsPerDay[*])}";

        ArangoCursor<Object> versionCountArangoCursor = arangoDBConnectionHandler.executeLinkDataQuery(query, paramMapBuilder);
        if (versionCountArangoCursor.hasNext())
            versionCount = getVersionCountResults(versionCountArangoCursor);
        try {
            versionCountArangoCursor.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return versionCount;
    }

    private ArrayList<HistogramEntry> getVersionCountResults(ArangoCursor<Object> versionCountArangoCursor) {
        ArrayList<HistogramEntry> versionCounts = new ArrayList<>();
        while (versionCountArangoCursor.hasNext()) {
            HashMap<String, String> versionCount = (HashMap<String, String>) versionCountArangoCursor.next();
            HistogramEntry versionCountHistogram = new HistogramEntry(Integer.valueOf(String.valueOf(versionCount.get("key"))),
                    Integer.valueOf(String.valueOf(versionCount.get("count"))));
            versionCounts.add(versionCountHistogram);
        }
        return versionCounts;
    }

    public boolean addNodesAndRelationships(Map<String, Node> graphNodes, ArrayList<Edge> graphEdges) {
        try {
            LOGGER.info("Adding nodes and edges started");
            LOGGER.info("Map: " + graphNodes.hashCode());
            ArrayList<JSONObject> nodesToImport = prepareNodesForArangoImport(graphNodes);
            if (!importAndMatchNodes(nodesToImport))
                return false;
            if (!(nodeIDs.isEmpty())) {
                ArrayList<BaseEdgeDocument> edgesToImport = prepareEdgesForArangoImport(graphEdges);
                LOGGER.info("Number of edges to import: " + edgesToImport.size());
                if (!importEdges(edgesToImport))
                    return false;
            }
        } catch (ArangoDBException exception) {
            exception.printStackTrace();
            return false;
        }
        return true;
    }

    private ArrayList<JSONObject> prepareNodesForArangoImport(Map<String, Node> graphNodes) {
        ArrayList<JSONObject> nodesToImport = new ArrayList<>();
        Iterator graphNodesIterator = graphNodes.entrySet().iterator();
        while (graphNodesIterator.hasNext()) {
            Map.Entry<String, Node> graphNodeEntry = (Map.Entry<String, Node>) graphNodesIterator.next();
            String nodeTimestamp = graphNodeEntry.getValue().getTimestamp();
            HashMap<String, String> parsedGraphNode = new HashMap<>();
            parsedGraphNode.put(requestIdAttribute, graphNodeEntry.getKey());
            parsedGraphNode.put(nameKey, graphNodeEntry.getValue().getIdentifier());
            parsedGraphNode.put(versionKey, nodeTimestamp);
            parsedGraphNode.put(nodeLabelAttribute, isNull(nodeTimestamp) ? parentNodeLabel : versionNodeLabel);
            nodesToImport.add(new JSONObject(parsedGraphNode));
        }
        return nodesToImport;
    }

    private boolean importAndMatchNodes(ArrayList<JSONObject> graphNodes) {
        LOGGER.info("Importing nodes");
        ArrayList<BaseDocument> nodesToImport = new ArrayList<>();
        Iterator<JSONObject> nodeJSONIterator = graphNodes.iterator();
        while (nodeJSONIterator.hasNext()) {
            JSONObject nodeJSON = nodeJSONIterator.next();
            BaseDocument node = new BaseDocument();
            node.addAttribute(nameKey, nodeJSON.get(nameKey));
            node.addAttribute(nodeLabelAttribute, nodeJSON.get(nodeLabelAttribute));
            if (nodeJSON.has(versionKey))
                node.addAttribute(versionKey, nodeJSON.get(versionKey));
            nodesToImport.add(node);
        }
        MultiDocumentEntity<DocumentCreateEntity<BaseDocument>> nodes;
        Long startTime = new Date().getTime();
        try {
            nodes = arangoDBConnectionHandler.insertNodes(nodesCollectionName, nodesToImport);
            LOGGER.info("Done importing in: " + Float.valueOf(new Date().getTime() - startTime) + " ms");
            return matchNodes(graphNodes, nodes.getDocumentsAndErrors());
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
//        return true;
    }

    private boolean matchNodes(ArrayList<JSONObject> graphNodes, Collection<Object> nodesDocumentsAndErrors) {
        HashMap<String, String> graphNodesMap = getGraphNodesMap(graphNodes);
        int duplicatesCount = 0;
        int newCount = 0;
        String nodeKey;
//        InputStream arangodbProperties = ArangoDBHandler.class.getResourceAsStream("arangodb.properties");
//        ArangoDB arangoDB = new ArangoDB.Builder()
//                .loadProperties(arangodbProperties)
//                .acquireHostList(true)
//                .loadBalancingStrategy(LoadBalancingStrategy.ROUND_ROBIN)
//                .build();
//        ArangoCollection nodesC = arangoDB.db("link-data").collection("nodes");

        Iterator<Object> insertionIterator = nodesDocumentsAndErrors.iterator();
        while (insertionIterator.hasNext()) {
            Object node = insertionIterator.next();
            BaseDocument existingNode;
            if (node.getClass().equals(ErrorEntity.class)) {
                nodeKey = ((ErrorEntity) node).getErrorMessage().split("conflicting key: ")[1];
                existingNode = nodesCollection.getDocument(nodeKey, BaseDocument.class);
                duplicatesCount++;
            } else {
                existingNode = (BaseDocument) ((DocumentCreateEntity) node).getNew();
                newCount++;
            }

            String nodeRequestID = isNull(existingNode.getAttribute(versionKey)) ?
                    graphNodesMap.get(existingNode.getAttribute(nameKey)) :
                    graphNodesMap.get(existingNode.getAttribute(versionKey) + "," + existingNode.getAttribute(nameKey));
            nodeIDs.put(nodeRequestID, existingNode.getId());
            LOGGER.info("=================================");
            LOGGER.info("nodesID: " + nodeIDs.hashCode() + "RequestId: " + nodeRequestID + " ArangoDBId: " + existingNode.getId() + " url and timestamp: " +
                    existingNode.getAttribute(versionKey) + "," + existingNode.getAttribute(nameKey));
        }
        LOGGER.info("Number of newly inserted nodes: " + newCount);
        LOGGER.info("Number of duplicate nodes: " + duplicatesCount);
        return updateDuplicates(duplicatesCount);
    }

    private HashMap<String, String> getGraphNodesMap(ArrayList<JSONObject> graphNodes) {
        HashMap<String, String> graphNodesMap = new HashMap<>();
        for(JSONObject graphNode : graphNodes){
            String graphNodesMapEntryKey = graphNode.has(versionKey) ?
                    (graphNode.get(versionKey) + "," + graphNode.get(nameKey)) :
                    String.valueOf(graphNode.get(nameKey));
            graphNodesMap.put(graphNodesMapEntryKey, String.valueOf(graphNode.get(requestIdAttribute)));
        }
        return graphNodesMap;
    }

    //TO-DO: Revisit Later
    private boolean updateDuplicates(int duplicatesCount) {
//        paramMapBuilder = new MapBuilder().put("duplicatesCount", duplicatesCount);
//        query = "FOR d IN duplicates\n" +
//                "UPDATE d WITH {count: d.count+@duplicatesCount} IN duplicates";
//        if(arangoDBConnectionHandler.executeLinkDataQuery(query, paramMapBuilder) == null)
//            return false;
        return true;
    }

    private ArrayList<BaseEdgeDocument> prepareEdgesForArangoImport(ArrayList<Edge> graphEdges) {
        LOGGER.info("Preparing edge records for ArangoImport");
        ArrayList<BaseEdgeDocument> edgesToInsert = new ArrayList<>();
        Iterator<Edge> edgesDocumentIterator = graphEdges.iterator();
        while (edgesDocumentIterator.hasNext()) {
            Edge requestEdge = edgesDocumentIterator.next();
            String sourceID = nodeIDs.get(requestEdge.getSource());
            String targetID = nodeIDs.get(requestEdge.getTarget());
            if (!isNull(sourceID) && !isNull(targetID)) {
                BaseEdgeDocument edge = new BaseEdgeDocument();
                edge.setFrom(sourceID);
                edge.setTo(targetID);
                edgesToInsert.add(edge);
            }
        }
        return edgesToInsert;
    }

    private boolean importEdges(ArrayList<BaseEdgeDocument> edgesToInsert) {
        try {
            arangoDBConnectionHandler.insertEdges(edgesCollectionName, edgesToInsert);
            return true;
        } catch (ArangoDBException e) {
            e.printStackTrace();
            return false;
        }
    }
}