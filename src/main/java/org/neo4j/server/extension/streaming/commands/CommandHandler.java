package org.neo4j.server.extension.streaming.commands;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.*;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.server.extension.streaming.cypher.json.JsonResultWriter;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.IOException;
import java.util.*;

/**
 * @author mh
 * @since 16.04.12
 */
@SuppressWarnings("unchecked")
public class CommandHandler {

    private final GraphDatabaseService gds;
    private final ReadableRelationshipIndex relAutoIndex;
    private final ReadableIndex<Node> nodeAutoIndex;
    private final IndexManager indexManager;
    private final ExecutionEngine executionEngine;

    public CommandHandler(GraphDatabaseService gds) {
        this.gds = gds;
        indexManager = gds.index();
        relAutoIndex = indexManager.getRelationshipAutoIndexer().getAutoIndex();
        nodeAutoIndex = indexManager.getNodeAutoIndexer().getAutoIndex();
        executionEngine = new ExecutionEngine(gds);
    }

    public void handle(Collection<List> commands, final JsonResultWriter writer) {
        Transaction tx = gds.beginTx();
        try {
            writer.startArray();
            try {
                Map<String, Object> context = new HashMap<String, Object>();
                for (List command : commands) {
                    handleSingleCommand(command, context, writer);
                }
                tx.success();
            } catch (Exception e) {
                // write error
                e.printStackTrace();
                tx.failure(); // really a failure?
            } finally {
                tx.finish();
                writer.endArray();
                writer.close();
            }
        } catch (IOException ioe) {
            ioe.printStackTrace(); // todo close writer??
        }
    }

    interface Command {
        void execute(Iterable<?> selection, Object data, Map context, JsonResultWriter output) throws IOException;
    }

    @SuppressWarnings("unchecked")
    class Update implements Command {
        /**
         * UPDATE_NODES
         * UPDATE_RELS
         * Updates properties and index values, null properties are removed, null index values and keys also cause removal.
         * "old" index value will be removed. Will be assigned to ref if set.
         *
         * @param selection selects nodes or relationships to be updated
         * @param input     { data : {props}, ref : "ref", index : {index: key: value: old:} | [{}]}
         */
        public void execute(Iterable<?> selection, Object input, Map context, JsonResultWriter output) throws IOException {
            if (input instanceof Map) {
                for (PropertyContainer pc : (Iterable<PropertyContainer>) selection) {
                    update((Map) input, pc, context);
                }
            }
            if (input instanceof List) {
                Iterator<PropertyContainer> it = ((Iterable<PropertyContainer>) selection).iterator();
                for (Map data : (List<Map>) input) {
                    update(data, it.next(), context);
                }
                if (it.hasNext())
                    throw new RuntimeException("Error updating elements, more elements than update-data");
            }
        }
    }

    class AddNodes implements Command {
        /**
         * ADD_NODES
         * Adds nodes, with the given properties and index values. Unique index lookup if unique entry in input.
         * Will be assigned to ref if set.
         *
         * @param input single or array of { data : {props}, ref : "ref", index : {index: key: value: old:} | [{}], unique: {index: key: value:}}
         */
        public void execute(Iterable<?> selection, Object input, Map context, JsonResultWriter output) throws IOException {
            if (input instanceof Map) output.writeNode(createNode((Map) input, context));
            if (input instanceof List) {
                for (Map data : (List<Map>) input) {
                    output.writeNode(createNode(data, context));
                }
            }
        }

        private Node createNode(final Map data, final Map context) {
            if (data.containsKey("unique")) {
                Map unique = (Map) data.get("unique");
                final Node node = new UniqueFactory.UniqueNodeFactory(gds, string(unique, "index")) {
                    protected void initialize(Node node, Map<String, Object> _) {
                        update(data, node, context);
                    }
                }.getOrCreate(string(unique, "key"), unique.get("value"));
                setRef(data, context, node);
                return node;
            }
            return update(data, gds.createNode(), context);
        }

    }

    class AddRelationships implements Command {
        /**
         * ADD_NODES
         * Adds relationships, with the given properties and index values. Unique index lookup if unique entry in input.
         * start and end will be looked up as selector and then the cross product of the relationships is created.
         * Will be assigned to ref if set.
         *
         * @param input single or array of { data : {props}, ref : "ref",type:"type", start: selector, end : selector,
         *              index : {index: key: value: old:} | [{}], unique: {index: key: value:}}
         */
        public void execute(Iterable<?> selection, Object input, Map context, JsonResultWriter output) throws IOException {
            if (input instanceof Map) createRelationships((Map) input, context, output);
            if (input instanceof List) {
                for (Map data : (List<Map>) input) {
                    createRelationships(data, context, output);
                }
            }
        }

        private void createRelationships(final Map data, Map context, JsonResultWriter output) throws IOException {
            // todo cross product
            final RelationshipType type = DynamicRelationshipType.withName(data.get("type").toString());
            for (Node start : selectNodes(data.get("start"), context)) {
                for (Node end : selectNodes(data.get("end"), context)) {
                    final Relationship relationship = getOrCreateRelationship(data, start, end, type, context);
                    // todo also return looked up unique rels?
                    output.writeRelationship(relationship);
                }
            }
        }

        // todo also update looked up unique rels?
        private Relationship getOrCreateRelationship(Map data, final Node start, final Node end, final RelationshipType type, final Map context) {
            final Relationship newRelationship = start.createRelationshipTo(end, type);
            if (data.containsKey("unique")) {
                Map unique = (Map) data.get("unique");
                final Relationship result = new UniqueFactory.UniqueRelationshipFactory(gds, string(unique, "index")) {
                    protected Relationship create(Map<String, Object> _) {
                        return newRelationship;
                    }
                }.getOrCreate(string(unique, "key"), unique.get("value"));
                if (!newRelationship.equals(result)) return result;
            }
            update(data, newRelationship, context);
            return newRelationship;
        }
    }

    private <T extends PropertyContainer> T update(Map data, T pc, Map context) {
        setProperties(data, pc);
        addToIndex(data, pc);
        setRef(data, context, pc);
        return pc;
    }

    private void addToIndex(Map data, PropertyContainer pc) {
        final Object indexInfo = data.get("index");
        if (indexInfo instanceof Map) {
            updateIndex(pc, (Map) indexInfo);
        }
        if (indexInfo instanceof List) {
            for (Map index : (List<Map>) indexInfo) {
                updateIndex(pc, index);
            }
        }
    }

    private void updateIndex(PropertyContainer pc, Map indexInfo) {
        final String indexName = string(indexInfo, "index");
        final Index<PropertyContainer> index = (Index<PropertyContainer>) (pc instanceof Node ? indexManager.forNodes(indexName) : indexManager.forRelationships(indexName));
        final Object value = indexInfo.get("value");
        final String key = string(indexInfo, "key");
        if (indexInfo.containsKey("old")) {
            index.remove(pc, key, indexInfo.get("old"));
        }
        if (value == null) {
            if (key == null) {
                index.remove(pc);
            } else {
                index.remove(pc, key);
            }
        } else {
            index.add(pc, key, value);
        }
    }

    private void setRef(Map data, Map context, Object value) {
        if (data.containsKey("ref")) {
            context.put(string(data, "ref"), value);
        }
    }

    private void setProperties(Map data, PropertyContainer pc) {
        if (data.containsKey("data")) {
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) data.get("data")).entrySet()) {
                if (entry.getValue() == null) pc.removeProperty(entry.getKey());
                else pc.setProperty(entry.getKey(), entry.getValue());
            }
        }
    }

    class DeleteNodes implements Command {
        /**
         * deletes nodes from selection
         *
         * @param selection selects nodes to be deleted
         * @param input     { force : true } to also delete relationships
         */

        public void execute(Iterable<?> selection, Object input, Map context, JsonResultWriter output) throws IOException {
            Map data = (Map) input;
            final boolean force = bool(data, "force");
            final Node refNode = gds.getReferenceNode();
            for (Node node : (Iterable<Node>) selection) {
                if (node.equals(refNode)) continue;
                if (force) {
                    for (Relationship rel : node.getRelationships()) {
                        rel.delete();
                    }
                }
                node.delete();
            }
        }
    }

    class DeleteRelationships implements Command {
        /**
         * deletes relationships from selection
         *
         * @param selection selects relationships to be deleted
         */
        public void execute(Iterable<?> selection, Object data, Map context, JsonResultWriter output) throws IOException {
            for (Relationship rel : (Iterable<Relationship>) selection) {
                rel.delete();
            }
        }
    }

    class GetNodes implements Command {
        /**
         * returns nodes from selection
         *
         * @param selection selects relationships to be deleted
         */
        public void execute(Iterable<?> selection, Object data, Map context, JsonResultWriter output) throws IOException {
            for (Node node : (Iterable<Node>) selection) {
                output.writeNode(node);
            }
        }
    }

    class CypherQuery implements Command {
        /**
         * executes cypher query and renders results
         *
         * @param input { query : "query" , [params : { params}], useContext: true} useContext -> merges current context with params,
         *              mergeResult : merges cypher result with context
         */
        public void execute(Iterable<?> selection, Object input, Map context, JsonResultWriter output) throws IOException {
            Map data = (Map) input;
            Map params = map(data, "params");
            if (bool(data, "useContext")) {
                params = new HashMap(params);
                params.putAll(context);
            }
            final long start = System.currentTimeMillis();
            final ExecutionResult result = executionEngine.execute(data.get("query").toString(), params);
            output.writeResult(result, start);
            // todo put result into context ? (avoid double execution, keep last row in wrapping result
            if (bool(data, "mergeResult")) {
                for (Map<String, Object> row : result) {
                    context.putAll(row);
                }
            }
        }
    }

    class GetRelationships implements Command {
        /**
         * returns relationships from selection
         *
         * @param selection selects relationships to be deleted
         */
        public void execute(Iterable<?> selection, Object data, Map context, JsonResultWriter output) throws IOException {
            for (Relationship relationship : (Iterable<Relationship>) selection) {
                output.writeRelationship(relationship);
            }
        }
    }

    private boolean bool(Map data, String key) {
        final Boolean value = (Boolean) data.get(key);
        return value != null && value;
    }

    private String string(Map map, String key) {
        return map.get(key).toString();
    }

    private Map map(Map data, String key) {
        if (data.containsKey(key)) return (Map) data.get(key);
        return Collections.EMPTY_MAP;
    }

    private final Map<String, Command> commands = MapUtil.<String, Command>genericMap(
            "ADD_NODES", new AddNodes(),
            "ADD_RELS", new AddRelationships(),
            "UPDATE_NODES", new Update(),
            "UPDATE_RELS", new Update(),
            "GET_NODES", new GetNodes(),
            "GET_RELS", new GetRelationships(),
            "DELETE_NODES", new DeleteNodes(),
            "DELETE_RELS", new DeleteRelationships(),
            "CYPHER", new CypherQuery()
    );

    // [opcode, selector, [{data}]]
    private void handleSingleCommand(List call, Map<String, Object> context, JsonResultWriter writer) throws IOException {
        final String opCode = call.get(0).toString().toUpperCase();
        final Command command = commands.get(opCode);
        boolean selectNodes = opCode.endsWith("NODES"); // todo ask the command
        writer.startArray();
        if (call.size() > 2) {
            Iterable<?> selection = selectNodes ? selectNodes(call.get(1), context) : selectRelationships(call.get(1), context);
            command.execute(selection, call.get(2), context, writer);
        } else {
            command.execute(Collections.EMPTY_LIST, call.get(1), context, writer);
        }
        writer.endArray();
    }

    // selector: id, "ref" [id1,id2] ["ref2","ref2"] "*", { index : "index" , key, "key", value: value | query : query}
    private Iterable<Node> selectNodes(Object selector, Map<String, Object> context) {
        if (selector instanceof Number) {
            return Collections.singleton(gds.getNodeById(((Number) selector).longValue()));
        }
        if (selector instanceof String) {
            if (selector.toString().equals("*")) return GlobalGraphOperations.at(gds).getAllNodes();
            return Collections.singleton((Node) context.get(selector.toString()));
        }
        if (selector instanceof List) {
            List list = (List) selector;
            if (list.isEmpty()) return Collections.EMPTY_LIST;
            for (int i = list.size() - 1; i >= 0; i--) {
                Object id = list.get(i);
                if (id instanceof Number)
                    list.set(i, gds.getNodeById(((Number) id).longValue()));
                if (id instanceof String) {
                    list.set(i, context.get(selector.toString()));
                }
            }
            return list;
        }
        if (selector instanceof Map) {
            Map indexInfo = (Map) selector;
            final ReadableIndex<Node> idx = indexInfo.containsKey("index") ? indexManager.forNodes(indexInfo.get("index").toString()) : nodeAutoIndex;
            if (indexInfo.containsKey("query")) return idx.query(indexInfo.get("query"));
            if (indexInfo.containsKey("key"))
                return idx.get(indexInfo.get("key").toString(), indexInfo.get("value"));
        }
        return Collections.EMPTY_LIST;
    }

    // selector: id, "ref" [id1,id2] ["ref2","ref2"] "*", { index : "index" , key, "key", value: value | query : query , [start : id, end: id]}
    private Iterable<Relationship> selectRelationships(Object selector, Map<String, Object> context) {
        if (selector instanceof Number) {
            return Collections.singleton(gds.getRelationshipById(((Number) selector).longValue()));
        }
        if (selector instanceof String) {
            if (selector.toString().equals("*")) return GlobalGraphOperations.at(gds).getAllRelationships();
            return Collections.singleton((Relationship) context.get(selector.toString()));
        }
        if (selector instanceof List) {
            List list = (List) selector;
            if (list.isEmpty()) return Collections.EMPTY_LIST;
            for (int i = list.size() - 1; i >= 0; i--) {
                Object id = list.get(i);
                if (id instanceof Number)
                    list.set(i, gds.getRelationshipById(((Number) id).longValue()));
                if (id instanceof String) {
                    list.set(i, context.get(selector.toString()));
                }
            }
            return list;
        }
        if (selector instanceof Map) {
            Map indexInfo = (Map) selector;
            final ReadableRelationshipIndex idx = indexInfo.containsKey("index") ? indexManager.forRelationships(indexInfo.get("index").toString()) : relAutoIndex;
            Node start = (indexInfo.containsKey("start")) ? nodeFromKey(indexInfo, "start") : null;
            Node end = (indexInfo.containsKey("end")) ? nodeFromKey(indexInfo, "end") : null;
            if (indexInfo.containsKey("query")) return idx.query(indexInfo.get("query"), start, end);
            if (indexInfo.containsKey("key"))
                return idx.get(indexInfo.get("key").toString(), indexInfo.get("value"), start, end);
        }
        return Collections.EMPTY_LIST;
    }

    private Node nodeFromKey(Map selector, String key) {
        return gds.getNodeById(((Number) selector.get(key)).longValue());
    }
}