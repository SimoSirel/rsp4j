package org.streamreasoning.rsp4j.yasper.querying.operators.r2r;

import org.apache.commons.rdf.api.IRI;

import org.apache.jena.sparql.function.library.leviathan.log;
import org.apache.log4j.Logger;
import org.jgrapht.Graph;
import org.jgrapht.GraphMapping;
import org.jgrapht.Graphs;
import org.streamreasoning.rsp4j.api.PredicateEdge;
import org.streamreasoning.rsp4j.api.RDFJGraphT;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.querying.result.SolutionMapping;
import org.streamreasoning.rsp4j.api.querying.result.SolutionMappingBase;
import org.streamreasoning.rsp4j.api.sds.SDS;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;
import org.streamreasoning.rsp4j.yasper.querying.operators.r2r.JGraphTMulti.VF2MultiSubgraphIsomorphismInspector;
import org.streamreasoning.rsp4j.yasper.querying.operators.windowing.CSPARQLStreamToRelationOp;

import java.util.*;
import java.util.stream.Stream;

public class R2RJGraphT implements RelationToRelationOperator<Graph<IRI, PredicateEdge>, Binding> {

    private static final Logger log = Logger.getLogger(CSPARQLStreamToRelationOp.class);

    private Graph<IRI, PredicateEdge> queryGraph;

    public R2RJGraphT(ContinuousTriplePatternQuery queryStringTriple) {

        String[] queryLst = queryStringTriple.getTriplePattern().replaceAll("[<>]", "").split(" ");
        if (queryLst.length != 3) {throw new RuntimeException("Illegal query pattern");}

        RDFJGraphT queryGraphTemp = new RDFJGraphT();
        IRI s = RDFJGraphT.createIRI(queryLst[0]);
        IRI p = RDFJGraphT.createIRI(queryLst[1]);
        IRI o = RDFJGraphT.createIRI(queryLst[2]);
        queryGraphTemp.addTriplet(s, p, o);
        this.queryGraph = queryGraphTemp.getGraph();

    }
    public R2RJGraphT(Graph<IRI, PredicateEdge> queryGraph) {
        this.queryGraph = queryGraph;
    }

    public R2RJGraphT(VarOrTerm s, VarOrTerm p, VarOrTerm o) {
        RDFJGraphT queryGraphTemp = new RDFJGraphT();
        queryGraphTemp.addTriplet(
            RDFJGraphT.createIRI(s.getIRIString()),
            RDFJGraphT.createIRI(p.getIRIString()),
            RDFJGraphT.createIRI(o.getIRIString()));
        this.queryGraph = queryGraphTemp.getGraph();
    }

    @Override
    public Stream<Binding> eval(Stream<Graph<IRI, PredicateEdge>> sds) {

        Graph<IRI, PredicateEdge> materializedGraph = RDFJGraphT.createGraph();
        sds.forEach(graph -> {Graphs.addGraph(materializedGraph, graph);});
        return evaluateQuery(materializedGraph, System.currentTimeMillis());
    }

    @Override
    public TimeVarying<Collection<Binding>> apply(SDS<Graph<IRI, PredicateEdge>> sds) {
        return null;
    }

    @Override
    public SolutionMapping<Binding> createSolutionMapping(Binding result) {
        return new SolutionMappingBase<>(result, System.currentTimeMillis());
        //return new TableResponse(query.getID() + "/ans/" + System.currentTimeMillis(), System.currentTimeMillis(),result);
    }

    public TimeVarying<Collection<Binding>> apply() {
        //TODO
        return null;
    }

    private Stream<Binding> evaluateQuery(Graph<IRI, PredicateEdge> graph, long ts) {



        Comparator<IRI> URIComparator = (o1, o2) -> o1.equals(o2) ? 0 : 1;
        Comparator<PredicateEdge> PredicateEdgeComparator = (o1, o2) -> o1.getPredicate().toString().equals(o2.getPredicate().toString()) ? 0 : 1;
        //TODO MIGHT NEED IMPROVED LOGIC
        Comparator<IRI> nodeComparator = (o1, o2) -> {
            if (o2.toString().startsWith("<?")) {
                return 0;
            } else return URIComparator.compare(o1, o2);
        };
        //TODO MIGHT NEED IMPROVED LOGIC
        Comparator<PredicateEdge> edgeComparator = (o1, o2) -> {
            if (o2.getLabel().startsWith("<?")) {
                return 0;
            } else return PredicateEdgeComparator.compare(o1, o2);
        };
        VF2MultiSubgraphIsomorphismInspector<IRI, PredicateEdge> inspector = new VF2MultiSubgraphIsomorphismInspector<>(graph, queryGraph, nodeComparator, edgeComparator);
        Iterator<GraphMapping<IRI, PredicateEdge>> mappings = inspector.getMappings();

        List<Binding> bindings = new ArrayList<>();//TODO list vs set???
        List<IRI> vars = getVariableFromGraph(queryGraph);
        long testCounter = 0;
        while(mappings.hasNext()) {
            testCounter += 1;
            GraphMapping<IRI, PredicateEdge> mapping = mappings.next();
            Binding binding = new BindingImpl();
            for (IRI var : vars) {
                IRI targetIRI = mapping.getVertexCorrespondence(var, false);
                binding.add(new VarImpl(var.getIRIString()), RDFUtils.createIRI(RDFUtils.trimTags(targetIRI.getIRIString())));
            }
            bindings.add(binding);


        }
//        log.debug("All bindings: "+bindings);
//        System.out.println(bindings);
        return bindings.stream();
    }

    private List<IRI> getVariableFromGraph(Graph<IRI, PredicateEdge> g) {
        List<IRI> graphVariables = new ArrayList<>();
        Iterator<IRI> iter = g.vertexSet().iterator();
        while (iter.hasNext()) {
            IRI node = iter.next();
            //TODO MIGHT NEED IMPROVED LOGIC
            if (node.toString().startsWith("<?")) {
                graphVariables.add(node);
            }
        }
        return graphVariables;
    }
}
