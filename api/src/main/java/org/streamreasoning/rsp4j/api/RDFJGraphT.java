package org.streamreasoning.rsp4j.api;

import org.apache.commons.rdf.api.*;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.jgrapht.Graph;
import org.jgrapht.graph.DirectedPseudograph;


public class RDFJGraphT {

    private Graph<IRI, PredicateEdge> graph;

    public RDFJGraphT() {

        this.graph = createGraph();
    }

    public static RDF getInstance() {
        RDF rdf = new SimpleRDF();
        return rdf;
    }

    public static Graph<IRI, PredicateEdge> createGraph() {
        return new DirectedPseudograph<>(PredicateEdge.class);
    }

    public static Graph<IRI, PredicateEdge> addTriplet(Graph<IRI, PredicateEdge> graph, IRI subject, IRI predicate, IRI object) {
        if (! graph.containsVertex(subject)) {
            graph.addVertex(subject);
        }
        if (! graph.containsVertex(object)) {
            graph.addVertex(object);
        }
        graph.addEdge(subject,object, new PredicateEdge(predicate));

        return graph;
    }

    public void addTriplet(IRI subject, IRI predicate, IRI object) {
        if (! this.graph.containsVertex(subject)) {
            this.graph.addVertex(subject);
        }
        if (! this.graph.containsVertex(object)) {
            this.graph.addVertex(object);
        }
        this.graph.addEdge(subject,object, new PredicateEdge(predicate));
    }

    public Graph<IRI, PredicateEdge> getGraph() {
        return this.graph;
    }

    public static IRI createIRI(String w1) {
        return getInstance().createIRI(w1);
    }
}
