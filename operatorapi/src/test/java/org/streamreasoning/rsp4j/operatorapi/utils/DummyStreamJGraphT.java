package org.streamreasoning.rsp4j.operatorapi.utils;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.jgrapht.Graph;
import org.streamreasoning.rsp4j.api.PredicateEdge;
import org.streamreasoning.rsp4j.api.RDFJGraphT;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;

public class DummyStreamJGraphT {

        public static void populateStream(DataStream<Graph<IRI, PredicateEdge>> stream, long startTime) {
            populateStream(stream, startTime,"");
        }

        public static void populateStream(DataStream<Graph<IRI, PredicateEdge>> stream, long startTime, String prefix) {

        //RUNTIME DATA

        RDFJGraphT graph = new RDFJGraphT();
        IRI p = RDFUtils.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        graph.addTriplet(RDFUtils.createIRI(prefix +"S1"), p, RDFUtils.createIRI("http://color#Green"));
        stream.put(graph.getGraph(), 1000 + startTime);

        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI(prefix +"S2"), p, RDFUtils.createIRI("http://color#Red"));
        stream.put(graph.getGraph(), 1999 + startTime);


        //cp.eval(1999l);
        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI(prefix +"S3"), p, RDFUtils.createIRI("http://color#Blue"));
        stream.put(graph.getGraph(), 2001 + startTime);


        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI(prefix +"S4"), p, RDFUtils.createIRI("http://color#Green"));
        stream.put(graph.getGraph(), 3000 + startTime);


        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI(prefix +"S5"), p, RDFUtils.createIRI("http://color#Blue"));
        stream.put(graph.getGraph(), 5000 + startTime);


        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI(prefix +"S6"), p, RDFUtils.createIRI("http://color#Red"));
        stream.put(graph.getGraph(), 5000 + startTime);

        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI(prefix +"S7"), p, RDFUtils.createIRI("http://color#Red"));
        stream.put(graph.getGraph(), 6001 + startTime);
    }
    /*

    public static void populateValueStream(DataStream<Graph<IRI, PredicateEdge>> stream, long startTime){
            populateValueStream(stream,startTime,"");
    }

    public static void populateValueStream(DataStream<Graph<IRI, PredicateEdge>> stream, long startTime, String prefix) {

        //RUNTIME DATA

        RDFJGraphT graph = new RDFJGraphT();
        IRI p = RDFUtils.createIRI("http://test/hasValue");
        graph.addTriplet(RDFUtils.createIRI(prefix +"S1"), p, RDFUtils.createLiteral("20",RDFUtils.createIRI("http://www.w3.org/2001/XMLSchema#integer"))));
        stream.put(graph, 1000 + startTime);

        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI(prefix +"S2"), p, RDFUtils.createLiteral("20",RDFUtils.createIRI("http://www.w3.org/2001/XMLSchema#integer"))));
        stream.put(graph, 1999 + startTime);


        //cp.eval(1999l);
        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI(prefix +"S3"), p, RDFUtils.createLiteral("40",RDFUtils.createIRI("http://www.w3.org/2001/XMLSchema#integer"))));
        stream.put(graph, 2001 + startTime);


        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI(prefix +"S4"), p, RDFUtils.createLiteral("50",RDFUtils.createIRI("http://www.w3.org/2001/XMLSchema#integer"))));
        stream.put(graph, 3000 + startTime);


        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI(prefix +"S5"), p, RDFUtils.createLiteral("40",RDFUtils.createIRI("http://www.w3.org/2001/XMLSchema#integer"))));
        stream.put(graph, 5000 + startTime);


        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI(prefix +"S6"), p, RDFUtils.createLiteral("90",RDFUtils.createIRI("http://www.w3.org/2001/XMLSchema#integer"))));
        stream.put(graph, 5000 + startTime);

        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI(prefix +"S7"), p, RDFUtils.createLiteral("20",RDFUtils.createIRI("http://www.w3.org/2001/XMLSchema#integer"))));
        stream.put(graph, 6001 + startTime);
    }

     */

    public static Graph<IRI, PredicateEdge> createSingleColorGraph(String prefix, String color){
        RDFJGraphT graph = new RDFJGraphT();
        IRI p = RDFUtils.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        graph.addTriplet(RDFUtils.createIRI(prefix +"S1"), p, RDFUtils.createIRI(prefix+color));
        return graph.getGraph();
    }
}