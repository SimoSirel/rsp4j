package org.streamreasoning.rsp4j.yasper.examples;

import org.apache.commons.rdf.api.IRI;
import org.jgrapht.Graph;
import org.streamreasoning.rsp4j.api.PredicateEdge;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.Consumer;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;

import java.util.ArrayList;
import java.util.List;

public class RDFStreamJGraphT implements DataStream<Graph<IRI, PredicateEdge>> {

    protected String stream_uri;
    protected List<Consumer<Graph<IRI, PredicateEdge>>> consumers = new ArrayList<>();

    public RDFStreamJGraphT(String stream_uri) {
        this.stream_uri = stream_uri;
    }

    @Override
    public void addConsumer(Consumer<Graph<IRI, PredicateEdge>> c) {
        consumers.add(c);
    }

    @Override
    public void put(Graph<IRI, PredicateEdge> e, long ts) {

        consumers.forEach(graphConsumer -> graphConsumer.notify(e, ts));
    }

    @Override
    public String getName() {
        return stream_uri;
    }

    public String uri() {
        return stream_uri;
    }

}
