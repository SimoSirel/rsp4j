package org.streamreasoning.rsp4j.yasper.content;

import org.apache.commons.rdf.api.IRI;
import org.jgrapht.Graph;
import org.streamreasoning.rsp4j.api.PredicateEdge;
import org.streamreasoning.rsp4j.api.RDFJGraphT;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.secret.content.Content;
import org.streamreasoning.rsp4j.api.secret.content.ContentFactory;
import org.streamreasoning.rsp4j.api.secret.time.Time;

public class GraphContentFactoryJGraphT implements ContentFactory<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>> {

    Time time;

    public GraphContentFactoryJGraphT(Time time) {
        this.time = time;
    }


    @Override
    public Content<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>> createEmpty() {
        return new EmptyContent(RDFJGraphT.createGraph());
    }

    @Override
    public Content<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>> create() {
        return new ContentGraphJGraphT(time);
    }
}
