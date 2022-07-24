package org.streamreasoning.rsp4j.yasper.content;

import org.apache.commons.rdf.api.IRI;
import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.streamreasoning.rsp4j.api.PredicateEdge;
import org.streamreasoning.rsp4j.api.RDFJGraphT;
import org.streamreasoning.rsp4j.api.secret.content.Content;
import org.streamreasoning.rsp4j.api.secret.time.Time;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class ContentGraphJGraphT implements Content<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>> {
    Time instance;
    private Set<Graph<IRI, PredicateEdge>> elements;
    private long last_timestamp_changed;

    public ContentGraphJGraphT(Time instance) {
        this.instance = instance;
        this.elements = new HashSet<>();
    }

    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public void add(Graph<IRI, PredicateEdge> e) {
        elements.add(e);

        this.last_timestamp_changed = instance.getAppTime();
    }

    @Override
    public Long getTimeStampLastUpdate() {
        return last_timestamp_changed;
    }


    @Override
    public String toString() {
        return elements.toString();
    }


    @Override
    public Graph<IRI, PredicateEdge> coalesce() {
        if (elements.size() == 1)
            return elements.stream().findFirst().orElseGet(RDFJGraphT::createGraph);
        else {
            Graph<IRI, PredicateEdge> g = RDFJGraphT.createGraph();
            for (Graph<IRI, PredicateEdge> element : elements){
                Graphs.addGraph(g, element);
            }
            return g;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ContentGraphJGraphT that = (ContentGraphJGraphT) o;
        return last_timestamp_changed == that.last_timestamp_changed &&
               Objects.equals(elements, that.elements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(elements, last_timestamp_changed);
    }
}
