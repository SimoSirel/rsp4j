package org.streamreasoning.rsp4j.api;

import org.apache.commons.rdf.api.IRI;
import org.jgrapht.graph.DefaultEdge;

public class PredicateEdge extends DefaultEdge {
    private final IRI predicate;

    public PredicateEdge(IRI predicate) {
        this.predicate = predicate;
    }

    public IRI getPredicate() {
        return predicate;
    }
    public IRI getSubject() {
        return (IRI)getSource();
    }
    public IRI getObject() {
        return (IRI)getTarget();
    }

    @Override
    public String toString() {
        return "(" + getSource() + " : " + predicate + " : " + getTarget() + ")";
        //return "(" + label + ")";
    }

    public String getLabel() {
        return predicate.getIRIString();
    }
}