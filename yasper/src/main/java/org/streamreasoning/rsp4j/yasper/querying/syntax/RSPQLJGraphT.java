package org.streamreasoning.rsp4j.yasper.querying.syntax;

import org.apache.commons.rdf.api.IRI;
import org.jgrapht.Graph;
import org.streamreasoning.rsp4j.api.PredicateEdge;
import org.streamreasoning.rsp4j.api.querying.ContinuousQuery;
import org.streamreasoning.rsp4j.yasper.querying.operators.r2r.Binding;

public interface RSPQLJGraphT<O> extends ContinuousQuery<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, O> {
}
