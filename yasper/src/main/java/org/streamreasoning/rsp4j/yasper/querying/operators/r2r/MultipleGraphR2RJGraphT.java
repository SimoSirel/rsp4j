package org.streamreasoning.rsp4j.yasper.querying.operators.r2r;


import org.apache.commons.rdf.api.IRI;
import org.jgrapht.Graph;
import org.streamreasoning.rsp4j.api.PredicateEdge;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.querying.result.SolutionMapping;
import org.streamreasoning.rsp4j.api.sds.SDS;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class MultipleGraphR2RJGraphT implements RelationToRelationOperator<Graph<IRI, PredicateEdge>, Binding> {

    private final Map<String, RelationToRelationOperator<Graph<IRI, PredicateEdge>, Binding>> r2rs;

    public MultipleGraphR2RJGraphT(Map<String,RelationToRelationOperator<Graph<IRI, PredicateEdge>, Binding>> r2rs){
        this.r2rs = r2rs;
    }
    @Override
    public Stream<Binding> eval(Stream<Graph<IRI, PredicateEdge>> sds) {
        return r2rs.values().stream().findFirst().get().eval(sds);
    }

    @Override
    public TimeVarying<Collection<Binding>> apply(SDS<Graph<IRI, PredicateEdge>> sds) {
        return null;
    }

    @Override
    public SolutionMapping<Binding> createSolutionMapping(Binding result) {
        return null;
    }
    @Override
    public Map<String, RelationToRelationOperator<Graph<IRI, PredicateEdge>, Binding>> getR2RComponents(){
        return r2rs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MultipleGraphR2RJGraphT that = (MultipleGraphR2RJGraphT) o;
        return Objects.equals(r2rs, that.r2rs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(r2rs);
    }
}
