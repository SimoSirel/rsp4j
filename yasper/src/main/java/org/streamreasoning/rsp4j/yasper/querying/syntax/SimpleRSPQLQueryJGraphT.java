package org.streamreasoning.rsp4j.yasper.querying.syntax;


import org.apache.commons.rdf.api.IRI;
import org.jgrapht.Graph;
import org.streamreasoning.rsp4j.api.PredicateEdge;
import org.streamreasoning.rsp4j.api.enums.StreamOperator;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.operators.r2r.Var;
import org.streamreasoning.rsp4j.api.operators.r2r.utils.R2RPipe;
import org.streamreasoning.rsp4j.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.StreamToRelationOp;
import org.streamreasoning.rsp4j.api.operators.s2r.syntax.WindowNode;
import org.streamreasoning.rsp4j.api.querying.Aggregation;
import org.streamreasoning.rsp4j.api.sds.DataSet;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import org.streamreasoning.rsp4j.io.DataStreamImpl;
import org.streamreasoning.rsp4j.io.utils.RDFBase;
import org.streamreasoning.rsp4j.yasper.querying.operators.Dstream;
import org.streamreasoning.rsp4j.yasper.querying.operators.Istream;
import org.streamreasoning.rsp4j.yasper.querying.operators.Rstream;
import org.streamreasoning.rsp4j.yasper.querying.operators.r2r.*;
import org.streamreasoning.rsp4j.yasper.sds.DataSetImplJGraphT;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SimpleRSPQLQueryJGraphT<O> implements RSPQLJGraphT<O> {

    private  DataSetImplJGraphT defaultGraph;
    private RelationToStreamOperator<Binding, O> r2s;
    private String id;

    private DataStream<O> outputStream;
    private Map<String, Graph<IRI, PredicateEdge>> windowsToGraphs;

    private Map<WindowNode, DataStream<Graph<IRI, PredicateEdge>>> windowMap = new HashMap<>();
    private List<String> graphURIs = new ArrayList<>();
    private List<String> namedwindowsURIs = new ArrayList<>();
    private List<String> namedGraphURIs = new ArrayList<>();
    private List<Aggregation> aggregations = new ArrayList<>();
    private StreamOperator streamOperator = StreamOperator.NONE;
    private Time time;
    private List<Var> projections;
    private Map<String, List<Predicate<Binding>>> windowsToFilters;

    public SimpleRSPQLQueryJGraphT(String id, DataStream<Graph<IRI, PredicateEdge>> stream, Time time, WindowNode win, Map<String,Graph<IRI, PredicateEdge>> windowToGraphs, RelationToStreamOperator<Binding, O> r2s) {
        this.id = id;
        this.outputStream = new DataStreamImpl<O>(id);
        this.windowsToGraphs = windowToGraphs;

        if (win != null && stream != null) {
            windowMap.put(win, stream);
        }
        this.r2s = r2s;
        this.time = time;
        this.projections = new ArrayList<>();

    }
    public SimpleRSPQLQueryJGraphT(String id, DataStream<Graph<IRI, PredicateEdge>> stream, Time time, WindowNode win, Map<String,Graph<IRI, PredicateEdge>> windowToGraphs, RelationToStreamOperator<Binding, O> r2s, String defaultGraphIRI) {
        this(id,stream,time,win,windowToGraphs,r2s);
        //load default graph
        this.defaultGraph = new DataSetImplJGraphT("default", defaultGraphIRI);

    }
    public SimpleRSPQLQueryJGraphT(String id) {
        this.id = id;
    }

    public void addFiltersIfDefined(Map<String,List<Predicate<Binding>>> windowsToFilters){
        this.windowsToFilters = windowsToFilters;
    }
    @Override
    public void addNamedWindow(String streamUri, WindowNode wo) {
        windowMap.put(wo, new DataStreamImpl<>(streamUri));
    }

    @Override
    public void setIstream() {
        streamOperator = StreamOperator.ISTREAM;
    }

    @Override
    public void setRstream() {
        streamOperator = StreamOperator.RSTREAM;
    }

    @Override
    public void setDstream() {
        streamOperator = StreamOperator.DSTREAM;
    }

    @Override
    public boolean isIstream() {
        return streamOperator.equals(StreamOperator.ISTREAM);
    }

    @Override
    public boolean isRstream() {
        return streamOperator.equals(StreamOperator.RSTREAM);
    }

    @Override
    public boolean isDstream() {
        return streamOperator.equals(StreamOperator.DSTREAM);
    }

    @Override
    public void setSelect() {

    }

    @Override
    public void setConstruct() {

    }

    @Override
    public boolean isSelectType() {
        return false;
    }

    @Override
    public boolean isConstructType() {
        return false;
    }

    @Override
    public DataStream<O> getOutputStream() {
        return outputStream;
    }

    @Override
    public void setOutputStream(String uri) {
        this.outputStream = new DataStreamImpl<>(uri);
    }

    @Override
    public String getID() {
        return id;
    }


    @Override
    public Map<WindowNode, DataStream<Graph<IRI, PredicateEdge>>> getWindowMap() {
        return windowMap;
    }


    @Override
    public Time getTime() {
        return this.time;
    }

    @Override
    public RelationToRelationOperator<Graph<IRI, PredicateEdge>, Binding> r2r() {
            Map<String, RelationToRelationOperator<Graph<IRI, PredicateEdge>, Binding>> r2rs = new LinkedHashMap<>();
            for(Map.Entry<String,Graph<IRI, PredicateEdge>> entry: windowsToGraphs.entrySet()){
              RelationToRelationOperator<Graph<IRI, PredicateEdge>, Binding> bgp = new R2RJGraphT(entry.getValue());
              RelationToRelationOperator<Graph<IRI, PredicateEdge>, Binding> filteredBgp = addFiltersIfDefined(entry.getKey(),bgp);
              r2rs.put(entry.getKey(), filteredBgp);
            }

            return new MultipleGraphR2RJGraphT(r2rs);

    }
    private RelationToRelationOperator<Graph<IRI, PredicateEdge>, Binding> createFilter(String graph){
        return addFiltersIfDefined(graph, null);
    }
    private RelationToRelationOperator<Graph<IRI, PredicateEdge>, Binding> addFiltersIfDefined(String graph, RelationToRelationOperator<Graph<IRI, PredicateEdge>, Binding> bgp){
        if(windowsToFilters.containsKey(graph)){
            List<RelationToRelationOperator> r2rList = windowsToFilters.get(graph).stream().map(p->new Filter(Stream.empty(),p)).collect(Collectors.toList());
            if (bgp != null) {
                r2rList.add(0, bgp); // add the bgp pattern as first
            }
            R2RPipe<Graph<IRI, PredicateEdge>,Binding> pipe = new R2RPipe(r2rList.toArray(new RelationToRelationOperator[0]));
            return pipe;
        }else{
            return bgp;
        }
    }

    @Override
    public StreamToRelationOp<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>>[] s2r() {
        return new StreamToRelationOp[0];
    }


    @Override
    public RelationToStreamOperator<Binding, O> r2s() {
        switch (streamOperator){
            case RSTREAM:
                return new Rstream();
            case ISTREAM:
                return new Istream(0);
            case DSTREAM:
                return new Dstream(0);
        }
        return  new Rstream<>();
    }

    @Override
    public List<Aggregation> getAggregations() {
        return aggregations;
    }

    @Override
    public DataSet<Graph<IRI, PredicateEdge>> getDefaultGraph(){
        return defaultGraph;
    }
    public List<Var> getProjections(){
        return projections;
    }
}
