package it.polimi.yasper.core.simple;

import it.polimi.yasper.core.query.formatter.QueryResponseFormatter;
import it.polimi.yasper.core.rspql.*;
import it.polimi.yasper.core.simple.sds.SDSBuilderImpl;
import it.polimi.yasper.core.spe.report.Report;
import it.polimi.yasper.core.spe.report.ReportGrain;
import it.polimi.yasper.core.spe.report.ReportImpl;
import it.polimi.yasper.core.spe.report.strategies.OnWindowClose;
import it.polimi.yasper.core.spe.scope.Tick;
import it.polimi.yasper.core.spe.stream.StreamElement;
import it.polimi.yasper.core.spe.stream.rdf.RDFStream;
import it.polimi.yasper.core.utils.EngineConfiguration;
import it.polimi.yasper.core.utils.QueryConfiguration;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RSPEngineImpl implements RSPEngine<StreamElement> {

    private final long t0;
    private Report report;
    private RDF rdf;
    private Tick tick;
    protected EngineConfiguration rsp_config;

    protected Map<String, SDS> assignedSDS;
    protected Map<String, ContinuousQueryExecution> queryExecutions;
    protected Map<String, ContinuousQuery> registeredQueries;
    protected Map<String, List<QueryResponseFormatter>> queryObservers;
    protected Map<String, Stream> registeredStreams;

    public RSPEngineImpl(long t0, EngineConfiguration rsp_config) {
        this.rsp_config = rsp_config;
        this.assignedSDS = new HashMap<>();
        this.registeredQueries = new HashMap<>();
        this.registeredStreams = new HashMap<>();
        this.queryObservers = new HashMap<>();
        this.queryExecutions = new HashMap<>();
        this.t0 = t0;
        this.report = new ReportImpl();
        this.report.add(new OnWindowClose());
        this.rdf = new SimpleRDF();
    }

    @Override
    public RDFStream register(RDFStream s) {
        registeredStreams.put(s.getURI(), s);
        return s;
    }

    @Override
    public void unregister(RDFStream s) {
        //TODO stop all the queries that are using s
        // destroy all the window asssigners
        // remove s from registeredStreams
    }

    @Override
    public ContinuousQuery parseQuery(String input) {
        return null;
    }

    @Override
    public ContinuousQueryExecution register(ContinuousQuery q, QueryConfiguration c) {
        SDSBuilder builder = new SDSBuilderImpl(rdf, registeredStreams, rsp_config, c, report, ReportGrain.SINGLE, Tick.TIME_DRIVEN);
        q.accept(builder);
        return builder.getContinuousQueryExecution();
    }

    @Override
    public ContinuousQueryExecution register(String q, QueryConfiguration c) {
        return null;
    }

    @Override
    public void unregister(ContinuousQuery qId) {

    }

    @Override
    public void register(ContinuousQuery q, QueryResponseFormatter o) {

    }

    @Override
    public void unregister(ContinuousQuery q, QueryResponseFormatter o) {

    }

    @Override
    public void register(ContinuousQueryExecution cqe, QueryResponseFormatter o) {

    }

    @Override
    public void unregister(ContinuousQueryExecution cqe, QueryResponseFormatter o) {

    }

    @Override
    public boolean process(StreamElement var1) {
        return false;
    }
}