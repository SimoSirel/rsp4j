package org.streamreasoning.rsp4j.operatorapi;

import org.apache.commons.rdf.api.IRI;
import org.jgrapht.Graph;
import org.junit.Test;
import org.streamreasoning.rsp4j.api.PredicateEdge;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.RDFJGraphT;
import org.streamreasoning.rsp4j.api.enums.ReportGrain;
import org.streamreasoning.rsp4j.api.enums.Tick;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.StreamToRelationOp;
import org.streamreasoning.rsp4j.api.querying.ContinuousQuery;
import org.streamreasoning.rsp4j.api.secret.report.Report;
import org.streamreasoning.rsp4j.api.secret.report.ReportImpl;
import org.streamreasoning.rsp4j.api.secret.report.strategies.OnWindowClose;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import org.streamreasoning.rsp4j.api.secret.time.TimeImpl;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import org.streamreasoning.rsp4j.operatorapi.functions.AggregationFunctionRegistry;
import org.streamreasoning.rsp4j.operatorapi.functions.CountFunction;
import org.streamreasoning.rsp4j.operatorapi.table.BindingStream;
import org.streamreasoning.rsp4j.operatorapi.utils.DummyConsumer;
import org.streamreasoning.rsp4j.yasper.content.GraphContentFactoryJGraphT;
import org.streamreasoning.rsp4j.yasper.examples.RDFStreamJGraphT;
import org.streamreasoning.rsp4j.yasper.querying.operators.Rstream;
import org.streamreasoning.rsp4j.yasper.querying.operators.r2r.*;
import org.streamreasoning.rsp4j.yasper.querying.operators.r2r.joins.HashJoinAlgorithm;
import org.streamreasoning.rsp4j.yasper.querying.operators.windowing.CSPARQLStreamToRelationOp;
import org.streamreasoning.rsp4j.yasper.querying.syntax.TPQueryFactoryJGraphT;
import org.streamreasoning.rsp4j.yasper.sds.SDSImplJGraphT;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CPTriplePatternTestJGraphT {

    // ENGINE DEFINITION
    private Report report;
    private Tick tick;
    private ReportGrain report_grain;

    private int scope = 0;

    public CPTriplePatternTestJGraphT() {
        report = new ReportImpl();
        report.add(new OnWindowClose());
        //        report.add(new NonEmptyContent());
        //        report.add(new OnContentChange());
        //        report.add(new Periodic());

        tick = Tick.TIME_DRIVEN;
        report_grain = ReportGrain.SINGLE;
    }


    @Test
    public void simpleTPAbstractionTest() {


        //QUERY


        //STREAM DECLARATION
        RDFStreamJGraphT stream = new RDFStreamJGraphT("stream1");
        BindingStream outStream = new BindingStream("out");


        //WINDOW DECLARATION

        Time instance = new TimeImpl(0);

        StreamToRelationOp<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>> build = new CSPARQLStreamToRelationOp<>(
            RDFUtils.createIRI("w1"),
            2000,
            2000,
            instance,
            tick,
            report,
            report_grain,
            new GraphContentFactoryJGraphT(instance));


        //R2R
        ContinuousTriplePatternQuery q = new ContinuousTriplePatternQuery("q1", "stream1", "?green http://test/type <http://color#Green>");

        RelationToRelationOperator<Graph<IRI, PredicateEdge>, Binding> r2r = new R2RJGraphT(q);


        // REGISTER FUNCTION
        AggregationFunctionRegistry.getInstance().addFunction("COUNT", new CountFunction());

        TaskOperatorAPIImpl<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> t =
            new TaskOperatorAPIImpl.TaskBuilder()
                .addS2R("stream1", build, "w1")
                .addR2R("w1", r2r)
                .addR2S("out", new Rstream<Binding, Binding>())
                .build();
        ContinuousProgram<Graph, Graph, Binding, Binding> cp = new ContinuousProgram.ContinuousProgramBuilder()
            .in(stream)
            .addTask(t)
            .out(outStream)
            .setSDS(new SDSImplJGraphT())
            .build();

        DummyConsumer<Binding> dummyConsumer = new DummyConsumer<>();
        outStream.addConsumer(dummyConsumer);


        //RUNTIME DATA
        populateStream(stream, instance.getAppTime());


        assertEquals(2, dummyConsumer.getSize());
        List<Binding> expected = new ArrayList<>();
        Binding b1 = new BindingImpl();
        b1.add(new VarImpl("green"), RDFUtils.createIRI("S1"));
        Binding b2 = new BindingImpl();
        b2.add(new VarImpl("green"), RDFUtils.createIRI("S4"));
        expected.add(b1);
        expected.add(b2);
        assertEquals(expected, dummyConsumer.getReceived());
    }

    @Test
    public void simpleTPAbstractionAggregationTest() {

        //STREAM DECLARATION
        RDFStreamJGraphT stream = new RDFStreamJGraphT("stream1");
        BindingStream outStream = new BindingStream("out");


        //WINDOW DECLARATION
        Time instance = new TimeImpl(0);

        StreamToRelationOp<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>> build = new CSPARQLStreamToRelationOp<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>>(RDFUtils.createIRI("w1"), 2000, 2000, instance, tick, report, report_grain, new GraphContentFactoryJGraphT(instance));

        //R2R
        ContinuousTriplePatternQuery q = new ContinuousTriplePatternQuery("q1", "stream1", "?green http://test/type <http://color#Green>");

        RelationToRelationOperator<Graph<IRI, PredicateEdge>, Binding> r2r = new R2RJGraphT(q);


        // REGISTER FUNCTION
        AggregationFunctionRegistry.getInstance().addFunction("COUNT", new CountFunction());

        TaskOperatorAPIImpl<Graph, Graph, Binding, Binding> t =
            new TaskOperatorAPIImpl.TaskBuilder()
                .addS2R("stream1", build, "w1")
                .addR2R("w1", r2r)
                .addR2S("out", new Rstream<Binding, Binding>())
                // comment this one out so you can see it works witouth aggregation as well
                .aggregate("gw", "COUNT", "green", "count")
                .build();
        ContinuousProgram<Graph, Graph, Binding, Binding> cp = new ContinuousProgram.ContinuousProgramBuilder()
            .in(stream)
            .addTask(t)
            .out(outStream)
            .setSDS(new SDSImplJGraphT())
            .build();

        DummyConsumer<Binding> dummyConsumer = new DummyConsumer<>();
        outStream.addConsumer(dummyConsumer);

        populateStream(stream, instance.getAppTime());


        assertEquals(3, dummyConsumer.getSize());

        List<Binding> expected = new ArrayList<>();
        Binding b1 = new BindingImpl();
        b1.add(new VarImpl("count"), RDFUtils.createIRI("1"));
        Binding b2 = new BindingImpl();
        b2.add(new VarImpl("count"), RDFUtils.createIRI("1"));
        Binding b3 = new BindingImpl();
        b3.add(new VarImpl("count"), RDFUtils.createIRI("0"));
        expected.add(b1);
        expected.add(b2);
        expected.add(b3);
        assertEquals(expected, dummyConsumer.getReceived());
    }


    @Test
    public void wrongWindowAggregationTest() {

        //STREAM DECLARATION
        RDFStreamJGraphT stream = new RDFStreamJGraphT("stream1");
        BindingStream outStream = new BindingStream("out");


        //WINDOW DECLARATION
        Time instance = new TimeImpl(0);

        StreamToRelationOp<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>> build = new CSPARQLStreamToRelationOp<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>>(RDFUtils.createIRI("w1"), 2000, 2000, instance, tick, report, report_grain, new GraphContentFactoryJGraphT(instance));

        //R2R
        ContinuousTriplePatternQuery q = new ContinuousTriplePatternQuery("q1", "stream1", "?green http://test/type <http://color#Green>");

        RelationToRelationOperator<Graph<IRI, PredicateEdge>, Binding> r2r = new R2RJGraphT(q);


        TaskOperatorAPIImpl<Graph, Graph, Binding, Binding> t =
            new TaskOperatorAPIImpl.TaskBuilder()
                .addS2R("stream1", build, "w1")
                .addR2R("wrongWindow", r2r)
                .addR2S("out", new Rstream<Binding, Binding>())

                .build();
        ContinuousProgram<Graph, Graph, Binding, Binding> cp = new ContinuousProgram.ContinuousProgramBuilder()
            .in(stream)
            .addTask(t)
            .out(outStream)
            .setSDS(new SDSImplJGraphT())
            .build();

        DummyConsumer<Binding> dummyConsumer = new DummyConsumer<>();
        outStream.addConsumer(dummyConsumer);

        populateStream(stream, instance.getAppTime());


        assertEquals(0, dummyConsumer.getSize());


    }

    @Test
    public void triplePatternQueryTest() {

        Time instance = new TimeImpl(0);

        RDFStreamJGraphT stream = new RDFStreamJGraphT("http://test/stream");


        ContinuousQuery<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> query = TPQueryFactoryJGraphT.parse("" +
            "REGISTER RSTREAM <http://out/stream> AS " +
            "SELECT * " +
            "FROM NAMED WINDOW <http://test/window> ON <http://test/stream> [RANGE PT2S STEP PT2S] " +
            "WHERE {" +
            "  WINDOW <http://test/window> {?green <http://test/type> <http://color#Green> .}" +
            "}");


        //SDS
        TaskOperatorAPIImpl<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> t =
            new QueryTaskOperatorAPIImplJGraphT.QueryTaskBuilder()
                .fromQuery(query)
                .build();
        ContinuousProgram<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> cp = new ContinuousProgram.ContinuousProgramBuilder()
            .in(stream)
            .addTask(t)
            .out(query.getOutputStream())
            .setSDS(new SDSImplJGraphT())
            .build();

        DummyConsumer<Binding> dummyConsumer = new DummyConsumer<>();

        query.getOutputStream().addConsumer(dummyConsumer);


        populateStream(stream, instance.getAppTime());


        assertEquals(2, dummyConsumer.getSize());
        List<Binding> expected = new ArrayList<>();
        Binding b1 = new BindingImpl();
        b1.add(new VarImpl("green"), RDFUtils.createIRI("S1"));
        Binding b2 = new BindingImpl();
        b2.add(new VarImpl("green"), RDFUtils.createIRI("S4"));
        expected.add(b1);
        expected.add(b2);

        assertEquals(expected, dummyConsumer.getReceived());

    }

    @Test
    public void JoinTest() {

        Time instance = new TimeImpl(0);

        RDFStreamJGraphT stream = new RDFStreamJGraphT("http://test/stream");


        ContinuousQuery<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> query = TPQueryFactoryJGraphT.parse("" +
            "REGISTER RSTREAM <http://out/stream> AS " +
            "SELECT * " +
            "FROM NAMED WINDOW <http://test/window> ON <http://test/stream> [RANGE PT2S STEP PT2S] " +
            "WHERE {" +
            "  WINDOW <http://test/window> {?l <http://test/emit> ?s1. ?l <http://test/emit> ?s2. ?s2 <http://test/type> <http://color#Blue>. ?s1 <http://test/type> <http://color#Green>. }" +
            "}");


        //SDS
        TaskOperatorAPIImpl<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> t =
            new QueryTaskOperatorAPIImplJGraphT.QueryTaskBuilder()
                .fromQuery(query)
                .build();
        ContinuousProgram<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> cp = new ContinuousProgram.ContinuousProgramBuilder()
            .in(stream)
            .addTask(t)
            .out(query.getOutputStream())
            .setSDS(new SDSImplJGraphT())
            .build();

        DummyConsumer<Binding> dummyConsumer = new DummyConsumer<>();

        query.getOutputStream().addConsumer(dummyConsumer);


        populateStream(stream, instance.getAppTime());


        assertEquals(1, dummyConsumer.getSize());
        List<Binding> expected = new ArrayList<>();
        Binding b1 = new BindingImpl();
        b1.add(new VarImpl("l"), RDFUtils.createIRI("L1"));
        b1.add(new VarImpl("s1"), RDFUtils.createIRI("S1"));
        b1.add(new VarImpl("s2"), RDFUtils.createIRI("S2"));
        expected.add(b1);

        assertEquals(expected, dummyConsumer.getReceived());

    }

    @Test
    public void JoinTestMultipleWindows() {

        Time instance = new TimeImpl(0);

        RDFStreamJGraphT stream = new RDFStreamJGraphT("http://test/stream");


        ContinuousQuery<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> query = TPQueryFactoryJGraphT.parse("" +
            "REGISTER RSTREAM <http://out/stream> AS " +
            "SELECT * " +
            "FROM NAMED WINDOW <http://test/window> ON <http://test/stream> [RANGE PT2S STEP PT2S] " +
            "FROM NAMED WINDOW <http://test/window2> ON <http://test/stream> [RANGE PT2S STEP PT2S] " +
            "WHERE {" +
            "  WINDOW <http://test/window> {?l <http://test/emit> ?s1. ?l <http://test/emit> ?s2. ?s2 <http://test/type> <http://color#Blue>. ?s1 <http://test/type> <http://color#Green>. }" +
            "  WINDOW <http://test/window2> {?l22 <http://test/emit> ?s12. ?l22 <http://test/emit> ?s22. ?s22 <http://test/type> <http://color#Blue>. ?s12 <http://test/type> <http://color#Green>. }" +
            "}");


        //SDS
        TaskOperatorAPIImpl<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> t =
            new QueryTaskOperatorAPIImplJGraphT.QueryTaskBuilder()
                .fromQuery(query)
                .build();
        ContinuousProgram<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> cp = new ContinuousProgram.ContinuousProgramBuilder()
            .in(stream)
            .addTask(t)
            .out(query.getOutputStream())
            .setSDS(new SDSImplJGraphT())
            .addJoinAlgorithm(new HashJoinAlgorithm())
            .build();

        DummyConsumer<Binding> dummyConsumer = new DummyConsumer<>();

        query.getOutputStream().addConsumer(dummyConsumer);


        populateStream(stream, instance.getAppTime());


        assertEquals(1, dummyConsumer.getSize());
        List<Binding> expected = new ArrayList<>();
        Binding b1 = new BindingImpl();
        b1.add(new VarImpl("l"), RDFUtils.createIRI("L1"));
        b1.add(new VarImpl("s1"), RDFUtils.createIRI("S1"));
        b1.add(new VarImpl("s2"), RDFUtils.createIRI("S2"));
        b1.add(new VarImpl("l22"), RDFUtils.createIRI("L1"));
        b1.add(new VarImpl("s12"), RDFUtils.createIRI("S1"));
        b1.add(new VarImpl("s22"), RDFUtils.createIRI("S2"));
        expected.add(b1);

        assertEquals(expected, dummyConsumer.getReceived());

    }
    /*


    @Test
    public void wrongWindowQueryTest() {

        Time instance = new TimeImpl(0);

        RDFStream stream = new RDFStream("http://test/stream");


        ContinuousQuery<Graph, Graph, Binding, Binding> query = TPQueryFactory.parse("" +
                "REGISTER RSTREAM <http://out/stream> AS " +
                "SELECT * " +
                "FROM NAMED WINDOW <http://test/window> ON <http://test/stream> [RANGE PT2S STEP PT2S] " +
                "WHERE {" +
                "  WINDOW <http://test/wrongwindow> {?green <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://color#Green> .}" +
                "}");


        //SDS
        TaskOperatorAPIImpl<Graph, Graph, Binding, Binding> t =
                new QueryTaskOperatorAPIImpl.QueryTaskBuilder()
                        .fromQuery(query)
                        .build();
        ContinuousProgram<Graph, Graph, Binding, Binding> cp = new ContinuousProgram.ContinuousProgramBuilder()
                .in(stream)
                .addTask(t)
                .out(query.getOutputStream())
                .build();

        DummyConsumer<Binding> dummyConsumer = new DummyConsumer<>();

        query.getOutputStream().addConsumer(dummyConsumer);


        populateStream(stream, instance.getAppTime());


        assertEquals(0, dummyConsumer.getSize());


    }

    @Test
    public void triplePatternQueryNoWindowTest() {

        RDFStream stream = new RDFStream("http://test/stream");


        ContinuousQuery<Graph, Graph, Binding, Binding> query = TPQueryFactory.parse("" +
                                                                                     "REGISTER RSTREAM <http://out/stream> AS " +
                                                                                     "SELECT * " +
                                                                                     "WHERE {" +
                                                                                     "   ?green <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://color#Green> ." +
                                                                                     "}");


        //SDS
        SDS<Graph> sds = new SDSImpl();
        // Add S2R
        Time instance = new TimeImpl(0);

        StreamToRelationOp<Graph, Graph> r2r = new CSPARQLStreamToRelationOp<Graph, Graph>(RDFUtils.createIRI("w1"), 2000, 2000, instance, tick, report, report_grain, new GraphContentFactory(instance));
        TimeVarying<Graph> tvg = r2r.apply(stream);
        sds.add(tvg);


        TaskOperatorAPIImpl<Graph, Graph, Binding, Binding> t =
                new QueryTaskOperatorAPIImpl.QueryTaskBuilder()
                        .fromQuery(query)
                        .build();
        ContinuousProgram<Graph, Graph, Binding, Binding> cp = new ContinuousProgram.ContinuousProgramBuilder()
                .in(stream)
                .addTask(t)
                .setSDS(sds)
                .out(query.getOutputStream())
                .build();

        r2r.link(cp); //TODO make sure the R2R is linked in the CP

        DummyConsumer<Binding> dummyConsumer = new DummyConsumer<>();

        query.getOutputStream().addConsumer(dummyConsumer);

        populateStream(stream, instance.getAppTime());


        assertEquals(2, dummyConsumer.getSize());
        List<Binding> expected = new ArrayList<>();
        Binding b1 = new BindingImpl();
        b1.add(new VarImpl("green"), RDFUtils.createIRI("S1"));
        Binding b2 = new BindingImpl();
        b2.add(new VarImpl("green"), RDFUtils.createIRI("S4"));
        expected.add(b1);
        expected.add(b2);

        assertEquals(expected, dummyConsumer.getReceived());

    }

    @Test
    public void triplePatternQueryNoWindowNoRegisterTest() {

        RDFStream stream = new RDFStream("http://test/stream");
        BindingStream outStream = new BindingStream("out");


        ContinuousQuery<Graph, Graph, Binding, Binding> query = TPQueryFactory.parse("" +
                                                                                     "SELECT * " +
                                                                                     "WHERE {" +
                                                                                     "   ?green <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://color#Green> ." +
                                                                                     "}");


        //SDS
        SDS<Graph> sds = new SDSImpl();
        // Add S2R
        Time instance = new TimeImpl(0);

        StreamToRelationOp<Graph, Graph> r2r = new CSPARQLStreamToRelationOp<Graph, Graph>(RDFUtils.createIRI("w1"), 2000, 2000, instance, tick, report, report_grain, new GraphContentFactory(instance));
        TimeVarying<Graph> tvg = r2r.apply(stream);
        sds.add(tvg);


        TaskOperatorAPIImpl<Graph, Graph, Binding, Binding> t =
                new QueryTaskOperatorAPIImpl.QueryTaskBuilder()
                        .fromQuery(query)
                        .addR2S("", Rstream.get())
                        .build();
        ContinuousProgram<Graph, Graph, Binding, Binding> cp = new ContinuousProgram.ContinuousProgramBuilder()
                .in(stream)
                .addTask(t)
                .setSDS(sds)
                .out(outStream)
                .build();

        r2r.link(cp); //TODO make sure the r2r is linked in the CP

        DummyConsumer<Binding> dummyConsumer = new DummyConsumer<>();

        outStream.addConsumer(dummyConsumer);

        populateStream(stream, instance.getAppTime());


        assertEquals(2, dummyConsumer.getSize());
        List<Binding> expected = new ArrayList<>();
        Binding b1 = new BindingImpl();
        b1.add(new VarImpl("green"), RDFUtils.createIRI("S1"));
        Binding b2 = new BindingImpl();
        b2.add(new VarImpl("green"), RDFUtils.createIRI("S4"));
        expected.add(b1);
        expected.add(b2);

        assertEquals(expected, dummyConsumer.getReceived());

    }

    @Test
    public void triplePatternAggregationQueryTest() {

        RDFStream stream = new RDFStream("http://test/stream");


        ContinuousQuery<Graph, Graph, Binding, Binding> query = TPQueryFactory.parse("" +
                                                                                     "REGISTER RSTREAM <http://out/stream> AS " +
                                                                                     "SELECT (Count(?green) AS ?count) " +
                                                                                     "FROM NAMED WINDOW <http://test/window> ON <http://test/stream> [RANGE PT2S STEP PT2S] " +
                                                                                     "WHERE {" +
                                                                                     "   WINDOW <http://test/window> {?green <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://color#Green> .}" +
                                                                                     "}");


        //SDS
        TaskOperatorAPIImpl<Graph, Graph, Binding, Binding> t =
                new QueryTaskOperatorAPIImpl.QueryTaskBuilder()
                        .fromQuery(query)
                        .build();
        ContinuousProgram<Graph, Graph, Binding, Binding> cp = new ContinuousProgram.ContinuousProgramBuilder()
                .in(stream)
                .addTask(t)
                .out(query.getOutputStream())
                .build();

        DummyConsumer<Binding> dummyConsumer = new DummyConsumer<>();

        query.getOutputStream().addConsumer(dummyConsumer);

        Time instance = new TimeImpl(0);

        populateStream(stream, instance.getAppTime());


        List<Binding> expected = new ArrayList<>();
        Binding b1 = new BindingImpl();
        b1.add(new VarImpl("count"), RDFUtils.createIRI("1"));
        Binding b2 = new BindingImpl();
        b2.add(new VarImpl("count"), RDFUtils.createIRI("1"));
        Binding b3 = new BindingImpl();
        b3.add(new VarImpl("count"), RDFUtils.createIRI("0"));
        expected.add(b1);
        expected.add(b2);
        expected.add(b3);
        assertEquals(expected, dummyConsumer.getReceived());

    }

*/
    private void populateStream(DataStream<Graph<IRI, PredicateEdge>> stream, long startTime) {

        //RUNTIME DATA

        RDFJGraphT graph = new RDFJGraphT();
        IRI p = RDFUtils.createIRI("http://test/type");
        graph.addTriplet(RDFUtils.createIRI("S1"), p, RDFUtils.createIRI("http://color#Green"));
        stream.put(graph.getGraph(), 1000 + startTime);

        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI("S2"), p, RDFUtils.createIRI("http://color#Blue"));
        stream.put(graph.getGraph(), 1000 + startTime);




        //cp.eval(1999l);
        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI("L1"), RDFUtils.createIRI("http://test/emit"), RDFUtils.createIRI("S1"));
        stream.put(graph.getGraph(), 1000 + startTime);


        graph = new RDFJGraphT();

        graph.addTriplet(RDFUtils.createIRI("L1"), RDFUtils.createIRI("http://test/emit"), RDFUtils.createIRI("S2"));
        stream.put(graph.getGraph(), 1000 + startTime);


        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI("L2"), RDFUtils.createIRI("http://test/emit"), RDFUtils.createIRI("S4"));
        stream.put(graph.getGraph(), 1000 + startTime);

        //cp.eval(1999l);
        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI("S3"), p, RDFUtils.createIRI("http://color#Red"));
        stream.put(graph.getGraph(), 2001 + startTime);


        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI("S4"), p, RDFUtils.createIRI("http://color#Green"));
        stream.put(graph.getGraph(), 3000 + startTime);


        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI("S5"), p, RDFUtils.createIRI("http://color#Blue"));
        stream.put(graph.getGraph(), 5000 + startTime);



        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI("S6"), p, RDFUtils.createIRI("http://color#Red"));
        stream.put(graph.getGraph(), 5000 + startTime);

        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI("S7"), p, RDFUtils.createIRI("http://color#Red"));
        stream.put(graph.getGraph(), 6001 + startTime);
    }
}
