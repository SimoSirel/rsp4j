package org.streamreasoning.rsp4j.operatorapi;

import org.apache.commons.rdf.api.IRI;
import org.jgrapht.Graph;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.streamreasoning.rsp4j.api.PredicateEdge;
import org.streamreasoning.rsp4j.api.RDFJGraphT;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.enums.ReportGrain;
import org.streamreasoning.rsp4j.api.enums.Tick;
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
import org.streamreasoning.rsp4j.operatorapi.utils.DummyStreamJGraphT;
import org.streamreasoning.rsp4j.yasper.content.GraphContentFactoryJGraphT;
import org.streamreasoning.rsp4j.yasper.examples.RDFStreamJGraphT;
import org.streamreasoning.rsp4j.yasper.querying.operators.Rstream;
import org.streamreasoning.rsp4j.yasper.querying.operators.r2r.*;
import org.streamreasoning.rsp4j.yasper.querying.operators.r2r.joins.NestedJoinAlgorithm;
import org.streamreasoning.rsp4j.yasper.querying.operators.windowing.CSPARQLStreamToRelationOp;
import org.streamreasoning.rsp4j.yasper.querying.syntax.TPQueryFactoryJGraphT;
import org.streamreasoning.rsp4j.yasper.sds.SDSImplJGraphT;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MultipleWindowTestJGraphT {

    // ENGINE DEFINITION
    private Report report;
    private Tick tick;
    private ReportGrain report_grain;

    private int scope = 0;

    public MultipleWindowTestJGraphT() {
        report = new ReportImpl();
        report.add(new OnWindowClose());
        //        report.add(new NonEmptyContent());
        //        report.add(new OnContentChange());
        //        report.add(new Periodic());

        tick = Tick.TIME_DRIVEN;
        report_grain = ReportGrain.SINGLE;
    }

    @Test
    public void multipleWindowTestSameStream() {

        //STREAM DECLARATION
        RDFStreamJGraphT stream = new RDFStreamJGraphT("stream1");
        BindingStream outStream = new BindingStream("out");


        //WINDOW DECLARATION

        Time instance = new TimeImpl(0);

        StreamToRelationOp<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>> window1 = new CSPARQLStreamToRelationOp<>(RDFUtils.createIRI("w1"), 2000, 2000, instance, tick, report, report_grain, new GraphContentFactoryJGraphT(instance));
        StreamToRelationOp<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>> window2 = new CSPARQLStreamToRelationOp<>(RDFUtils.createIRI("w2"), 4000, 2000, instance, tick, report, report_grain, new GraphContentFactoryJGraphT(instance));


        //R2R
        VarOrTerm s = new VarImpl("green");
        VarOrTerm s2 = new VarImpl("red");
        VarOrTerm p = new TermImpl("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        VarOrTerm o = new TermImpl("http://color#Green");
        VarOrTerm o2 = new TermImpl("http://color#Red");
        R2RJGraphT tp = new R2RJGraphT(s, p, o);
        R2RJGraphT tp2 = new R2RJGraphT(s2, p, o2);

        // REGISTER FUNCTION
        AggregationFunctionRegistry.getInstance().addFunction("COUNT", new CountFunction());

        TaskOperatorAPIImpl<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> t =
            new TaskOperatorAPIImpl.TaskBuilder()
                .addS2R("stream1", window1, "w1")
                .addR2R("w1", tp)
                .addS2R("stream1", window2, "w2")
                .addR2R("w2", tp2)
                .addR2S("out", new Rstream<Binding, Binding>())
                .build();
        ContinuousProgram<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> cp = new ContinuousProgram.ContinuousProgramBuilder()
            .in(stream)
            .addTask(t)
            .addJoinAlgorithm(new NestedJoinAlgorithm())
            .out(outStream)
            .setSDS(new SDSImplJGraphT())
            .build();

        DummyConsumer<Binding> dummyConsumer = new DummyConsumer<>();
        outStream.addConsumer(dummyConsumer);


        //RUNTIME DATA
        DummyStreamJGraphT.populateStream(stream, instance.getAppTime());

        System.out.println(dummyConsumer.getReceived());

        assertEquals(2, dummyConsumer.getSize());
        List<Binding> expected = new ArrayList<>();
        Binding b1 = new BindingImpl();
        b1.add(new VarImpl("green"), RDFUtils.createIRI("S1"));
        b1.add(new VarImpl("red"), RDFUtils.createIRI("S2"));
        Binding b2 = new BindingImpl();
        b2.add(new VarImpl("green"), RDFUtils.createIRI("S4"));
        b2.add(new VarImpl("red"), RDFUtils.createIRI("S2"));

        expected.add(b1);
        expected.add(b2);
        assertEquals(expected, dummyConsumer.getReceived());
    }

    @Test
    public void multipleWindowTestMultipleStream() {

        //STREAM DECLARATION
        RDFStreamJGraphT stream1 = new RDFStreamJGraphT("stream1");
        RDFStreamJGraphT stream2 = new RDFStreamJGraphT("stream2");
        BindingStream outStream = new BindingStream("out");


        //WINDOW DECLARATION

        Time instance = new TimeImpl(0);

        StreamToRelationOp<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>> window1 = new CSPARQLStreamToRelationOp<>(RDFUtils.createIRI("w1"), 2000, 2000, instance, tick, report, report_grain, new GraphContentFactoryJGraphT(instance));
        StreamToRelationOp<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>> window2 = new CSPARQLStreamToRelationOp<>(RDFUtils.createIRI("w2"), 4000, 2000, instance, tick, report, report_grain, new GraphContentFactoryJGraphT(instance));


        //R2R
        VarOrTerm s = new VarImpl("green");
        VarOrTerm s2 = new VarImpl("red");
        VarOrTerm p = new TermImpl("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        VarOrTerm o = new TermImpl("http://color#Green");
        VarOrTerm o2 = new TermImpl("http://color#Red");
        R2RJGraphT tp = new R2RJGraphT(s, p, o);
        R2RJGraphT tp2 = new R2RJGraphT(s2, p, o2);

        // REGISTER FUNCTION
        AggregationFunctionRegistry.getInstance().addFunction("COUNT", new CountFunction());

        TaskOperatorAPIImpl<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> t =
            new TaskOperatorAPIImpl.TaskBuilder()
                .addS2R("stream1", window1, "w1")
                .addR2R("w1", tp)
                .addS2R("stream2", window2, "w2")
                .addR2R("w2", tp2)
                .addR2S("out", new Rstream<Binding, Binding>())
                .build();
        ContinuousProgram<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> cp = new ContinuousProgram.ContinuousProgramBuilder()
            .in(stream1)
            .in(stream2)
            .addTask(t)
            .addJoinAlgorithm(new NestedJoinAlgorithm())
            .out(outStream)
            .setSDS(new SDSImplJGraphT())
            .build();

        DummyConsumer<Binding> dummyConsumer = new DummyConsumer<>();
        outStream.addConsumer(dummyConsumer);


        //RUNTIME DATA
        populateMultipleStreams(stream1,stream2, instance.getAppTime());

        System.out.println(dummyConsumer.getReceived());

        assertEquals(2, dummyConsumer.getSize());
        List<Binding> expected = new ArrayList<>();
        Binding b1 = new BindingImpl();
        b1.add(new VarImpl("green"), RDFUtils.createIRI("S1"));
        b1.add(new VarImpl("red"), RDFUtils.createIRI("S2"));
        Binding b2 = new BindingImpl();
        b2.add(new VarImpl("green"), RDFUtils.createIRI("S4"));
        b2.add(new VarImpl("red"), RDFUtils.createIRI("S2"));

        expected.add(b1);
        expected.add(b2);
        assertEquals(expected, dummyConsumer.getReceived());
    }

    @Test
    public void multipleWindowTestMultipleStreamWithJoin() throws InterruptedException {

        //STREAM DECLARATION
        RDFStreamJGraphT stream1 = new RDFStreamJGraphT("stream1");
        RDFStreamJGraphT stream2 = new RDFStreamJGraphT("stream2");
        BindingStream outStream = new BindingStream("out");


        //WINDOW DECLARATION

        Time instance = new TimeImpl(0);

        StreamToRelationOp<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>> window1 = new CSPARQLStreamToRelationOp<>(RDFUtils.createIRI("w1"), 2000, 2000, instance, tick, report, report_grain, new GraphContentFactoryJGraphT(instance));
        StreamToRelationOp<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>> window2 = new CSPARQLStreamToRelationOp<>(RDFUtils.createIRI("w2"), 4000, 2000, instance, tick, report, report_grain, new GraphContentFactoryJGraphT(instance));


        //R2R
        VarOrTerm s = new VarImpl("blue");
        VarOrTerm p = new TermImpl("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        VarOrTerm o = new TermImpl("http://color#Blue");
        R2RJGraphT tp = new R2RJGraphT(s, p, o);
        R2RJGraphT tp2 = new R2RJGraphT(s, p, o);

        // REGISTER FUNCTION
        AggregationFunctionRegistry.getInstance().addFunction("COUNT", new CountFunction());

        TaskOperatorAPIImpl<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> t =
            new TaskOperatorAPIImpl.TaskBuilder()
                .addS2R("stream1", window1, "w1")
                .addR2R("w1", tp)
                .addS2R("stream2", window2, "w2")
                .addR2R("w2", tp2)
                .addR2S("out", new Rstream<Binding, Binding>())
                .build();
        ContinuousProgram<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> cp = new ContinuousProgram.ContinuousProgramBuilder()
            .in(stream1)
            .in(stream2)
            .addTask(t)
            .addJoinAlgorithm(new NestedJoinAlgorithm())
            .out(outStream)
            .setSDS(new SDSImplJGraphT())
            .build();

        DummyConsumer<Binding> dummyConsumer = new DummyConsumer<>();
        outStream.addConsumer(dummyConsumer);


        //RUNTIME DATA
        populateMultipleStreams(stream1,stream2, instance.getAppTime());

        System.out.println(dummyConsumer.getReceived());
        //Thread.sleep(5000);
        assertEquals(2, dummyConsumer.getSize());
        List<Binding> expected = new ArrayList<>();
        Binding b1 = new BindingImpl();
        b1.add(new VarImpl("blue"), RDFUtils.createIRI("S3"));
        Binding b2 = new BindingImpl();
        b2.add(new VarImpl("blue"), RDFUtils.createIRI("S5"));

        expected.add(b1);
        expected.add(b2);
        assertEquals(expected, dummyConsumer.getReceived());
    }
    @Test
    public void multipleWindowFromQueryTest() {

        Time instance = new TimeImpl(0);

        RDFStreamJGraphT stream = new RDFStreamJGraphT("http://test/stream");

    ContinuousQuery<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> query =
        TPQueryFactoryJGraphT.parse(
            ""
                + "REGISTER ISTREAM <http://out/stream> AS "
                + "SELECT * "
                + "FROM NAMED WINDOW <window1> ON <http://test/stream> [RANGE PT2S STEP PT2S] "
                + "FROM NAMED WINDOW <window2> ON <http://test/stream> [RANGE PT4S STEP PT2S] "
                + "WHERE {"
                + "  WINDOW <window1> {?green <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://color#Green>.}"
                + "  WINDOW <window2> {?red <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://color#Red>.}"
                + "}");

        //SDS
        TaskOperatorAPIImpl<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> t =
            new QueryTaskOperatorAPIImplJGraphT.QueryTaskBuilder()
                .fromQuery(query)
                .build();
        ContinuousProgram<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> cp = new ContinuousProgram.ContinuousProgramBuilder()
            .in(stream)
            .addTask(t)
            .addJoinAlgorithm(new NestedJoinAlgorithm())
            .out(query.getOutputStream())
            .setSDS(new SDSImplJGraphT())
            .build();

        DummyConsumer<Binding> dummyConsumer = new DummyConsumer<>();

        query.getOutputStream().addConsumer(dummyConsumer);


        DummyStreamJGraphT.populateStream(stream, instance.getAppTime());



        System.out.println(dummyConsumer.getReceived());

        assertEquals(2, dummyConsumer.getSize());
        List<Binding> expected = new ArrayList<>();
        Binding b1 = new BindingImpl();
        b1.add(new VarImpl("green"), RDFUtils.createIRI("S1"));
        b1.add(new VarImpl("red"), RDFUtils.createIRI("S2"));
        Binding b2 = new BindingImpl();
        b2.add(new VarImpl("green"), RDFUtils.createIRI("S4"));
        b2.add(new VarImpl("red"), RDFUtils.createIRI("S2"));

        expected.add(b1);
        expected.add(b2);
        assertEquals(expected, dummyConsumer.getReceived());
    }

    //TODO WIll fail due to parser
/*
@Test
public void multipleWindowFromQueryTestWithStaticData() {

    Time instance = new TimeImpl(0);

    RDFStreamJGraphT stream = new RDFStreamJGraphT("http://test/stream");
    URL fileURL = MultipleWindowTestJGraphT.class.getClassLoader().getResource(
            "colors.nt");
    ContinuousQuery<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> query = TPQueryFactoryJGraphT.parse("" +
            "REGISTER ISTREAM <http://out/stream> AS " +
            "SELECT * " +
            "FROM <" + fileURL.getPath() + "> "
            + "FROM NAMED WINDOW <window1> ON <http://test/stream> [RANGE PT2S STEP PT2S] "
            + "FROM NAMED WINDOW <window2> ON <http://test/stream> [RANGE PT4S STEP PT2S] "
            + "WHERE {" +
            " ?green <http://color#source> ?source. ?red <http://color#source> ?source " +
            "  WINDOW <window1> {?green <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://color#Green>.}" +
            "  WINDOW <window2> {?red <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://color#Red>.}" +
            "}");

    //SDS
    TaskOperatorAPIImpl<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> t =
        new QueryTaskOperatorAPIImplJGraphT.QueryTaskBuilder()
            .fromQuery(query)
            .build();
    ContinuousProgram<Graph, Graph, Binding, Binding> cp = new ContinuousProgram.ContinuousProgramBuilder()
        .in(stream)
        .addTask(t)
        .addJoinAlgorithm(new NestedJoinAlgorithm())
        .out(query.getOutputStream())
        .setSDS(new SDSImplJGraphT())
        .build();

    DummyConsumer<Binding> dummyConsumer = new DummyConsumer<>();

    query.getOutputStream().addConsumer(dummyConsumer);


    DummyStreamJGraphT.populateStream(stream, instance.getAppTime());



    System.out.println(dummyConsumer.getReceived());

    assertEquals(1, dummyConsumer.getSize());
    List<Binding> expected = new ArrayList<>();
    Binding b1 = new BindingImpl();
    b1.add(new VarImpl("green"), RDFUtils.createIRI("S4"));
    b1.add(new VarImpl("red"), RDFUtils.createIRI("S2"));
    b1.add(new VarImpl("source"), RDFUtils.createIRI("Source2"));


    expected.add(b1);
    assertEquals(expected, dummyConsumer.getReceived());
}

 */

    /*


@Test
public void multipleWindowFromQueryTestWithStaticDataAndFileSources() throws InterruptedException {
    URL fileURL = MultipleWindowTestJGraphT.class.getClassLoader().getResource(
            "colors.nt");
    URL streamURL = MultipleWindowTestJGraphT.class.getClassLoader().getResource(
            "stream1.nt");
    Time instance = new TimeImpl(0);

    // create parsing strategy
    JenaRDFParsingStrategy parsingStrategy = new JenaRDFParsingStrategy(RDFBase.NT);
    // create file source to read the newly created file
    FileSource<Graph> stream1 = new FileSource<Graph>(streamURL.getPath(), 1000, parsingStrategy);
    FileSource<Graph> stream2 = new FileSource<Graph>(streamURL.getPath(), 1000, parsingStrategy);

    ContinuousQuery<Graph, Graph, Binding, Binding> query = TPQueryFactory.parse("" +
            "REGISTER ISTREAM <http://out/stream> AS " +
            "SELECT * " +
            "FROM <" + fileURL.getPath() + "> "
            + "FROM NAMED WINDOW <window1> ON <"+streamURL.getPath()+"> [RANGE PT2S STEP PT2S] "
            + "FROM NAMED WINDOW <window2> ON  <"+streamURL.getPath() +">[RANGE PT4S STEP PT2S] "
            + "WHERE {" +
            " ?green <http://color#source> ?source. ?red <http://color#source> ?source " +
            "  WINDOW <window1> {?green <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://color#Green>.}" +
            "  WINDOW <window2> {?red <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://color#Red>.}" +
            "}");

    //SDS
    TaskOperatorAPIImpl<Graph, Graph, Binding, Binding> t =
            new QueryTaskOperatorAPIImpl.QueryTaskBuilder()
                    .fromQuery(query)
                    .build();
    ContinuousProgram<Graph, Graph, Binding, Binding> cp = new ContinuousProgram.ContinuousProgramBuilder()
            .in(stream1)
            .in(stream2)
            .addTask(t)
            .addJoinAlgorithm(new NestedJoinAlgorithm())
            .out(query.getOutputStream())
            .build();

    DummyConsumer<Binding> dummyConsumer = new DummyConsumer<>();

    query.getOutputStream().addConsumer(dummyConsumer);
    stream1.stream();
    stream2.stream();
    Thread.sleep(7000);



    System.out.println(dummyConsumer.getReceived());

    assertEquals(1, dummyConsumer.getSize());
    List<Binding> expected = new ArrayList<>();
    Binding b1 = new BindingImpl();
    b1.add(new VarImpl("green"), RDFUtils.createIRI("S4"));
    b1.add(new VarImpl("red"), RDFUtils.createIRI("S2"));
    b1.add(new VarImpl("source"), RDFUtils.createIRI("Source2"));


    expected.add(b1);
    assertEquals(expected, dummyConsumer.getReceived());
}
 */


    private void populateMultipleStreams(DataStream<Graph<IRI, PredicateEdge>> stream, DataStream<Graph<IRI, PredicateEdge>> stream2, long startTime) {

        //RUNTIME DATA

        RDFJGraphT graph = new RDFJGraphT();
        IRI p = RDFUtils.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        graph.addTriplet(RDFUtils.createIRI("S1"), p, RDFUtils.createIRI("http://color#Green"));
        stream.put(graph.getGraph(), 1000 + startTime);

        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI("S2"), p, RDFUtils.createIRI("http://color#Red"));
        stream2.put(graph.getGraph(), 1999 + startTime);


        //cp.eval(1999l);
        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI("S3"), p, RDFUtils.createIRI("http://color#Blue"));
        stream.put(graph.getGraph(), 2001 + startTime);
        stream2.put(graph.getGraph(), 2001 + startTime);


        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI("S4"), p, RDFUtils.createIRI("http://color#Green"));
        stream.put(graph.getGraph(), 3000 + startTime);


        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI("S5"), p, RDFUtils.createIRI("http://color#Blue"));
        stream.put(graph.getGraph(), 5000 + startTime);
        stream2.put(graph.getGraph(), 5000 + startTime);


        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI("S6"), p, RDFUtils.createIRI("http://color#Red"));
        stream2.put(graph.getGraph(), 5000 + startTime);

        graph = new RDFJGraphT();
        graph.addTriplet(RDFUtils.createIRI("S7"), p, RDFUtils.createIRI("http://color#Red"));
        stream2.put(graph.getGraph(), 6001 + startTime);
    }

}
