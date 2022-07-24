package org.streamreasoning.rsp4j.operatorapi;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.jgrapht.Graph;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.streamreasoning.rsp4j.api.PredicateEdge;
import org.streamreasoning.rsp4j.api.RDFJGraphT;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.enums.ReportGrain;
import org.streamreasoning.rsp4j.api.enums.Tick;
import org.streamreasoning.rsp4j.api.querying.ContinuousQuery;
import org.streamreasoning.rsp4j.api.secret.report.Report;
import org.streamreasoning.rsp4j.api.secret.report.ReportImpl;
import org.streamreasoning.rsp4j.api.secret.report.strategies.OnWindowClose;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import org.streamreasoning.rsp4j.api.secret.time.TimeImpl;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import org.streamreasoning.rsp4j.operatorapi.utils.DummyConsumer;
import org.streamreasoning.rsp4j.yasper.examples.RDFStream;
import org.streamreasoning.rsp4j.yasper.examples.RDFStreamJGraphT;
import org.streamreasoning.rsp4j.yasper.querying.operators.r2r.*;
import org.streamreasoning.rsp4j.yasper.querying.operators.r2r.joins.NestedJoinAlgorithm;
import org.streamreasoning.rsp4j.yasper.querying.syntax.TPQueryFactory;
import org.streamreasoning.rsp4j.yasper.querying.syntax.TPQueryFactoryJGraphT;
import org.streamreasoning.rsp4j.yasper.sds.SDSImplJGraphT;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.*;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CityBenchJGraphT {

    // ENGINE DEFINITION
    private Report report;
    private Tick tick;
    private ReportGrain report_grain;

    private int scope = 0;

    public CityBenchJGraphT() {
        report = new ReportImpl();
        report.add(new OnWindowClose());
        //        report.add(new NonEmptyContent());
        //        report.add(new OnContentChange());
        //        report.add(new Periodic());

        tick = Tick.TIME_DRIVEN;
        report_grain = ReportGrain.SINGLE;
    }

    @Test
    public void cityBenchQueryOldWay() throws IOException {

        Time instance = new TimeImpl(0);

        RDFStream stream1 = new RDFStream("http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData182955");
        RDFStream stream2 = new RDFStream("http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData158505");
        URL sensorRepository = CityBenchJGraphT.class.getClassLoader().getResource("JenaSensorRepository.n3.csv");

        ContinuousQuery<org.apache.commons.rdf.api.Graph, org.apache.commons.rdf.api.Graph, Binding, Binding> query =
            TPQueryFactory.parse(
                ""+
                    "PREFIX ses: <http://www.insight-centre.org/dataset/SampleEventService#> " +
                    "PREFIX ct: <http://www.insight-centre.org/citytraffic#> " +
                    "PREFIX ssn: <http://purl.oclc.org/NET/ssnx/ssn#> " +
                    "REGISTER RSTREAM <q1> AS " +
                    "SELECT ?obId1 ?obId2 ?v1 ?v2 " +
                    "FROM NAMED WINDOW <w1> ON ses:AarhusTrafficData182955 [RANGE PT3S STEP PT1S] " +
                    "FROM NAMED WINDOW <w2> ON ses:AarhusTrafficData158505 [RANGE PT3S STEP PT1S] " +
                    "FROM <" + sensorRepository.getPath() + "> " +
                    "WHERE {" +
                    "?p1 a ct:CongestionLevel ." +
                    "?p2 a ct:CongestionLevel ." +
                    "WINDOW <w1> {" +
                    "?obId1 ssn:observedProperty ?p1 ;" +
                    "ssn:hasValue ?v1 ;" +
                    "ssn:observedBy ses:AarhusTrafficData182955 ." +
                    "}" +
                    "WINDOW <w2> {" +
                    "?obId2 ssn:observedProperty ?p2 ;" +
                    "ssn:hasValue ?v2 ;" +
                    "ssn:observedBy ses:AarhusTrafficData158505 ." +
                    "}" +
                    "}");



        //SDS
        TaskOperatorAPIImpl<org.apache.commons.rdf.api.Graph, org.apache.commons.rdf.api.Graph, Binding, Binding> t =
            new QueryTaskOperatorAPIImpl.QueryTaskBuilder()
                .fromQuery(query)
                .build();
        ContinuousProgram<org.apache.commons.rdf.api.Graph, org.apache.commons.rdf.api.Graph, Binding, Binding> cp = new ContinuousProgram.ContinuousProgramBuilder()
            .in(stream1)
            .in(stream2)
            .addTask(t)
            .addJoinAlgorithm(new NestedJoinAlgorithm())
            .out(query.getOutputStream())
            .build();

        DummyConsumer<Binding> dummyConsumer = new DummyConsumer<>();

        query.getOutputStream().addConsumer(dummyConsumer);


        CityBenchJGraphT.populateMultipleStreamsOldWay(stream1, stream2, instance);



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
    public void cityBenchQuery() throws IOException {

        Time instance = new TimeImpl(0);

        RDFStreamJGraphT stream1 = new RDFStreamJGraphT("http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData182955");
        RDFStreamJGraphT stream2 = new RDFStreamJGraphT("http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData158505");
        URL sensorRepository = CityBenchJGraphT.class.getClassLoader().getResource("JenaSensorRepository.n3.csv");

        ContinuousQuery<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> query =
            TPQueryFactoryJGraphT.parse(
                ""+
                "PREFIX ses: <http://www.insight-centre.org/dataset/SampleEventService#> " +
                "PREFIX ct: <http://www.insight-centre.org/citytraffic#> " +
                "PREFIX ssn: <http://purl.oclc.org/NET/ssnx/ssn#> " +
                "REGISTER RSTREAM <q1> AS " +
                "SELECT ?obId1 ?obId2 ?v1 ?v2 " +
                "FROM NAMED WINDOW <w1> ON ses:AarhusTrafficData182955 [RANGE PT3S STEP PT1S] " +
                "FROM NAMED WINDOW <w2> ON ses:AarhusTrafficData158505 [RANGE PT3S STEP PT1S] " +
                "FROM <" + sensorRepository.getPath() + "> " +
                "WHERE {" +
                    "?p1 a ct:CongestionLevel ." +
                    "?p2 a ct:CongestionLevel ." +
                    "WINDOW <w1> {" +
                        "?obId1 ssn:observedProperty ?p1 ;" +
                            "ssn:hasValue ?v1 ;" +
                            "ssn:observedBy ses:AarhusTrafficData182955 ." +
                        "}" +
                        "WINDOW <w2> {" +
                            "?obId2 ssn:observedProperty ?p2 ;" +
                            "ssn:hasValue ?v2 ;" +
                            "ssn:observedBy ses:AarhusTrafficData158505 ." +
                    "}" +
                "}");



        //SDS
        TaskOperatorAPIImpl<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> t =
            new QueryTaskOperatorAPIImplJGraphT.QueryTaskBuilder()
                .fromQuery(query)
                .build();
        ContinuousProgram<Graph<IRI, PredicateEdge>, Graph<IRI, PredicateEdge>, Binding, Binding> cp = new ContinuousProgram.ContinuousProgramBuilder()
            .in(stream1)
            .in(stream2)
            .addTask(t)
            .addJoinAlgorithm(new NestedJoinAlgorithm())
            .out(query.getOutputStream())
            .setSDS(new SDSImplJGraphT())
            .build();

        DummyConsumer<Binding> dummyConsumer = new DummyConsumer<>();

        query.getOutputStream().addConsumer(dummyConsumer);


        CityBenchJGraphT.populateMultipleStreams(stream1, stream2, instance);



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




    private static void populateMultipleStreams(DataStream<Graph<IRI, PredicateEdge>> stream1, DataStream<Graph<IRI, PredicateEdge>> stream2, Time timeInstance) throws IOException {

//        setup file data
        URL f1 = CityBenchJGraphT.class.getClassLoader().getResource("AarhusTrafficData158505.stream");
        URL f2 = CityBenchJGraphT.class.getClassLoader().getResource("AarhusTrafficData182955.stream");
        ArrayList<BufferedReader> readers = new ArrayList<>();
        readers.add(new BufferedReader(new FileReader(f1.getPath())));
        readers.add(new BufferedReader(new FileReader(f2.getPath())));
        HashMap<BufferedReader, String> fName = new HashMap<>();
        fName.put(readers.get(0), f1.getFile());
        fName.put(readers.get(1), f2.getFile());
        HashMap<BufferedReader, DataStream<Graph<IRI, PredicateEdge>>> streamMap = new HashMap<>();
        streamMap.put(readers.get(0),stream1);
        streamMap.put(readers.get(1),stream2);
        HashMap<BufferedReader, Integer> lineNrs = new HashMap<>();
        lineNrs.put(readers.get(0), 0);
        lineNrs.put(readers.get(1), 0);

        //set up headers
        HashMap<BufferedReader, List<String>> cols = new HashMap<>();
//        ArrayList<String> cols = new ArrayList<String>();
        readers.stream().forEach(f -> {
            //set up headers
            try {
                List<String> colsTemp = Arrays.asList(f.readLine().split(","));
                cols.put(f, colsTemp);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        //stream file
        while(!readers.isEmpty()) {
            for(BufferedReader reader: readers) {
                String line = reader.readLine();
                lineNrs.put(reader, lineNrs.get(reader) + 1);
                if(line == null) {
                    readers.remove(reader);
                } else {
                    String[] l = line.split(",");
                    List<String> headerLine = cols.get(reader);
                    ListIterator<String> it = headerLine.listIterator();

                    while (it.hasNext()){
                        int index = it.nextIndex();
                        String header = it.next();
                        IRI s = RDFUtils.createIRI(fName.get(reader)+"&lineNr="+lineNrs.get(reader));
                        IRI p = RDFUtils.createIRI(header);
                        IRI o = RDFUtils.createIRI(encode(l[index]));
                        RDFJGraphT graph = new RDFJGraphT();
                        graph.addTriplet(s,p,o);
                        streamMap.get(reader).put(graph.getGraph(), timeInstance.getAppTime());

                    }
                }
            }
        }
    }

    private static void populateMultipleStreamsOldWay(RDFStream stream1, RDFStream stream2, Time timeInstance) throws IOException {

//        setup file data
        URL f1 = CityBenchJGraphT.class.getClassLoader().getResource("AarhusTrafficData158505.stream");
        URL f2 = CityBenchJGraphT.class.getClassLoader().getResource("AarhusTrafficData182955.stream");
        ArrayList<BufferedReader> readers = new ArrayList<>();
        readers.add(new BufferedReader(new FileReader(f1.getPath())));
        readers.add(new BufferedReader(new FileReader(f2.getPath())));
        HashMap<BufferedReader, String> fName = new HashMap<>();
        fName.put(readers.get(0), f1.getFile());
        fName.put(readers.get(1), f2.getFile());
        HashMap<BufferedReader, RDFStream> streamMap = new HashMap<>();
        streamMap.put(readers.get(0),stream1);
        streamMap.put(readers.get(1),stream2);
        HashMap<BufferedReader, Integer> lineNrs = new HashMap<>();
        lineNrs.put(readers.get(0), 0);
        lineNrs.put(readers.get(1), 0);

        //set up headers
        HashMap<BufferedReader, List<String>> cols = new HashMap<>();
//        ArrayList<String> cols = new ArrayList<String>();
        readers.stream().forEach(f -> {
            //set up headers
            try {
                List<String> colsTemp = Arrays.asList(f.readLine().split(","));
                cols.put(f, colsTemp);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        //stream file
        RDF instance = RDFUtils.getInstance();
        while(!readers.isEmpty()) {
            for(BufferedReader reader: readers) {
                String line = reader.readLine();
                lineNrs.put(reader, lineNrs.get(reader) + 1);
                if(line == null) {
                    readers.remove(reader);
                } else {
                    String[] l = line.split(",");
                    List<String> headerLine = cols.get(reader);
                    ListIterator<String> it = headerLine.listIterator();

                    while (it.hasNext()){
                        int index = it.nextIndex();
                        String header = it.next();
                        IRI s = RDFUtils.createIRI(fName.get(reader)+"&lineNr="+lineNrs.get(reader));
                        IRI p = RDFUtils.createIRI(header);
                        IRI o = RDFUtils.createIRI(encode(l[index]));
                        org.apache.commons.rdf.api.Graph graph = instance.createGraph();
                        graph.add(instance.createTriple(s,p,o));
                        streamMap.get(reader).put(graph, timeInstance.getAppTime());

                    }
                }
            }
        }
    }

    public static String encode(String url)
    {
        try {
            return URLEncoder.encode( url, "UTF-8" );
        } catch (UnsupportedEncodingException e) {
            return "Issue while encoding" +e.getMessage();
        }
    }

}
