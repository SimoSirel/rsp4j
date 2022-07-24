package org.streamreasoning.rsp4j.yasper.sds;

import org.apache.commons.rdf.api.IRI;
import org.apache.log4j.Logger;
import org.jgrapht.Graph;
import org.streamreasoning.rsp4j.api.PredicateEdge;
import org.streamreasoning.rsp4j.api.RDFJGraphT;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.sds.DataSet;
import org.streamreasoning.rsp4j.io.utils.RDFBase;
import org.streamreasoning.rsp4j.io.utils.parsing.ParsingStrategy;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

public class DataSetImplJGraphT implements DataSet<Graph<IRI, PredicateEdge>> {
    private final String name;
    private final String path;
    private static final Logger log = Logger.getLogger(DataSetImplJGraphT.class);
    public DataSetImplJGraphT(String name, String path) {
        this.name = name;
        this.path = path;
    }

    @Override
    public Collection<Graph<IRI, PredicateEdge>> getContent() {
        if (path != null) {
            RDFJGraphT graph = new RDFJGraphT();
            try (BufferedReader br = new BufferedReader(new FileReader(path))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] vars = line.replaceAll("[<>]","").split(" ");
                    IRI s = RDFUtils.createIRI(vars[0]);
                    IRI p = RDFUtils.createIRI(vars[1]);
                    IRI o = RDFUtils.createIRI(vars[2]);
                    graph.addTriplet(s,p,o);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return Collections.singleton(graph.getGraph());
        } else {
            log.error("No path found for loading Data Set " + name);
            return Collections.emptySet();
        }
    }

    @Override
    public String getName() {
        return name;
    }

    private static String readLineByLine(String filePath)
    {
        StringBuilder contentBuilder = new StringBuilder();
        try (Stream<String> stream = Files.lines( Paths.get(filePath), StandardCharsets.UTF_8))
        {
            stream.forEach(s -> contentBuilder.append(s).append("\n"));
        }
		catch (IOException e)
        {
            e.printStackTrace();
        }

        return contentBuilder.toString();
    }
}
