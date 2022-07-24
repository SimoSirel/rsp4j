/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.streamreasoning.rsp4j.yasper.sds;

import org.apache.commons.rdf.api.*;
import org.apache.commons.rdf.simple.DatasetGraphView;
import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.streamreasoning.rsp4j.api.PredicateEdge;
import org.streamreasoning.rsp4j.api.RDFJGraphT;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.sds.SDS;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A simple, memory-based implementation of Dataset.
 */
final public class SDSImplJGraphT implements SDS<Graph<IRI, PredicateEdge>> {

    private boolean materialized = false;

    private final IRI def; //Unnamed default graph IRI
    //private final TimeVarying<Graph<IRI, PredicateEdge>> defE; //Unnamed default graph
    //unnamed graphs???
    private final Set<TimeVarying<Graph<IRI, PredicateEdge>>> defs = new HashSet<>();
    //named graphs???
    private final Map<IRI, TimeVarying<Graph<IRI, PredicateEdge>>> tvgs = new HashMap<>();

    private final Map<IRI, Graph<IRI, PredicateEdge>> resultGraphs = new HashMap<>();

    public SDSImplJGraphT() {
        this.def = RDFJGraphT.createIRI("def");
        //this.defE = new TimeVaryingObject<>()

    }

    public void add(final BlankNodeOrIRI graphName, final Graph<IRI, PredicateEdge> graph) {
        if (resultGraphs.containsKey(graphName)) {
            Graphs.addGraph(resultGraphs.get(graphName), graph);
        }
    }

    @Override
    public Collection<TimeVarying<Graph<IRI, PredicateEdge>>> asTimeVaryingEs() {
        return tvgs.values();
    }

    @Override
    public void add(IRI iri, TimeVarying<Graph<IRI, PredicateEdge>> tvg) {
        tvgs.put(iri, tvg);
    }

    @Override
    public void add(TimeVarying<Graph<IRI, PredicateEdge>> tvg) {
        defs.add(tvg);
    }

    public SDS<Graph<IRI, PredicateEdge>> materialize(final long ts) {
        // quads.clear() - empty
        //return null;

        defs.stream().map(g -> {
                g.materialize(ts);
                return g.get();
            }).forEach(g -> this.add(def, g));

        tvgs.entrySet().stream()
            .map(e -> {
                e.getValue().materialize(ts);
                return new NamedGraph(e.getKey(), e.getValue().get());
            }).forEach(n -> this.add(n.name, n.g));

        materialized();

        return this;
    }

    @Override
    public void materialized() {
        this.materialized = true;
    }

    @Override
    public Stream<Graph<IRI, PredicateEdge>> toStream() {
        return null;
        //TODO IMPLEMENT??
    }

    class NamedGraph {
        public IRI name;
        public Graph<IRI, PredicateEdge> g;

        public NamedGraph(IRI name, Graph<IRI, PredicateEdge> g) {
            this.name = name;
            this.g = g;
        }
    }
}

