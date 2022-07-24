/*
 * (C) Copyright 2015-2021, by Fabian Späh and Contributors.
 *
 * JGraphT : a free Java graph-theory library
 *
 * See the CONTRIBUTORS.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the
 * GNU Lesser General Public License v2.1 or later
 * which is available at
 * http://www.gnu.org/licenses/old-licenses/lgpl-2.1-standalone.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR LGPL-2.1-or-later
 */
package org.streamreasoning.rsp4j.yasper.querying.operators.r2r.JGraphTMulti;

import org.jgrapht.Graph;
import org.jgrapht.alg.isomorphism.VF2SubgraphIsomorphismInspector;

import java.util.Comparator;

/**
 * This class is used to iterate over all existing (subgraph isomorphic) mappings between two
 * graphs. It is used by the {@link VF2SubgraphIsomorphismInspector}.
 *
 * @param <V> the type of the vertices
 * @param <E> the type of the edges
 */
class VF2MultiSubgraphMappingIterator<V, E>
    extends
    VF2MultiMappingIterator<V, E>
{
    public VF2MultiSubgraphMappingIterator(
            MultiGraphOrdering<V, E> ordering1, MultiGraphOrdering<V, E> ordering2,
            Comparator<V> vertexComparator, Comparator<E> edgeComparator)
    {
        super(ordering1, ordering2, vertexComparator, edgeComparator);
    }

    @Override
    protected IsomorphicMultiGraphMapping<V, E> match()
    {
        VF2MultiState<V, E> s;

        if (stateStack.isEmpty()) {
            Graph<V, E> g1 = ordering1.getGraph(), g2 = ordering2.getGraph();

            if ((g1.vertexSet().size() < g2.vertexSet().size())
                || (g1.edgeSet().size() < g2.edgeSet().size()))
            {
                return null;
            }

            s = new VF2MultiSubgraphIsomorphismState<>(
                ordering1, ordering2, vertexComparator, edgeComparator);

            if (g2.vertexSet().isEmpty()) {
                return (hadOneMapping != null) ? null : s.getCurrentMapping();
            }
        } else {
            stateStack.pop().backtrack();
            s = stateStack.pop();
        }

        while (true) {
            while (s.nextPair()) {
                if (s.isFeasiblePair()) {
                    stateStack.push(s);
                    s = new VF2MultiSubgraphIsomorphismState<>(s);
                    s.addPair();

                    if (s.isGoal()) {
                        stateStack.push(s);
                        return s.getCurrentMapping();
                    }

                    s.resetAddVertexes();
                }
            }

            if (stateStack.isEmpty()) {
                return null;
            }

            s.backtrack(); //why is this needed if we are gonna pop anyway?
            s = stateStack.pop();
        }
    }
}
