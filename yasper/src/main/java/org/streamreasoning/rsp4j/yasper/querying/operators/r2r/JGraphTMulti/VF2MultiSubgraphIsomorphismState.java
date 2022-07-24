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


import java.util.Comparator;

class VF2MultiSubgraphIsomorphismState<V, E>
        extends
    VF2MultiState<V, E> {
    public VF2MultiSubgraphIsomorphismState(
            MultiGraphOrdering<V, E> g1, MultiGraphOrdering<V, E> g2, Comparator<V> vertexComparator,
            Comparator<E> edgeComparator) {
        super(g1, g2, vertexComparator, edgeComparator);
    }

    public VF2MultiSubgraphIsomorphismState(VF2MultiState<V, E> s) {
        super(s);
    }

    /**
     * @return true, if the already matched vertices of graph1 plus the first vertex of nextPair are
     * subgraph isomorphic to the already matched vertices of graph2 and the second one
     * vertex of nextPair.
     */
    @Override
    public boolean isFeasiblePair() {
        final String pairstr =
                (DEBUG) ? "(" + g1.getVertex(addVertex1) + ", " + g2.getVertex(addVertex2) + ")" : null;
        final String abortmsg = (DEBUG) ? pairstr + " does not fit in the current matching" : null;

        // check for semantic equality of both vertexes
        if (!areCompatibleVertexes(addVertex1, addVertex2)) {
            return false;
        }

        int termOutPred1 = 0, termOutPred2 = 0, termInPred1 = 0, termInPred2 = 0, newPred1 = 0,
                newPred2 = 0, termOutSucc1 = 0, termOutSucc2 = 0, termInSucc1 = 0, termInSucc2 = 0,
                newSucc1 = 0, newSucc2 = 0;

        // check outgoing edges of addVertex1
        final int[] outEdgesG1 = g1.getOutEdges(addVertex1);
        for (final int outEdgeG1 : outEdgesG1) {
            if (core1[outEdgeG1] != NULL_NODE) {
                final int outEdgeG2 = core1[outEdgeG1];
                if (!g2.hasEdge(addVertex2, outEdgeG2)
                    || !areCompatibleEdges(addVertex1, outEdgeG1, addVertex2, outEdgeG2)) {
                    if (DEBUG)
                        showLog(
                            "isFeasiblePair", abortmsg + ": edge from " + g2.getVertex(addVertex2)
                                + " to " + g2.getVertex(outEdgeG2) + " is missing in the 2nd graph");
                    return false;
                }
            } else {
                final int in1O1 = in1[outEdgeG1];
                final int out1O1 = out1[outEdgeG1];
                if ((in1O1 == 0) && (out1O1 == 0)) {
                    newSucc1++;
                    continue;
                }
                if (in1O1 > 0) {
                    termInSucc1++;
                }
                if (out1O1 > 0) {
                    termOutSucc1++;
                }
            }
        }

        // check outgoing edges of addVertex2
        final int[] outEdgesG2 = g2.getOutEdges(addVertex2);
        for (final int outEdgeG2 : outEdgesG2) {
            if (core2[outEdgeG2] != NULL_NODE) {
                int outEdgeG1 = core2[outEdgeG2];
                if (!g1.hasEdge(addVertex1, outEdgeG1)) {
                    if (DEBUG)
                        showLog(
                            "isFeasbilePair", abortmsg + ": edge from " + g1.getVertex(addVertex1)
                                + " to " + g1.getVertex(outEdgeG1) + " is missing in the 1st graph");
                    return false;
                }
            } else {
                final int in2O2 = in2[outEdgeG2];
                final int out2O2 = out2[outEdgeG2];
                if ((in2O2 == 0) && (out2O2 == 0)) {
                    newSucc2++;
                    continue;
                }
                if (in2O2 > 0) {
                    termInSucc2++;
                }
                if (out2O2 > 0) {
                    termOutSucc2++;
                }
            }
        }

        if ((termInSucc1 < termInSucc2) || (termOutSucc1 < termOutSucc2) || (newSucc1 < newSucc2)) {
            if (DEBUG) {
                String cause = "", v1 = g1.getVertex(addVertex1).toString(),
                        v2 = g2.getVertex(addVertex2).toString();

                if (termInSucc2 > termInSucc1) {
                    cause = "|Tin2 ∩ Succ(Graph2, " + v2 + ")| > |Tin1 ∩ Succ(Graph1, " + v1 + ")|";
                } else if (termOutSucc2 > termOutSucc1) {
                    cause =
                            "|Tout2 ∩ Succ(Graph2, " + v2 + ")| > |Tout1 ∩ Succ(Graph1, " + v1 + ")|";
                } else if (newSucc2 > newSucc1) {
                    cause = "|N‾ ∩ Succ(Graph2, " + v2 + ")| > |N‾ ∩ Succ(Graph1, " + v1 + ")|";
                }

                showLog("isFeasbilePair", abortmsg + ": " + cause);
            }

            return false;
        }

        // check incoming edges of addVertex1
        final int[] inEdgesG1 = g1.getInEdges(addVertex1);
        for (final int inEdgeG1 : inEdgesG1) {
            if (core1[inEdgeG1] != NULL_NODE) {
                final int inEdgeG2 = core1[inEdgeG1];
                if (!g2.hasEdge(inEdgeG2, addVertex2)
                    || !areCompatibleEdges(inEdgeG1, addVertex1, inEdgeG2, addVertex2)) {
                    if (DEBUG)
                        showLog(
                            "isFeasbilePair",
                            abortmsg + ": edge from " + g2.getVertex(inEdgeG2) + " to "
                                + g2.getVertex(addVertex2) + " is missing in the 2nd graph");
                    return false;
                }
            } else {
                final int in1O1 = in1[inEdgeG1];
                final int out1O1 = out1[inEdgeG1];
                if ((in1O1 == 0) && (out1O1 == 0)) {
                    newPred1++;
                    continue;
                }
                if (in1O1 > 0) {
                    termInPred1++;
                }
                if (out1O1 > 0) {
                    termOutPred1++;
                }
            }
        }

        // check incoming edges of addVertex2
        final int[] inEdgesG2 = g2.getInEdges(addVertex2);
        for (final int inEdgeG2 : inEdgesG2) {
            if (core2[inEdgeG2] != NULL_NODE) {
                final int inEdgeG1 = core2[inEdgeG2];
                if (!g1.hasEdge(inEdgeG1, addVertex1)) {
                    if (DEBUG)
                        showLog(
                            "isFeasiblePair",
                            abortmsg + ": edge from " + g1.getVertex(inEdgeG1) + " to "
                                + g1.getVertex(addVertex1) + " is missing in the 1st graph");
                    return false;
                }
            } else {
                final int in2O2 = in2[inEdgeG2];
                final int out2O2 = out2[inEdgeG2];
                if ((in2O2 == 0) && (out2O2 == 0)) {
                    newPred2++;
                    continue;
                }
                if (in2O2 > 0) {
                    termInPred2++;
                }
                if (out2O2 > 0) {
                    termOutPred2++;
                }
            }
        }

        if ((termInPred1 >= termInPred2) && (termOutPred1 >= termOutPred2)
                && (newPred1 >= newPred2)) {
            if (DEBUG)
                showLog("isFeasiblePair", pairstr + " fits");
            return true;
        } else {
            if (DEBUG) {
                String cause = "", v1 = g1.getVertex(addVertex1).toString(),
                        v2 = g2.getVertex(addVertex2).toString();

                if (termInPred2 > termInPred1) {
                    cause = "|Tin2 ∩ Pred(Graph2, " + v2 + ")| > |Tin1 ∩ Pred(Graph1, " + v1 + ")|";
                } else if (termOutPred2 > termOutPred1) {
                    cause =
                            "|Tout2 ∩ Pred(Graph2, " + v2 + ")| > |Tout1 ∩ Pred(Graph1, " + v1 + ")|";
                } else if (newPred2 > newPred1) {
                    cause = "|N‾ ∩ Pred(Graph2, " + v2 + ")| > |N‾ ∩ Pred(Graph1, " + v1 + ")|";
                }

                showLog("isFeasbilePair", abortmsg + ": " + cause);
            }

            return false;
        }
    }
}
