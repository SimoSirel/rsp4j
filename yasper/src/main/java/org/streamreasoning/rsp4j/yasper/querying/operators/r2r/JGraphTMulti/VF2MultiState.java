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

import java.util.Arrays;
import java.util.Comparator;

/**
 * controls the matching between two graphs according to the VF2 algorithm.
 *
 * @param <V> the type of the vertices
 * @param <E> the type of the edges
 * @author Fabian Späh
 */
abstract class VF2MultiState<V, E> {
    public static final int NULL_NODE = -1;

    protected static final boolean DEBUG = false;

    protected final int[] core1, core2, in1, in2, out1, out2;

    protected final int n1, n2;

    protected int coreLen_howManyHaveBeenMatched, t1BothLen, t2BothLen, t1InLen, t2InLen, t1OutLen, t2OutLen, addedVertex1,
            addVertex1, addVertex2;

    protected final MultiGraphOrdering<V, E> g1, g2;

    protected final Comparator<V> vertexComparator;
    protected final Comparator<E> edgeComparator;

    /**
     * @param g1               GraphOrdering on first graph
     * @param g2               GraphOrdering on second graph (possible subgraph)
     * @param vertexComparator comparator for semantic equality of vertices
     * @param edgeComparator   comparator for semantic equality of edges
     */
    public VF2MultiState(
            MultiGraphOrdering<V, E> g1, MultiGraphOrdering<V, E> g2, Comparator<V> vertexComparator,
            Comparator<E> edgeComparator) {
        this.g1 = g1;
        this.g2 = g2;
        this.vertexComparator = vertexComparator;
        this.edgeComparator = edgeComparator;

        n1 = g1.getVertexCount();
        n2 = g2.getVertexCount();

        core1 = new int[n1];
        in1 = new int[n1];
        out1 = new int[n1];
        core2 = new int[n2];
        in2 = new int[n2];
        out2 = new int[n2];
        Arrays.fill(core1, NULL_NODE);
        Arrays.fill(core2, NULL_NODE);

        coreLen_howManyHaveBeenMatched = 0;
        addedVertex1 = addVertex1 = addVertex2 = NULL_NODE;

        t1BothLen = t2BothLen = t1InLen = t2InLen = t1OutLen = t2OutLen = 0;
    }

    /**
     * copy constructor
     *
     * @param s
     */
    public VF2MultiState(VF2MultiState<V, E> s) {
        g1 = s.g1;
        g2 = s.g2;

        core1 = s.core1;
        core2 = s.core2;
        in1 = s.in1;
        in2 = s.in2;
        out1 = s.out1;
        out2 = s.out2;

        coreLen_howManyHaveBeenMatched = s.coreLen_howManyHaveBeenMatched;

        n1 = s.n1;
        n2 = s.n2;

        t1BothLen = s.t1BothLen;
        t2BothLen = s.t2BothLen;
        t1InLen = s.t1InLen;
        t2InLen = s.t2InLen;
        t1OutLen = s.t1OutLen;
        t2OutLen = s.t2OutLen;

        vertexComparator = s.vertexComparator;
        edgeComparator = s.edgeComparator;

        addVertex1 = s.addVertex1;
        addVertex2 = s.addVertex2;
        addedVertex1 = s.addedVertex1;
    }

    /**
     * calculates a pair of nodes which may be added to the current matching, according to the VF2
     * algorithm.
     *
     * @return false, if there are no more pairs left
     */
    public boolean nextPair() {
        if (addVertex2 == NULL_NODE) {
            addVertex2 = 0;
        }

        if (addVertex1 == NULL_NODE) {
            addVertex1 = 0;
        } else {
            addVertex1++;
        }

        // check incoming and outgoing edges
        if ((t1BothLen > coreLen_howManyHaveBeenMatched) && (t2BothLen > coreLen_howManyHaveBeenMatched)) {
            // find minimum for addVertex2 in core2 and t2in/t2out
            while ((addVertex2 < n2) && ((core2[addVertex2] != NULL_NODE) || (out2[addVertex2] == 0)
                    || (in2[addVertex2] == 0))) {
                addVertex2++;
                addVertex1 = 0;
            }

            // find first/next vertex for addVertex1 in core1 and t1in/t1out
            while ((addVertex1 < n1) && ((core1[addVertex1] != NULL_NODE) || (out1[addVertex1] == 0)
                    || (in1[addVertex1] == 0))) {
                addVertex1++;
            }
        }

        // check outgoing edges - do we have potential outgoing nodes - look until we find a node that is not matched(not NULL in core) AND is a potential out node
        else if ((t1OutLen > coreLen_howManyHaveBeenMatched) && (t2OutLen > coreLen_howManyHaveBeenMatched)) {
            while ((addVertex2 < n2)
                    && ((core2[addVertex2] != NULL_NODE) || (out2[addVertex2] == 0))) {
                addVertex2++;
                addVertex1 = 0;
            }

            while ((addVertex1 < n1)
                    && ((core1[addVertex1] != NULL_NODE) || (out1[addVertex1] == 0))) {
                addVertex1++;
            }
        }

        // check incoming edges
        else if ((t1InLen > coreLen_howManyHaveBeenMatched) && (t2InLen > coreLen_howManyHaveBeenMatched)) {
            while ((addVertex2 < n2)
                    && ((core2[addVertex2] != NULL_NODE) || (in2[addVertex2] == 0))) {
                addVertex2++;
                addVertex1 = 0;
            }

            while ((addVertex1 < n1)
                    && ((core1[addVertex1] != NULL_NODE) || (in1[addVertex1] == 0))) {
                addVertex1++;
            }
        }

        // check new edges - until we find any node that is not matched
        else {
            while ((addVertex2 < n2) && (core2[addVertex2] != NULL_NODE)) {
                addVertex2++;
                addVertex1 = 0;
            }

            while ((addVertex1 < n1) && (core1[addVertex1] != NULL_NODE)) {
                addVertex1++;
            }
        }

        if ((addVertex1 < n1) && (addVertex2 < n2)) {
            if (DEBUG)
                showLog(
                        "nextPair", "next candidate pair: (" + g1.getVertex(addVertex1) + ", "
                                + g2.getVertex(addVertex2) + ")");
            return true;
        }

        // there are no more pairs..
        if (DEBUG)
            showLog("nextPair", "no more candidate pairs");

        addVertex1 = addVertex2 = NULL_NODE;
        return false;
    }

    /**
     * adds the pair to the current matching.
     */
    public void addPair() {
        if (DEBUG)
            showLog(
                    "addPair",
                    "(" + g1.getVertex(addVertex1) + ", " + g2.getVertex(addVertex2) + ") added");

        coreLen_howManyHaveBeenMatched++;
        addedVertex1 = addVertex1;

        if (in1[addVertex1] == 0) {
            in1[addVertex1] = coreLen_howManyHaveBeenMatched;
            t1InLen++;
            if (out1[addVertex1] > 0) {
                t1BothLen++;
            }
        }

        if (out1[addVertex1] == 0) {
            out1[addVertex1] = coreLen_howManyHaveBeenMatched;
            t1OutLen++;
            if (in1[addVertex1] > 0) {
                t1BothLen++;
            }
        }

        if (in2[addVertex2] == 0) {
            in2[addVertex2] = coreLen_howManyHaveBeenMatched;
            t2InLen++;
            if (out2[addVertex2] > 0) {
                t2BothLen++;
            }
        }

        if (out2[addVertex2] == 0) {
            out2[addVertex2] = coreLen_howManyHaveBeenMatched;
            t2OutLen++;
            if (in2[addVertex2] > 0) {
                t2BothLen++;
            }
        }

        core1[addVertex1] = addVertex2;
        core2[addVertex2] = addVertex1;

        for (int inEdgeG1 : g1.getInEdges(addVertex1)) {
            if (in1[inEdgeG1] == 0) {
                in1[inEdgeG1] = coreLen_howManyHaveBeenMatched;
                t1InLen++;
                if (out1[inEdgeG1] > 0) {
                    t1BothLen++;
                }
            }
        }

        for (int outEdgeG1 : g1.getOutEdges(addVertex1)) {
            if (out1[outEdgeG1] == 0) {
                out1[outEdgeG1] = coreLen_howManyHaveBeenMatched;
                t1OutLen++;
                if (in1[outEdgeG1] > 0) {
                    t1BothLen++;
                }
            }
        }

        for (int inEdgeG2 : g2.getInEdges(addVertex2)) {
            if (in2[inEdgeG2] == 0) {
                in2[inEdgeG2] = coreLen_howManyHaveBeenMatched;
                t2InLen++;
                if (out2[inEdgeG2] > 0) {
                    t2BothLen++;
                }
            }
        }

        for (int outEdgeG2 : g2.getOutEdges(addVertex2)) {
            if (out2[outEdgeG2] == 0) {
                out2[outEdgeG2] = coreLen_howManyHaveBeenMatched;
                t2OutLen++;
                if (in2[outEdgeG2] > 0) {
                    t2BothLen++;
                }
            }
        }
    }

    /**
     * @return is the matching already complete?
     */
    public boolean isGoal() {
        return coreLen_howManyHaveBeenMatched == n2;
    }

    /**
     * @return true, if the already matched vertices of graph1 plus the first vertex of nextPair are
     * isomorphic to the already matched vertices of graph2 and the second one vertex of
     * nextPair.
     */
    public abstract boolean isFeasiblePair();

    /**
     * removes the last added pair from the matching
     */
    public void backtrack() {
        int addedVertex2 = core1[addedVertex1];

        if (DEBUG)
            showLog(
                    "backtrack", "remove (" + g1.getVertex(addedVertex1) + ", "
                            + g2.getVertex(addedVertex2) + ") from the matching");

        if (in1[addedVertex1] == coreLen_howManyHaveBeenMatched) {
            in1[addedVertex1] = 0;
        }

        for (int inEdgesG1 : g1.getInEdges(addedVertex1)) {
            if (in1[inEdgesG1] == coreLen_howManyHaveBeenMatched) {
                in1[inEdgesG1] = 0;
            }
        }

        if (out1[addedVertex1] == coreLen_howManyHaveBeenMatched) {
            out1[addedVertex1] = 0;
        }

        for (int other : g1.getOutEdges(addedVertex1)) {
            if (out1[other] == coreLen_howManyHaveBeenMatched) {
                out1[other] = 0;
            }
        }

        if (in2[addedVertex2] == coreLen_howManyHaveBeenMatched) {
            in2[addedVertex2] = 0;
        }

        for (int other : g2.getInEdges(addedVertex2)) {
            if (in2[other] == coreLen_howManyHaveBeenMatched) {
                in2[other] = 0;
            }
        }

        if (out2[addedVertex2] == coreLen_howManyHaveBeenMatched) {
            out2[addedVertex2] = 0;
        }

        for (int other : g2.getOutEdges(addedVertex2)) {
            if (out2[other] == coreLen_howManyHaveBeenMatched) {
                out2[other] = 0;
            }
        }

        core1[addedVertex1] = core2[addedVertex2] = NULL_NODE;
        coreLen_howManyHaveBeenMatched--;
        addedVertex1 = NULL_NODE;
    }

    /**
     * checks the vertices $v_1$ and $v_2$ for semantic equivalence
     *
     * @param v1
     * @param v2
     * @return v1 and v2 are equivalent
     */
    protected boolean areCompatibleVertexes(int v1, int v2) {
        return (vertexComparator == null)
                || (vertexComparator.compare(g1.getVertex(v1), g2.getVertex(v2)) == 0);
    }

    /**
     * checks the edges from $v_1$ to $v_2$ and from $u_1$ to $u_2$ for semantic equivalence
     *
     * @param v1
     * @param v2
     * @param u1
     * @param u2
     * @return edges are equivalent
     */
    protected boolean areCompatibleEdges(int v1, int v2, int u1, int u2) {
        return (edgeComparator == null)
                || (edgeComparator.compare(g1.getEdge(v1, v2), g2.getEdge(u1, u2)) == 0);
    }

    public IsomorphicMultiGraphMapping<V, E> getCurrentMapping() {
        return new IsomorphicMultiGraphMapping<>(g1, g2, core1, core2);
    }

    public void resetAddVertexes() {
        addVertex1 = addVertex2 = NULL_NODE;
    }

    /**
     * creates the debug output only if DEBUG is true.
     *
     * @param method
     * @param str
     */
    protected void showLog(String method, String str) {
        if (!DEBUG) {
            return;
        }

        char[] indent = new char[2 * coreLen_howManyHaveBeenMatched];
        Arrays.fill(indent, ' ');
        System.out.println((new String(indent)) + method + "> " + str);
    }
}
