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

import org.jgrapht.GraphMapping;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

abstract class VF2MultiMappingIterator<V, E>
        implements
        Iterator<GraphMapping<V, E>> {
    protected Comparator<V> vertexComparator;
    protected Comparator<E> edgeComparator;

    protected IsomorphicMultiGraphMapping<V, E> nextMapping;
    protected Boolean hadOneMapping;

    protected MultiGraphOrdering<V, E> ordering1, ordering2;

    protected ArrayDeque<VF2MultiState<V, E>> stateStack;

    public VF2MultiMappingIterator(
            MultiGraphOrdering<V, E> ordering1, MultiGraphOrdering<V, E> ordering2,
            Comparator<V> vertexComparator, Comparator<E> edgeComparator) {
        this.ordering1 = ordering1;
        this.ordering2 = ordering2;
        this.vertexComparator = vertexComparator;
        this.edgeComparator = edgeComparator;
        this.stateStack = new ArrayDeque<>();
    }

    /**
     * This function moves over all mappings between graph1 and graph2. It changes the state of the
     * whole iterator.
     *
     * @return null or one matching between graph1 and graph2
     */
    protected abstract IsomorphicMultiGraphMapping<V, E> match();

    protected IsomorphicMultiGraphMapping<V, E> matchAndCheck() {
        IsomorphicMultiGraphMapping<V, E> rel = match();
        if (rel != null) {
            hadOneMapping = true;
        }
        return rel;
    }

    @Override
    public boolean hasNext() {
        return nextMapping != null || (nextMapping = matchAndCheck()) != null;

    }

    @Override
    public IsomorphicMultiGraphMapping<V, E> next() {
        if (nextMapping != null) {
            IsomorphicMultiGraphMapping<V, E> tmp = nextMapping;
            nextMapping = null;
            return tmp;
        }

        IsomorphicMultiGraphMapping<V, E> rel = matchAndCheck();
        if (rel == null) {
            throw new NoSuchElementException();
        }
        return rel;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
