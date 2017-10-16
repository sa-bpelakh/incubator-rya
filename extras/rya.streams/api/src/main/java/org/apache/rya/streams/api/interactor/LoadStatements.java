/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.streams.api.interactor;

import org.apache.rya.api.model.VisibilityStatement;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import info.aduna.iteration.CloseableIteration;

/**
 * An interactor that is used to load {@link VisibilityStatement}s into a Rya Streaming program.
 */
@DefaultAnnotation(NonNull.class)
public interface LoadStatements {

    /**
     * Loads a series of statements into the Rya Streams system. Once there are no more statements to load, or
     * an exception is thrown, the iteration is closed.
     *
     * @param statements - The {@link VisibilityStatements} that will be loaded. (not null)
     * @param Exception If an exception was thrown while iterating through the statements or while
     *   loading them into the Rya Streams system.
     */
    public void load(CloseableIteration<VisibilityStatement, Exception> statements) throws Exception;
}