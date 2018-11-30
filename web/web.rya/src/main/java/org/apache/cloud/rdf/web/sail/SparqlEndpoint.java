package org.apache.cloud.rdf.web.sail;

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

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.log.LogUtils;
import org.apache.rya.api.security.SecurityProvider;
import org.apache.rya.rdftriplestore.RdfCloudTripleStoreConnection;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.parser.*;
import org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONWriter;
import org.eclipse.rdf4j.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.rdfxml.RDFXMLWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.FormParam;
import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

import static org.apache.rya.api.RdfCloudTripleStoreConstants.VALUE_FACTORY;

/**
 * Class RdfController
 * Date: Mar 7, 2012
 * Time: 11:07:19 AM
 */
@Controller
public class SparqlEndpoint {
    private static final Logger log = Logger.getLogger(SparqlEndpoint.class);

    private static final int QUERY_TIME_OUT_SECONDS = 120;
    public static final String SPARQL_JSON_MIME_TYPE = "application/sparql-results+json";
    public static final String SPARQL_XML_MIME_TYPE = "application/sparql-results+xml";

    @Autowired
    private SailRepository repository;

    @Autowired
    private SecurityProvider provider;

    @RequestMapping(value = "/sparql", method = RequestMethod.GET)
    public void queryRdfGet(@RequestParam("query") final String query,
                         @RequestParam(value = RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, required = false) String auth,
                         @RequestParam(value = RdfCloudTripleStoreConfiguration.CONF_CV, required = false) final String vis,
                         @RequestParam(value = "infer", required = false) final String infer,
                         final HttpServletRequest request,
                         final HttpServletResponse response) {
        queryInternal(query, vis, infer, request, response);
    }

    @RequestMapping(value = "/sparql", method = RequestMethod.POST, consumes = "application/x-www-form-urlencoded")
    public void queryRdfForm(@FormParam("query") final String query,
                             @FormParam(value = RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH) String auth,
                             @FormParam(value = RdfCloudTripleStoreConfiguration.CONF_CV) final String vis,
                             @FormParam(value = "infer") final String infer,
                             final HttpServletRequest request,
                             final HttpServletResponse response) {
        queryInternal(query, vis, infer, request, response);
    }

    @RequestMapping(value = "/sparql", method = RequestMethod.POST, consumes = "\tapplication/sparql-query")
    public void queryRdfPost(@RequestBody final String query,
                             @RequestParam(value = RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, required = false) String auth,
                             @RequestParam(value = RdfCloudTripleStoreConfiguration.CONF_CV, required = false) final String vis,
                             @RequestParam(value = "infer", required = false) final String infer,
                             final HttpServletRequest request,
                             final HttpServletResponse response) {
        queryInternal(query, vis, infer, request, response);
    }

    private void queryInternal(final String query, final String vis, final String infer,
                               final HttpServletRequest request,
                               final HttpServletResponse response)
    {
        // WARNING: if you add to the above request variables,
        // Be sure to validate and encode since they come from the outside and could contain odd damaging character sequences.

        final Thread queryThread = Thread.currentThread();
        final String auth = StringUtils.arrayToCommaDelimitedString(provider.getUserAuths(request));
        final Timer timer = new Timer();
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                log.debug("interrupting");
                queryThread.interrupt();

            }
        }, QUERY_TIME_OUT_SECONDS * 1000);

        try (final SailRepositoryConnection conn = repository.getConnection()) {
            final ServletOutputStream os = response.getOutputStream();

            final boolean isBlankQuery = StringUtils.isEmpty(query);
            final ParsedOperation operation = QueryParserUtil.parseOperation(QueryLanguage.SPARQL, query, null);

            final String emit = request.getHeader(HttpHeaders.ACCEPT);

            if (!isBlankQuery) {
                if (operation instanceof ParsedGraphQuery) {
                    // Perform Graph Query
                    final RDFHandler handler = new RDFXMLWriter(os);
                    response.setContentType("text/xml");
                    performGraphQuery(query, conn, auth, infer, handler);
                } else if (operation instanceof ParsedTupleQuery) {
                    // Perform Tuple Query
                    TupleQueryResultHandler handler;


                    if (SPARQL_JSON_MIME_TYPE.equals(emit)) {
                        handler = new SPARQLResultsJSONWriter(os);
                        response.setContentType(SPARQL_JSON_MIME_TYPE);
                    } else {
                        handler = new SPARQLResultsXMLWriter(os);
                        response.setContentType(SPARQL_XML_MIME_TYPE);
                    }

                    performQuery(query, conn, auth, infer, handler);
                } else if (operation instanceof ParsedUpdate) {
                    // Perform Update Query
                    performUpdate(query, conn, os, infer, vis);
                } else {
                    throw new MalformedQueryException("Cannot process query. Query type not supported.");
                }
            }

        } catch (final Exception e) {
            log.error("Error running query", e);
            throw new RuntimeException(e);
        }

        timer.cancel();
    }

    private void performQuery(final String query, final RepositoryConnection conn, final String auth, final String infer, final TupleQueryResultHandler handler) throws RepositoryException, MalformedQueryException, QueryEvaluationException, TupleQueryResultHandlerException {
        final TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        if (auth != null && auth.length() > 0) {
            tupleQuery.setBinding(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, VALUE_FACTORY.createLiteral(auth));
        }
        if (infer != null && infer.length() > 0) {
            tupleQuery.setBinding(RdfCloudTripleStoreConfiguration.CONF_INFER, VALUE_FACTORY.createLiteral(Boolean.parseBoolean(infer)));
        }

        final CountingTupleQueryResultHandlerWrapper sparqlWriter = new CountingTupleQueryResultHandlerWrapper(handler);
        final long startTime = System.currentTimeMillis();
        tupleQuery.evaluate(sparqlWriter);
        log.info(String.format("Query Time = %.3f   Result Count = %s\n",
                (System.currentTimeMillis() - startTime) / 1000.,
                sparqlWriter.getCount()));
    }

    private void performGraphQuery(final String query, final RepositoryConnection conn, final String auth, final String infer, final RDFHandler handler) throws RepositoryException, MalformedQueryException, QueryEvaluationException, RDFHandlerException {
        final GraphQuery graphQuery = conn.prepareGraphQuery(QueryLanguage.SPARQL, query);
        if (auth != null && auth.length() > 0) {
            graphQuery.setBinding(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, VALUE_FACTORY.createLiteral(auth));
        }
        if (infer != null && infer.length() > 0) {
            graphQuery.setBinding(RdfCloudTripleStoreConfiguration.CONF_INFER, VALUE_FACTORY.createLiteral(Boolean.parseBoolean(infer)));
        }

        final long startTime = System.currentTimeMillis();
        graphQuery.evaluate(handler);
        log.info(String.format("Query Time = %.3f\n", (System.currentTimeMillis() - startTime) / 1000.));
    }

    private void performUpdate(final String query, final SailRepositoryConnection conn, final ServletOutputStream os, final String infer, final String vis) throws RepositoryException, MalformedQueryException, IOException {
        final Update update = conn.prepareUpdate(QueryLanguage.SPARQL, query);
        if (infer != null && infer.length() > 0) {
            update.setBinding(RdfCloudTripleStoreConfiguration.CONF_INFER, VALUE_FACTORY.createLiteral(Boolean.parseBoolean(infer)));
        }

        if (conn.getSailConnection() instanceof RdfCloudTripleStoreConnection && vis != null) {
            final RdfCloudTripleStoreConnection<?> sailConnection = (RdfCloudTripleStoreConnection<?>) conn.getSailConnection();
            sailConnection.getConf().set(RdfCloudTripleStoreConfiguration.CONF_CV, vis);
        }

        final long startTime = System.currentTimeMillis();

        try {
            update.execute();
        } catch (final UpdateExecutionException e) {
            final String message = "Update could not be successfully completed for query: ";
            os.print(String.format(message + "%s\n\n", StringEscapeUtils.escapeHtml4(query)));
            log.error(message + LogUtils.clean(query), e);
        }

        log.info(String.format("Update Time = %.3f\n", (System.currentTimeMillis() - startTime) / 1000.));
    }

    private static final class CountingTupleQueryResultHandlerWrapper implements TupleQueryResultHandler {
        private final TupleQueryResultHandler indir;
        private int count = 0;

        CountingTupleQueryResultHandlerWrapper(final TupleQueryResultHandler indir) {
            this.indir = indir;
        }

        int getCount() {
            return count;
        }

        @Override
        public void endQueryResult() throws TupleQueryResultHandlerException {
            indir.endQueryResult();
        }

        @Override
        public void handleSolution(final BindingSet bindingSet) throws TupleQueryResultHandlerException {
            count++;
            indir.handleSolution(bindingSet);
        }

        @Override
        public void startQueryResult(final List<String> bindingNames) throws TupleQueryResultHandlerException {
            count = 0;
            indir.startQueryResult(bindingNames);
        }

        @Override
        public void handleBoolean(final boolean arg0) throws QueryResultHandlerException {
        }

        @Override
        public void handleLinks(final List<String> arg0) throws QueryResultHandlerException {
        }
    }

    @RequestMapping(value = "/sparql", method = RequestMethod.POST)
    public void storeRdf(@RequestParam(value = RdfCloudTripleStoreConfiguration.CONF_CV, required = false) final String cv,
                         @RequestParam(required = false) final String graph,
                         @RequestBody final String body,
                         final HttpServletRequest request)
            throws RepositoryException, IOException, RDFParseException {
        storeRdfInternal(cv, graph, body, request, false);
    }

    @RequestMapping(value = "/sparql", method = RequestMethod.PUT)
    public void replaceRdf(@RequestParam(value = RdfCloudTripleStoreConfiguration.CONF_CV, required = false) final String cv,
                           @RequestParam(required = false) final String graph,
                           @RequestBody final String body,
                           final HttpServletRequest request)
            throws RepositoryException, IOException, RDFParseException {
        storeRdfInternal(cv, graph, body, request, true);
    }

    private void storeRdfInternal(final String cv, final String graph,
                                  final String body, final HttpServletRequest request, final boolean delete)
            throws IOException {
        final String format = request.getHeader(HttpHeaders.CONTENT_TYPE);
        final RDFFormat format_r = rdfFormatFromMimeType(format);

        // add named graph as context (if specified).
        final List<Resource> contextList = new ArrayList<>();
        if (graph != null) {
            contextList.add(VALUE_FACTORY.createIRI(graph));
        }

        try (final SailRepositoryConnection conn = repository.getConnection())
        {
            if (conn.getSailConnection() instanceof RdfCloudTripleStoreConnection && cv != null) {
                final RdfCloudTripleStoreConnection<?> sailConnection = (RdfCloudTripleStoreConnection<?>) conn.getSailConnection();
                sailConnection.getConf().set(RdfCloudTripleStoreConfiguration.CONF_CV, cv);
            }

            if (delete) {
                if (!contextList.isEmpty()) {
                    conn.prepareUpdate("DROP SILENT GRAPH <" + graph + ">").execute();
                } else {
                    conn.prepareUpdate("DROP SILENT DEFAULT").execute();
                }
            }
            conn.add(new StringReader(body), "", format_r, contextList.toArray(new Resource[contextList.size()]));
            conn.commit();
        }
    }

    private RDFFormat rdfFormatFromMimeType(final String mimeType) {
        if (StringUtils.isEmpty(mimeType))
        {
            return RDFFormat.RDFXML;
        }
        else
        {
            final Set<RDFFormat> RDF_FORMATS = RDFParserRegistry.getInstance().getKeys();
            return RDF_FORMATS.stream().filter(fmt -> fmt.getMIMETypes().contains(mimeType)).findFirst().
                    orElseThrow(() -> new RuntimeException("RDFFormat[" + mimeType + "] unknown or unsupported"));
        }
    }

    @RequestMapping(value = "/sparql", method = RequestMethod.DELETE)
    public void deleteGraph(@RequestParam(required = false) final String graph)
            throws RepositoryException {
        try (final SailRepositoryConnection conn = repository.getConnection())
        {

            if (graph != null) {
                conn.prepareUpdate("DROP GRAPH <" + graph + ">").execute();
            } else {
                conn.prepareUpdate("DROP DEFAULT").execute();
            }
            conn.commit();
        }
    }
}
