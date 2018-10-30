package org.culturegraph.solr.handler.dataimport;

import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.dataimport.*;
import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@SuppressWarnings("unchecked")
public class MetamorphEntityProcessorTest {
    @Test
    public void testSkipOnError() throws IOException {
        /* we want to create the equiv of :
         *  <entity name="morphed_record"
         *          processor="MetamorphEntityProcessor"
         *          url="record.mrc"
         *          inputFormat="marc"
         *          metamorph="morph.xml"
         *          includeFullRecord="true"
         *          onError="skip"
         *          />
         */

        String marc21Record = loadMarc21Record();

        Map attrs = createMap(
                MetamorphEntityProcessor.DATASOURCE_URL, "src/test-files/record.mrc",
                MetamorphEntityProcessor.INPUT_FORMAT, "marc21",
                MetamorphEntityProcessor.MORPH_DEF, "src/test-files/morph.xml",
                MetamorphEntityProcessor.INCLUDE_FULL_RECORD, "true",
                MetamorphEntityProcessor.ON_ERROR, MetamorphEntityProcessor.SKIP
        );


        Context ctx = getContext(
                null,                      //parentEntity
                new VariableResolver(),          //resolver
                createReaderDataSource("tooShort\u001D" + marc21Record + "tooShort\u001D"),  //parentDataSource
                Context.FULL_DUMP,               //currProcess
                Collections.EMPTY_LIST,          //entityFields
                attrs                            //entityAttrs
        );

        MetamorphEntityProcessor ep = new MetamorphEntityProcessor();
        ep.init(ctx);

        Map<String, Object> row = ep.nextRow();
        assertThat(row, hasEntry("idn", "000000000"));
        assertThat(row, hasEntry("type", "record"));
        assertThat(row, hasEntry("fullRecord", marc21Record));
        assertThat(ep.nextRow(), is(nullValue()));
    }

    @Test
    public void shouldReadMarcRecordFromReader() throws IOException {
        /* we want to create the equiv of :
         *  <entity name="morphed_record"
         *          processor="MetamorphEntityProcessor"
         *          url="record.mrc"
         *          inputFormat="marc"
         *          metamorph="morph.xml"
         *          includeFullRecord="true"
         *          />
         */

        String marc21Record = loadMarc21Record();

        Map attrs = createMap(
                MetamorphEntityProcessor.DATASOURCE_URL, "src/test-files/record.mrc",
                MetamorphEntityProcessor.INPUT_FORMAT, "marc21",
                MetamorphEntityProcessor.MORPH_DEF, "src/test-files/morph.xml",
                MetamorphEntityProcessor.INCLUDE_FULL_RECORD, "true"
        );


        Context ctx = getContext(
                null,                      //parentEntity
                new VariableResolver(),          //resolver
                createReaderDataSource(marc21Record),  //parentDataSource
                Context.FULL_DUMP,               //currProcess
                Collections.EMPTY_LIST,          //entityFields
                attrs                            //entityAttrs
        );

        MetamorphEntityProcessor ep = new MetamorphEntityProcessor();
        ep.init(ctx);

        Map<String, Object> row = ep.nextRow();
        assertThat(row, hasEntry("idn", "000000000"));
        assertThat(row, hasEntry("type", "record"));
        assertThat(row, hasEntry("fullRecord", marc21Record));
    }

    @Test
    public void shouldReadFromMarcRecordFromInputStream() throws IOException {
        /* we want to create the equiv of :
         *  <entity name="morphed_record"
         *          processor="MetamorphEntityProcessor"
         *          url="record.mrc"
         *          inputFormat="marc"
         *          metamorph="morph.xml"
         *          includeFullRecord="true"
         *          />
         */

        String marc21Record = loadMarc21Record();

        Map attrs = createMap(
                MetamorphEntityProcessor.DATASOURCE_URL, "src/test-files/record.mrc",
                MetamorphEntityProcessor.INPUT_FORMAT, "marc21",
                MetamorphEntityProcessor.MORPH_DEF, "src/test-files/morph.xml",
                MetamorphEntityProcessor.INCLUDE_FULL_RECORD, "true"
        );


        Context ctx = getContext(
                null,                      //parentEntity
                new VariableResolver(),          //resolver
                createInputStreamDataSource(marc21Record),  //parentDataSource
                Context.FULL_DUMP,               //currProcess
                Collections.EMPTY_LIST,          //entityFields
                attrs                            //entityAttrs
        );

        MetamorphEntityProcessor ep = new MetamorphEntityProcessor();
        ep.init(ctx);

        Map<String, Object> row = ep.nextRow();
        assertThat(row, hasEntry("idn", "000000000"));
        assertThat(row, hasEntry("type", "record"));
        assertThat(row, hasEntry("fullRecord", marc21Record));
    }

    @Test
    public void shouldReadMarcRecordFromCompressedInputStream() throws IOException {
        /* we want to create the equiv of :
         *  <entity name="morphed_record"
         *          processor="MetamorphEntityProcessor"
         *          url="record.mrc"
         *          inputFormat="marc"
         *          metamorph="morph.xml"
         *          includeFullRecord="true"
         *          />
         */

        String marc21Record = loadMarc21Record();

        Map attrs = createMap(
                MetamorphEntityProcessor.DATASOURCE_URL, "src/test-files/record.mrc.gz",
                MetamorphEntityProcessor.INPUT_FORMAT, "marc21",
                MetamorphEntityProcessor.MORPH_DEF, "src/test-files/morph.xml",
                MetamorphEntityProcessor.INCLUDE_FULL_RECORD, "true"
        );


        Context ctx = getContext(
                null,                      //parentEntity
                new VariableResolver(),          //resolver
                createCompressedInputStreamDataSource(marc21Record),  //parentDataSource
                Context.FULL_DUMP,               //currProcess
                Collections.EMPTY_LIST,          //entityFields
                attrs                            //entityAttrs
        );

        MetamorphEntityProcessor ep = new MetamorphEntityProcessor();
        ep.init(ctx);

        Map<String, Object> row = ep.nextRow();
        assertThat(row, hasEntry("idn", "000000000"));
        assertThat(row, hasEntry("type", "record"));
        assertThat(row, hasEntry("fullRecord", marc21Record));
    }

    @Test
    public void shouldReadMarcXmlFromInputStream() throws IOException {
        /* we want to create the equiv of :
         *  <entity name="morphed_record"
         *          processor="MetamorphEntityProcessor"
         *          url="record.xml"
         *          inputFormat="marcxml"
         *          metamorph="morph.xml"
         *          includeFullRecord="true"
         *          />
         */

        String marcxml = loadMarcXmlRecord();

        Map attrs = createMap(
                MetamorphEntityProcessor.DATASOURCE_URL, "src/test-files/record.xml",
                MetamorphEntityProcessor.INPUT_FORMAT, "marcxml",
                MetamorphEntityProcessor.MORPH_DEF, "src/test-files/morph.xml",
                MetamorphEntityProcessor.INCLUDE_FULL_RECORD, "true"
        );


        Context ctx = getContext(
                null,                      //parentEntity
                new VariableResolver(),          //resolver
                createInputStreamDataSource(marcxml),  //parentDataSource
                Context.FULL_DUMP,               //currProcess
                Collections.EMPTY_LIST,          //entityFields
                attrs                            //entityAttrs
        );

        MetamorphEntityProcessor ep = new MetamorphEntityProcessor();
        ep.init(ctx);

        Map<String, Object> row = ep.nextRow();
        assertThat(row, hasEntry("idn", "000000000"));
        assertThat(row, hasEntry("type", "record"));
        assertThat(row, hasEntry("fullRecord", loadMarc21Record()));
    }

    @Test
    public void shouldReadMarcXmlFromCompressedInputStream() throws IOException {
        /* we want to create the equiv of :
         *  <entity name="morphed_record"
         *          processor="MetamorphEntityProcessor"
         *          url="record.xml.gz"
         *          inputFormat="marcxml"
         *          metamorph="morph.xml"
         *          includeFullRecord="true"
         *          />
         */

        String marcxml = loadMarcXmlRecord();

        Map attrs = createMap(
                MetamorphEntityProcessor.DATASOURCE_URL, "src/test-files/record.xml.gz",
                MetamorphEntityProcessor.INPUT_FORMAT, "marcxml",
                MetamorphEntityProcessor.MORPH_DEF, "src/test-files/morph.xml",
                MetamorphEntityProcessor.INCLUDE_FULL_RECORD, "true"
        );


        Context ctx = getContext(
                null,                      //parentEntity
                new VariableResolver(),          //resolver
                createCompressedInputStreamDataSource(marcxml),  //parentDataSource
                Context.FULL_DUMP,               //currProcess
                Collections.EMPTY_LIST,          //entityFields
                attrs                            //entityAttrs
        );

        MetamorphEntityProcessor ep = new MetamorphEntityProcessor();
        ep.init(ctx);

        Map<String, Object> row = ep.nextRow();
        assertThat(row, hasEntry("idn", "000000000"));
        assertThat(row, hasEntry("type", "record"));
        assertThat(row, hasEntry("fullRecord", loadMarc21Record()));
    }

    /**
     * Helper for creating data sources from strings.
     * @param records A collection of records.
     * @return A data source that returns a reader for that resource.
     */
    private DataSource<Reader> createReaderDataSource(String records) {
        return new DataSource<Reader>() {
            @Override
            public void init(Context context, Properties properties) {

            }

            @Override
            public Reader getData(String s) {
                return new StringReader(records);
            }

            @Override
            public void close() {

            }
        };
    }

    /**
     * Helper for creating data sources from strings.
     * @param records A collection of records.
     * @return A data source that returns a reader for that resource.
     */
    private DataSource<InputStream> createInputStreamDataSource(String records) {
        return new DataSource<InputStream>() {
            @Override
            public void init(Context context, Properties properties) {

            }

            @Override
            public InputStream getData(String s) {
                return new ByteArrayInputStream(records.getBytes(StandardCharsets.UTF_8));
            }

            @Override
            public void close() {

            }
        };
    }


    /**
     * Helper for creating data sources from strings.
     * @param records A collection of records.
     * @return A data source that returns a reader for that resource.
     */
    private DataSource<InputStream> createCompressedInputStreamDataSource(String records) {
        return new DataSource<InputStream>() {
            @Override
            public void init(Context context, Properties properties) {

            }

            @Override
            public InputStream getData(String s) {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                try {
                    OutputStream outputStream = new GZIPOutputStream(buffer);
                    outputStream.write(records.getBytes(StandardCharsets.UTF_8));
                    outputStream.close();
                } catch (IOException exp) {
                    throw new IllegalArgumentException();
                }
                ByteArrayInputStream result = new ByteArrayInputStream(buffer.toByteArray());
                return result;
            }

            @Override
            public void close() {

            }
        };
    }

    /**
     * Helper for creating a Context instance. Useful for testing Transformers
     */
    @SuppressWarnings("unchecked")
    private TestContext getContext(EntityProcessorWrapper parent,
                                   VariableResolver resolver, DataSource parentDataSource,
                                   String currProcess, final List<Map<String, String>> entityFields,
                                   final Map<String, String> entityAttrs) {
        if (resolver == null) resolver = new VariableResolver();
        final Context delegate = new ContextImpl(parent, resolver,
                parentDataSource, currProcess,
                new HashMap<>(), null, null);
        return new TestContext(entityAttrs, delegate, entityFields, parent == null);
    }

    /**
     * Helper for creating a Context instance. Useful for testing Transformers
     *
     * Copy of
     * https://github.com/apache/lucene-solr/blob/master/solr/contrib/dataimporthandler/src/test/org/apache/solr/handler/dataimport/AbstractDataImportHandlerTestCase.java#L141
     */
    @SuppressWarnings("unchecked")
    private Context getContext(final Map<String, String> entityAttrs) {
        VariableResolver resolver = new VariableResolver();
        final Context delegate = new ContextImpl(null, resolver, null, null, new HashMap<String, Object>(), null, null);
        return new TestContext(entityAttrs, delegate, null, true);
    }

    /**
     * Helper for creating a Context instance. Useful for testing Transformers
     *
     * Copy of
     * https://github.com/apache/lucene-solr/blob/master/solr/contrib/dataimporthandler/src/test/org/apache/solr/handler/dataimport/AbstractDataImportHandlerTestCase.java#L185
     */
    @SuppressWarnings("unchecked")
    class TestContext extends Context {
        private final Map<String, String> entityAttrs;
        private final Context delegate;
        private final List<Map<String, String>> entityFields;
        private final boolean root;
        String script,scriptlang;

        public TestContext(Map<String, String> entityAttrs, Context delegate,
                           List<Map<String, String>> entityFields, boolean root) {
            this.entityAttrs = entityAttrs;
            this.delegate = delegate;
            this.entityFields = entityFields;
            this.root = root;
        }

        @Override
        public String getEntityAttribute(String name) {
            return entityAttrs == null ? delegate.getEntityAttribute(name) : entityAttrs.get(name);
        }

        @Override
        public String getResolvedEntityAttribute(String name) {
            return entityAttrs == null ? delegate.getResolvedEntityAttribute(name) :
                    delegate.getVariableResolver().replaceTokens(entityAttrs.get(name));
        }

        @Override
        public List<Map<String, String>> getAllEntityFields() {
            return entityFields == null ? delegate.getAllEntityFields()
                    : entityFields;
        }

        @Override
        public VariableResolver getVariableResolver() {
            return delegate.getVariableResolver();
        }

        @Override
        public DataSource getDataSource() {
            return delegate.getDataSource();
        }

        @Override
        public boolean isRootEntity() {
            return root;
        }

        @Override
        public String currentProcess() {
            return delegate.currentProcess();
        }

        @Override
        public Map<String, Object> getRequestParameters() {
            return delegate.getRequestParameters();
        }

        @Override
        public EntityProcessor getEntityProcessor() {
            return null;
        }

        @Override
        public void setSessionAttribute(String name, Object val, String scope) {
            delegate.setSessionAttribute(name, val, scope);
        }

        @Override
        public Object getSessionAttribute(String name, String scope) {
            return delegate.getSessionAttribute(name, scope);
        }

        @Override
        public Context getParentContext() {
            return delegate.getParentContext();
        }

        @Override
        public DataSource getDataSource(String name) {
            return delegate.getDataSource(name);
        }

        @Override
        public SolrCore getSolrCore() {
            return delegate.getSolrCore();
        }

        @Override
        public Map<String, Object> getStats() {
            return delegate.getStats();
        }


        @Override
        public String getScript() {
            return script == null ? delegate.getScript() : script;
        }

        @Override
        public String getScriptLanguage() {
            return scriptlang == null ? delegate.getScriptLanguage() : scriptlang;
        }

        @Override
        public void deleteDoc(String id) {

        }

        @Override
        public void deleteDocByQuery(String query) {

        }

        @Override
        public Object resolve(String var) {
            return delegate.resolve(var);
        }

        @Override
        public String replaceTokens(String template) {
            return delegate.replaceTokens(template);
        }
    }

    /**
     * Strings at even index are keys, odd-index strings are values in the
     * returned map
     *
     * Copy of
     * https://github.com/apache/lucene-solr/blob/master/solr/contrib/dataimporthandler/src/test/org/apache/solr/handler/dataimport/AbstractDataImportHandlerTestCase.java#L157
     */
    @SuppressWarnings("unchecked")
    public static Map createMap(Object... args) {
        return Utils.makeMap(args);
    }

    public String loadMarc21Record() throws IOException {
        byte[] bytes = Files.readAllBytes(Paths.get("src/test-files/record.mrc"));
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public String loadMarcXmlRecord() throws IOException {
        byte[] bytes = Files.readAllBytes(Paths.get("src/test-files/record.xml"));
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
