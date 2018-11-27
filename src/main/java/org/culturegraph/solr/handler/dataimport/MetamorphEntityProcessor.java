package org.culturegraph.solr.handler.dataimport;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DataImportHandlerException;
import org.apache.solr.handler.dataimport.DataSource;
import org.apache.solr.handler.dataimport.EntityProcessorBase;
import org.culturegraph.plugin.io.ChunkReader;
import org.culturegraph.plugin.io.DecompressedInputStream;
import org.culturegraph.plugin.io.MarcConverter;
import org.culturegraph.plugin.metamorph.xml.LocalSystemEntityResolver;
import org.culturegraph.plugin.metamorph.xml.RemoteSystemEntityResolver;
import org.marc4j.MarcXmlReader;
import org.metafacture.metamorph.Metamorph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import javax.xml.stream.*;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

public class MetamorphEntityProcessor extends EntityProcessorBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /** Flag that marks a init that happens only once */
    private boolean once = false;
    /** Flag that marks the end of a data source */
    private boolean ended = false;

    /** Data source reader */
    private Reader reader;
    /** Name of the file format red by the reader */
    private String format;
    /** Iterator that iterates the reader */
    private Iterator<String> recordIterator;
    /** Pattern that is used to replace leading whitespace */
    private Pattern startsWithWhitespace = Pattern.compile("^\\s+");
    /** Pattern that is used to replace newline or carriage return */
    private Pattern containsNewlineOrCarriageReturn = Pattern.compile("[\\n\\r]");

    private MetamorphProcessor recordProcessor;
    private boolean includeFullRecord;

    @Override
    public void init(Context context) {
        super.init(context);

        if (!once) {
            String format = context.getResolvedEntityAttribute(INPUT_FORMAT);
            if (format != null) {
                this.format = format;
            } else {
                throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
                        "'" + INPUT_FORMAT + "' is a required attribute");
            }

            String includeFullRecord = context.getResolvedEntityAttribute(INCLUDE_FULL_RECORD);
            if (includeFullRecord != null) {
                this.includeFullRecord = Boolean.parseBoolean(includeFullRecord);
            } else {
                this.includeFullRecord = false;
            }


            List<Metamorph> metamorphList = new ArrayList<>();
            String morphDefs = context.getResolvedEntityAttribute(MORPH_DEF);
            if (morphDefs != null) {
                morphDefs = context.replaceTokens(morphDefs);
                String[] morphDefArr = morphDefs.split(",");
                for (int i = 0; i < morphDefArr.length; i++) {
                    String morphDef = morphDefArr[i];
                    Metamorph metamorph;

                    final SolrCore core = context.getSolrCore();
                    if (core != null) {
                        ResourceLoader loader = core.getResourceLoader();
                        try {
                            metamorph = loadMetamorph(loader, morphDef);
                        } catch (IOException e) {
                            String target = ((SolrResourceLoader) loader).resourceLocation(morphDef);
                            throw new DataImportHandlerException(DataImportHandlerException.SEVERE, "'" + target + "' not readable", e);
                        }
                    } else {
                        try {
                            metamorph = loadMetamorph(getClass().getClassLoader(), morphDef);
                        } catch (IOException e) {
                            throw new IllegalArgumentException("MorphDef '" + morphDef + "' not found.", e);
                        }
                    }
                    metamorphList.add(metamorph);
                }
            } else {
                throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
                        "'" + MORPH_DEF + "' is a required attribute");
            }

            if (recordProcessor == null) {
                /* MARCXML gets transformed to MARC21, so we need the processor for Marc21.
                 * See {@link org.culturegraph.solr.handler.dataimport.MetamorphEntityProcessor#createRecordStream }. */
                if (format.equalsIgnoreCase("marc21") ||format.equalsIgnoreCase("marc") || format.equalsIgnoreCase("marcxml")) {
                    recordProcessor = new MetamorphProcessor("marc21", metamorphList);
                } else {
                    recordProcessor = new MetamorphProcessor(format, metamorphList);
                }
                recordProcessor.buildPipeline();
            }

            once = true;
        }

        ended = false;
    }

    @Override
    public Map<String, Object> nextRow() {
        if (ended) return null;

        if (recordIterator == null) {
            String url = context.replaceTokens(context.getEntityAttribute(URL));
            DataSource ds = context.getDataSource();

            try {
                Object data = ds.getData(url);
                if (data instanceof  Reader) {
                    reader = (Reader) data;
                } else if (data instanceof InputStream) {
                    InputStream inputStream = (InputStream) data;
                    Charset utf8 = StandardCharsets.UTF_8;
                    reader = new InputStreamReader(DecompressedInputStream.of(inputStream), utf8);
                } else {
                    throw new DataImportHandlerException(SEVERE, "Could not create reader!");
                }
            } catch (Exception e) {
                wrapAndThrow(SEVERE, e, "Exception reading url : " + url);
            }

            recordIterator = createRecordStream(reader, format)
                    .map(chunk -> startsWithWhitespace.matcher(chunk).replaceAll(""))
                    .map(chunk -> containsNewlineOrCarriageReturn.matcher(chunk).replaceAll(" "))
                    .iterator();
        }

        Map<String, Object> row = null;
        while (recordIterator.hasNext()) {
            String record = recordIterator.next();
            try {
                row = createRow(record);
                break;
            } catch (Exception e) {
                if (!onError.equals(SKIP)) {
                    if (log.isDebugEnabled()) log.debug("Could not process '{}'. Error: {}.", record, e.getMessage());
                    wrapAndThrow(SEVERE, e, "Unable to process record.");
                }
            }
        }

        if (row == null) ended = true;

        if (!recordIterator.hasNext()) {
            ended = true;
            IOUtils.closeQuietly(reader);
            recordIterator = null;
        }

        return row;
    }

    private Map<String,Object> createRow(String record) {
        Map<String,Object> row = recordProcessor.process(record);
        if (includeFullRecord) {
            row.put("fullRecord", record);
        }
        return row;
    }

    private Metamorph loadMetamorph(ResourceLoader loader, String morphDefSystemID) throws IOException {
        XMLInputFactory inputFactory = XMLInputFactory.newInstance();
        inputFactory.setXMLResolver(new RemoteSystemEntityResolver(loader));

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try {
            XMLEventReader reader = inputFactory.createXMLEventReader(loader.openResource(morphDefSystemID));
            XMLEventWriter writer = XMLOutputFactory.newInstance().createXMLEventWriter(buf);
            writer.add(reader);
            writer.flush();
            writer.close();
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }

        String morphDefXmlWithResolvedSystemEntities = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        return new Metamorph(new StringReader(morphDefXmlWithResolvedSystemEntities));
    }

    private Metamorph loadMetamorph(ClassLoader loader, String morphDefSystemID) throws IOException {
        XMLInputFactory inputFactory = XMLInputFactory.newInstance();
        inputFactory.setXMLResolver(new LocalSystemEntityResolver(loader));

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try {
            XMLEventReader reader = inputFactory.createXMLEventReader(loader.getResourceAsStream(morphDefSystemID));
            XMLEventWriter writer = XMLOutputFactory.newInstance().createXMLEventWriter(buf);
            writer.add(reader);
            writer.flush();
            writer.close();
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }

        String morphDefXmlWithResolvedSystemEntities = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        return new Metamorph(new StringReader(morphDefXmlWithResolvedSystemEntities));
    }

    private Stream<String> createRecordStream(Reader reader, String format) {
        switch (format.toLowerCase()) {
            case "marc21":
                return new ChunkReader(reader, "\u001D").records();
            case "marcxml":
                MarcXmlReader marcXmlReader = new MarcXmlReader(new InputSource(reader));
                Iterator<String> rawMarcIterator = new MarcConverter(marcXmlReader);
                boolean isNotParallel = false;
                return StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(rawMarcIterator, Spliterator.ORDERED),
                        isNotParallel);
            default:
                return new BufferedReader(reader).lines();
        }
    }

    /**
     * Holds the name of entity attribute that will be parsed to indicate the
     * insertion of the full record to the output row.
     */
    public static final String INCLUDE_FULL_RECORD = "includeFullRecord";

    /**
     * Holds the name of entity attribute that will be parsed to obtain
     * the location of the metamorph definition file.
     */
    public static final String MORPH_DEF = "morphDef";

    /**
     * Holds the name of entity attribute that will be parsed to obtain
     * the format of the processed input.
     */
    public static final String INPUT_FORMAT = "inputFormat";

    /**
     * Holds the name of entity attribute that will be parsed to obtain
     * the filename.
     */
    public static final String URL = "url";
}
