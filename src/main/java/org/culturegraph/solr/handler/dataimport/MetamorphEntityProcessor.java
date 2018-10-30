package org.culturegraph.solr.handler.dataimport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DataImportHandlerException;
import org.apache.solr.handler.dataimport.EntityProcessorBase;
import org.culturegraph.plugin.io.ChunkReader;
import org.culturegraph.plugin.io.DecompressedInputStream;
import org.metafacture.metamorph.InlineMorph;
import org.metafacture.metamorph.Metamorph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

public class MetamorphEntityProcessor extends EntityProcessorBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private String url;
    private String inputFormat;
    private boolean addRecord;

    private MetamorphProcessor processor;

    private Reader reader;
    private Stream<String> recordStream;
    private Iterator<String> recordIter;

    // Preprocessing
    private Pattern startsWithWhitespace = Pattern.compile("^\\s+");
    private Pattern containsNewlineOrCarriageReturn = Pattern.compile("[\\n\\r]");

    /**
     * Parses each of the entity attributes.
     */
    @Override
    public void init(Context context) {
        super.init(context);

        // Init a file format for the records we want to load
        String inputFormat = context.getResolvedEntityAttribute(INPUT_FORMAT);
        if (inputFormat != null) {
            this.inputFormat = inputFormat;
        } else {
            throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
                    "'" + INPUT_FORMAT + "' is a required attribute");
        }

        // Init the metamorph definitions we want to use
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
                        InputStream morphDefInputStream = loader.openResource(morphDef);
                        metamorph = loadMetamorph(morphDefInputStream);
                    } catch (IOException e) {
                        String target = ((SolrResourceLoader) loader).resourceLocation(morphDef);
                        throw new DataImportHandlerException(DataImportHandlerException.SEVERE, "'" + target + "' not readable", e);
                    }
                } else {
                    metamorph = new Metamorph(morphDef);
                }
                metamorphList.add(metamorph);
            }
        } else {
            throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
                    "'" + MORPH_DEF + "' is a required attribute");
        }

        url = context.getResolvedEntityAttribute(DATASOURCE_URL);
        if (url == null) {
            throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
                    "'" + DATASOURCE_URL + "' is a required attribute");
        }

        // Append processed record to the row
        String includeFullRecord = context.getResolvedEntityAttribute(INCLUDE_FULL_RECORD);
        if (includeFullRecord != null) {
            addRecord = Boolean.parseBoolean(includeFullRecord);
        } else {
            addRecord = false;
        }

        if (processor == null) {
            if (inputFormat.equals("marc21") || inputFormat.equals("marc")) {
                processor = new MetamorphProcessor("marc21", metamorphList);
            } else {
                processor = new MetamorphProcessor(inputFormat, metamorphList);
            }
            processor.buildPipeline();
        }
    }

    private Metamorph loadMetamorph(InputStream inputStream) {
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        Iterator<String> iter = br.lines().filter(s -> !s.startsWith("<?")).iterator();
        InlineMorph inline = InlineMorph.in(this);
        while (iter.hasNext()) {
            inline = inline.with(iter.next());
        }
        return inline.create();
    }

    private Stream<String> createRecordStream(String format, Reader reader) {
        switch (format.toLowerCase()) {
            case "marc21":
                return new ChunkReader(reader, "\u001D").records();
            default:
                return new BufferedReader(reader).lines();
        }
    }

    /**
     * Reads records from a url.
     *
     * @return A row containing each literal from the metamorph transformation
     * and the complete record (if chosen).
     */
    @Override
    public Map<String, Object> nextRow() {

        if (reader == null) {
            Object data = context.getDataSource().getData(url);
            Reader dataSourceReader = null;

            if (data instanceof Reader) {
                dataSourceReader = (Reader) data;
            }

            if (data instanceof InputStream) {
                InputStream dataInputStream = (InputStream) data;

                try {
                    final Charset utf8 = StandardCharsets.UTF_8;
                    dataSourceReader = new InputStreamReader(DecompressedInputStream.of(dataInputStream), utf8);
                } catch (IOException e) {
                    throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
                            "Problem with data source " + "'" + url + "'" + ".");
                }
            }

            if (dataSourceReader == null) {
                throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
                        "Data source " + "'" + url + "'" + " is missing.");
            }



            reader = new BufferedReader(dataSourceReader);
            recordStream = createRecordStream(inputFormat, reader);
            recordIter = recordStream
                    .map(chunk -> startsWithWhitespace.matcher(chunk).replaceAll(""))
                    .map(chunk -> containsNewlineOrCarriageReturn.matcher(chunk).replaceAll(" "))
                    .iterator();
        }  // End of reader init



        // End of input
        if (!recordIter.hasNext()) {
            closeResources();
            return null;
        }

        Map<String, Object> row = null;
        while (recordIter.hasNext()) {
            String record = recordIter.next();

            // End of input
            if (record == null) {
                break;
            }

            if (record.isEmpty()) {
                continue;
            }

            try {
                row = processor.process(record);
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.debug("Skipping record '{}', got '{}'.", record, e);
                }

                if (onError.equals(SKIP)) {
                    continue;
                } else {
                    wrapAndThrow(DataImportHandlerException.SEVERE, e, "Unable to process record.");
                }
            }

            if (addRecord) {
                row.put("fullRecord", record);
            }

            break;
        }

        // End of input
        if (row == null) {
            closeResources();
            return null;
        }

        return row;
    }

    public void closeResources() {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException exp) {
                throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
                        "Problem closing input.", exp);
            }
        }
        reader = null;
        recordStream = null;
        recordIter = null;
    }

    @Override
    public void destroy() {
        closeResources();
        super.destroy();
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
    public static final String DATASOURCE_URL = "url";
}
