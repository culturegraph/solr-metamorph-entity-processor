package com.github.eberhardtj.solr.handler.dataimport;

import com.github.eberhardtj.io.DecompressedInputStream;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DataImportHandlerException;
import org.apache.solr.handler.dataimport.EntityProcessorBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MetamorphEntityProcessor extends EntityProcessorBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private String url;
    private String inputFormat;
    private boolean addRecord;

    private MetamorphProcessor processor;

    private Reader reader;
    private RecordReader recordReader;

    /**
     * Parses each of the entity attributes.
     */
    @Override
    public void init(Context context) {
        super.init(context);

        // init a file format for the records we want to read
        String inputFormat = context.getResolvedEntityAttribute(INPUT_FORMAT);
        if (inputFormat != null) {
            this.inputFormat = inputFormat;
        } else {
            throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
                    "'" + INPUT_FORMAT + "' is a required attribute");
        }

        // init a metamorph definitions we want to use
        List<String> morphDefList;
        String morphDefs = context.getResolvedEntityAttribute(MORPH_DEF);
        if (morphDefs != null) {
            morphDefs = context.replaceTokens(morphDefs);
            String[] elements = morphDefs.split(",");
            morphDefList = Arrays.stream(elements).collect(Collectors.toList());
        } else {
            throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
                    "'" + MORPH_DEF + "' is a required attribute");
        }

        url = context.getResolvedEntityAttribute(URL);
        if (url == null) {
            throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
                    "'" + URL + "' is a required attribute");
        }

        // append processed record to the row
        String includeFullRecord = context.getResolvedEntityAttribute(INCLUDE_FULL_RECORD);
        if (includeFullRecord != null) {
            addRecord = Boolean.parseBoolean(includeFullRecord);
        } else {
            addRecord = false;
        }

        processor = new MetamorphProcessor(inputFormat, morphDefList);
        processor.buildPipeline();
    }

    private RecordReader createRecordReader(String format, Reader reader) {
        switch (format.toLowerCase()) {
            case "marc21":
                return new RawRecordReader(reader);
            default:
                return (RecordReader) new BufferedReader(reader).lines().iterator();
        }
    }

    /**
     * Reads records from the url.
     *
     * @return A row containing each literal from the metamorph transformation
     * and the complete record (if chosen).
     */
    @Override
    public Map<String, Object> nextRow() {

        if (recordReader == null) {
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
            recordReader = createRecordReader(inputFormat, reader);
        }

        if (!recordReader.hasNext()) {
            closeResources();
            return null;
        }

        String record = recordReader.next();

        log.info("Record: {}", record);

        if (record == null) {
            closeResources();
            return null;
        }

        if (record.isEmpty()) {
            throw new DataImportHandlerException(DataImportHandlerException.SEVERE, "Problem empty record detected.");
        }

        Map<String, Object> row = processor.process(record);
        if (addRecord) {
            row.put("fullRecord", record);
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
    public static final String URL = "url";
}
