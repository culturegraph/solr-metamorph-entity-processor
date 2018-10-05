package org.culturegraph.solr.handler.dataimport;

import org.culturegraph.plugin.metamorph.RowCollector;
import org.metafacture.biblio.marc21.Marc21Decoder;
import org.metafacture.biblio.pica.PicaDecoder;
import org.metafacture.framework.StreamPipe;
import org.metafacture.framework.StreamReceiver;
import org.metafacture.framework.helpers.DefaultObjectPipe;
import org.metafacture.framework.helpers.ForwardingStreamPipe;
import org.metafacture.metamorph.Metamorph;

import java.util.List;
import java.util.Map;

public class MetamorphProcessor {
    private String format;
    private List<Metamorph> metamorphList;
    private DefaultObjectPipe<String, StreamReceiver> decoder;
    private RowCollector collector;

    public MetamorphProcessor(String format, List<Metamorph> metamorphList) {
        this.format = format;
        this.metamorphList = metamorphList;
        this.collector = new RowCollector();
    }

    public void buildPipeline() {
        decoder = createDecoder(format);

        if (metamorphList.isEmpty()) {
            decoder.setReceiver(collector);
        } else if (metamorphList.size() == 1) {
            Metamorph metamorph = metamorphList.get(0);
            decoder.setReceiver(metamorph).setReceiver(collector);
        } else {
            StreamPipe<StreamReceiver> receiver = decoder.setReceiver(new ForwardingStreamPipe());

            for (Metamorph metamorph: metamorphList) {
                receiver = receiver.setReceiver(metamorph);
            }

            receiver.setReceiver(collector);
        }
    }

    public Map<String,Object> process(String record) {
        decoder.resetStream();
        decoder.process(record);
        decoder.closeStream();
        return collector.getRow();
    }

    private DefaultObjectPipe<String, StreamReceiver> createDecoder(String format) {
        switch (format.toLowerCase()) {
            case "marc21":
                return new Marc21Decoder();
            case "pica":
                return new PicaDecoder();
            default:
                throw new IllegalArgumentException("Unknown format " + format + "!");
        }
    }
}
