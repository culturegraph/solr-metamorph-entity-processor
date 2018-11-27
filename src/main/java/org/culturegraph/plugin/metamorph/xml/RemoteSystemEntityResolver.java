package org.culturegraph.plugin.metamorph.xml;

import org.apache.lucene.analysis.util.ResourceLoader;

import javax.xml.stream.XMLResolver;

public class RemoteSystemEntityResolver implements XMLResolver {

    private ResourceLoader loader;
    public RemoteSystemEntityResolver(ResourceLoader loader) {
        this.loader = loader;
    }

    @Override
    public Object resolveEntity(String publicID, String systemID, String baseURI, String namespace)
    {
        try {
            return loader.openResource(systemID);
        } catch (Exception e) {
            return null;
        }
    }
}
