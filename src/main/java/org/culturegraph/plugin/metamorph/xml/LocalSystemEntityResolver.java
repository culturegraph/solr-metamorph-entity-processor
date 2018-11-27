package org.culturegraph.plugin.metamorph.xml;

import javax.xml.stream.XMLResolver;

public class LocalSystemEntityResolver implements XMLResolver {

    private ClassLoader loader;
    public LocalSystemEntityResolver(ClassLoader loader) {
        this.loader = loader;
    }

    @Override
    public Object resolveEntity(String publicID, String systemID, String baseURI, String namespace)
    {
        try {
            return loader.getResourceAsStream(systemID);
        } catch (Exception e) {
            return null;
        }
    }
}
