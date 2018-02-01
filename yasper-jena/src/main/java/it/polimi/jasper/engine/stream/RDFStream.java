package it.polimi.jasper.engine.stream;

import it.polimi.jasper.engine.stream.items.RDFStreamSchema;
import it.polimi.rspql.Stream;
import it.polimi.yasper.core.stream.StreamSchema;
import lombok.Getter;
import lombok.NonNull;

/**
 * Created by riccardo on 10/07/2017.
 */
@Getter
public class RDFStream implements Stream {

    @NonNull
    private String stream_uri;
    @NonNull
    private RDFStreamSchema type;

    public RDFStream(String stream_uri, RDFStreamSchema type) {
        this.stream_uri = stream_uri;
        this.type = type;
    }

    @Override
    public StreamSchema getSchema() {
        return type;
    }

    @Override
    public String getTboxUri() {
        return null;
    }

    @Override
    public String getURI() {
        return stream_uri;
    }


}