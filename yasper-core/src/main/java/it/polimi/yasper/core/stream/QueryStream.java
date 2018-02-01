package it.polimi.yasper.core.stream;

import com.espertech.esper.client.EPStatement;
import it.polimi.rspql.RSPEngine;
import it.polimi.rspql.querying.ContinuousQuery;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * Created by riccardo on 10/07/2017.
 */

//TODO InstantaneousResponse
public class QueryStream extends StreamImpl {

    protected RSPEngine e;
    @NonNull
    protected String query_id;
    @Setter
    @Getter
    protected EPStatement streamStatemnt;
    protected ContinuousQuery q;

    public QueryStream(RSPEngine e, String stream_uri, EPStatement streamStatemnt) {
        super(stream_uri);
        this.streamStatemnt = streamStatemnt;
        this.e = e;
        this.query_id = stream_uri;
        //TODO consider define construct clause as a stream schema
    }

    //TODO define a decoration pattern that allow to speak with the RSP engine
    public void setRSPEngine(RSPEngine e) {
        this.e = e;
    }

    public RSPEngine getRSPEngine() {
        return e;
    }


    @Override
    public String getURI() {
        return query_id;
    }


}