package datawave.webservice.result;

import java.io.Serializable;
import java.util.List;

import datawave.webservice.metadata.MetadataFieldBase;
import io.protostuff.Message;

public abstract class MetadataQueryResponseBase<T> extends BaseQueryResponse implements Serializable, TotalResultsAware, Message<T> {
    private static final long serialVersionUID = 1937054629579713080L;

    public abstract List<MetadataFieldBase> getFields();

    public abstract void setTotalResults(long totalResults);

    public abstract long getTotalResults();

}
