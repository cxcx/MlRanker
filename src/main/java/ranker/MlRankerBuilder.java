/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package ranker;

import model.ModelManage;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.search.rescore.RescorerBuilder;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Example rescorer that multiplies the score of the hit by some factor and doesn't resort them.
 */
public class MlRankerBuilder extends RescorerBuilder<MlRankerBuilder> {
    private static final Logger LOG = Loggers.getLogger(MlRankerBuilder.class, "Rescore");

    public static final String NAME = "MlRanker";

    private final String features;

    public MlRankerBuilder(@Nullable String features) {
        this.features = features;
    }

    public MlRankerBuilder(StreamInput in) throws IOException {
        super(in);
        features = in.readOptionalString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalString(features);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public RescorerBuilder<MlRankerBuilder> rewrite(QueryRewriteContext ctx) throws IOException {
        return this;
    }

    private static final ParseField FACTOR_FIELD = new ParseField("features");

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        if (features != null) {
            builder.field(FACTOR_FIELD.getPreferredName(), features);
        }
    }

    private static final ConstructingObjectParser<MlRankerBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME,
            args -> new MlRankerBuilder((String) args[0]));

    static {
        PARSER.declareString(optionalConstructorArg(), FACTOR_FIELD);
    }

    public static MlRankerBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public RescoreContext innerBuildContext(int windowSize, QueryShardContext context) throws IOException {
        LOG.info("get features:[" + this.features + "]");
        String[] featureFields = this.features.split("\\|");
        List<IndexFieldData<?>> fieldDataList = new ArrayList<>();
        for (String field : featureFields) {
            LOG.info("get filed data of field [" + field + "]");
            IndexFieldData<?> factorField = context.getForField(context.fieldMapper(field));
            fieldDataList.add(factorField);
        }

        return new MlRankerContext(windowSize, fieldDataList);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        MlRankerBuilder other = (MlRankerBuilder) obj;
        return Objects.equals(features, other.features);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), features);
    }


    @Nullable
    String getFeatures() {
        return features;
    }

    private static class MlRankerContext extends RescoreContext {
        @Nullable
        private final List<IndexFieldData<?>> featuresFields;

        MlRankerContext(int windowSize, List<IndexFieldData<?>> fields) {
            super(windowSize, MlRankerRescorer.INSTANCE);
            this.featuresFields = fields;
        }
    }

    private static class MlRankerRescorer implements Rescorer {
        private static final MlRankerRescorer INSTANCE = new MlRankerRescorer();

        @Override
        public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) throws IOException {
            MlRankerContext context = (MlRankerContext) rescoreContext;
            ModelManage.loadPmmlFile("lr.pmml");
            int end = Math.min(topDocs.scoreDocs.length, rescoreContext.getWindowSize());
            if (context.featuresFields != null && context.featuresFields.size() > 0) {
                /*
                 * Since this example looks up a single field value it should
                 * access them in docId order because that is the order in
                 * which they are stored on disk and we want reads to be
                 * forwards and close together if possible.
                 *
                 * If accessing multiple fields we'd be better off accessing
                 * them in (reader, field, docId) order because that is the
                 * order they are on disk.
                 */
                ScoreDoc[] sortedByDocId = new ScoreDoc[topDocs.scoreDocs.length];
                System.arraycopy(topDocs.scoreDocs, 0, sortedByDocId, 0, topDocs.scoreDocs.length);
                Arrays.sort(sortedByDocId, (a, b) -> a.doc - b.doc); // Safe because doc ids >= 0
                Iterator<LeafReaderContext> leaves = searcher.getIndexReader().leaves().iterator();
                LeafReaderContext leaf = null;
                SortedNumericDoubleValues data = null;
                int endDoc = 0;
                for (int i = 0; i < end; i++) {
                    if (topDocs.scoreDocs[i].doc >= endDoc) {
                        do {
                            leaf = leaves.next();
                            endDoc = leaf.docBase + leaf.reader().maxDoc();
                        } while (topDocs.scoreDocs[i].doc >= endDoc);
                    }

                    List<Double> inputs = new ArrayList<>();
                    for (IndexFieldData<?> field : context.featuresFields) {
                        AtomicFieldData fd = field.load(leaf);
                        if (!(fd instanceof AtomicNumericFieldData)) {
                            throw new IllegalArgumentException("document [" + topDocs.scoreDocs[i].doc
                                    + "] is not a number field [" + field.getFieldName() + "]");
                        }
                        SortedNumericDoubleValues dataValue = ((AtomicNumericFieldData) fd).getDoubleValues();
                        if (!dataValue.advanceExact(topDocs.scoreDocs[i].doc - leaf.docBase)) {
                            throw new IllegalArgumentException("document [" + topDocs.scoreDocs[i].doc
                                    + "] does not have the field [" + field.getFieldName() + "]");
                        }

                        if (dataValue.docValueCount() > 1) {
                            throw new IllegalArgumentException("document [" + topDocs.scoreDocs[i].doc
                                    + "] has more than one value for the field [" + field.getFieldName() + "]");
                        }
                        inputs.add(dataValue.nextValue());
                    }

                    topDocs.scoreDocs[i].score = (float) ModelManage.inference(inputs);
                    inputs = null;
                }


                // Sort by score descending, then docID ascending, just like lucene's QueryRescorer
                Arrays.sort(topDocs.scoreDocs, (a, b) -> {
                    if (a.score > b.score) {
                        return -1;
                    }
                    if (a.score < b.score) {
                        return 1;
                    }
                    // Safe because doc ids >= 0
                    return a.doc - b.doc;
                });
            }
            return topDocs;
        }

        @Override
        public Explanation explain(int i, IndexSearcher indexSearcher, RescoreContext rescoreContext, Explanation explanation) throws IOException {
            return null;
        }
    }
}