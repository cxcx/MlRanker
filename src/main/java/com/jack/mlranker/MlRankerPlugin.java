package com.jack.mlranker;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import ranker.MlRankerBuilder;

import java.util.Collections;
import java.util.List;

public class MlRankerPlugin extends Plugin implements SearchPlugin {

    @Override
    public List<RescorerSpec<?>> getRescorers() {
        return Collections.singletonList(new RescorerSpec<>(MlRankerBuilder.NAME, MlRankerBuilder::new, MlRankerBuilder::fromXContent));
    }
}
