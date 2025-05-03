package com.doge.chat.server.user;

import java.util.HashSet;

import org.apache.commons.lang3.tuple.Pair;

// DotSet
// Each Dot = Pair<@ServerIdType, Clock>
public class DotSet extends HashSet<Pair<Integer, Integer>> {
    public DotSet() {
        super();
    }

    public DotSet(DotSet dotSet) {
        super(dotSet);
    }

    public void addDot(Pair<Integer, Integer> dot) {
        this.add(dot);
    }
}
