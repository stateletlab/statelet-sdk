// Copyright 2024 Statelet Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ai.statelet.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import statelet.v1.StateletProto.VectorSearchRequest;
import statelet.v1.StateletProto.VectorSearchResult;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

/**
 * Round-trip tests for result grouping / field-collapse (epic #1427, Phase 3):
 * the grouping kwargs marshal onto the proto request and group_key surfaces on
 * the result, surviving a wire serialize/parse cycle.
 */
class VectorGroupingTest {

    @Test
    void groupingKwargsRoundTripThroughProto() throws InvalidProtocolBufferException {
        VectorSearchRequest req = VectorSearchRequest.newBuilder()
                .setIndexName("idx")
                .setK(5)
                .setGroupField("doc_id")
                .setGroupSize(2)
                .setGroups(3)
                .setGroupOverfetch(4)
                .setGroupMissingAsOwn(true)
                .build();

        VectorSearchRequest parsed = VectorSearchRequest.parseFrom(req.toByteArray());
        assertEquals("doc_id", parsed.getGroupField());
        assertEquals(2, parsed.getGroupSize());
        assertEquals(3, parsed.getGroups());
        assertEquals(4, parsed.getGroupOverfetch());
        assertTrue(parsed.getGroupMissingAsOwn());
    }

    @Test
    void groupKeySurfacedOnResult() throws InvalidProtocolBufferException {
        VectorSearchResult r = VectorSearchResult.newBuilder()
                .setId(10)
                .setDistance(0.25f)
                .setGroupKey("docA")
                .build();
        VectorSearchResult parsed = VectorSearchResult.parseFrom(r.toByteArray());
        assertEquals("docA", parsed.getGroupKey());

        // Map onto the SDK record exactly as StateletClient.vectorSearchGrouped does.
        StateletClient.VectorResult mapped =
                new StateletClient.VectorResult(parsed.getId(), parsed.getDistance(), parsed.getGroupKey());
        assertEquals(10L, mapped.id());
        assertEquals("docA", mapped.groupKey());
    }

    @Test
    void groupSpecDefaultsAndBackCompatResult() {
        StateletClient.GroupSpec spec = new StateletClient.GroupSpec("doc_id");
        assertEquals("doc_id", spec.field());
        assertEquals(0, spec.groupSize());
        assertEquals(0, spec.groups());
        assertEquals(0, spec.overfetch());
        assertEquals(false, spec.missingAsOwn());

        // Ungrouped results default to an empty group key.
        StateletClient.VectorResult ungrouped = new StateletClient.VectorResult(1L, 0.1f);
        assertEquals("", ungrouped.groupKey());
    }
}
