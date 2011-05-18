/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mahout.math.hadoop.similarity.vector;

import org.junit.Test;

/**
 * tests {@link DistributedEuclideanDistanceVectorSimilarity}
 */
public final class DistributedEuclideanDistanceVectorSimilarityTest extends
    DistributedVectorSimilarityTestCase {

  @Test
  public void testEuclideanDistance() throws Exception {

    assertSimilar(new DistributedEuclideanDistanceVectorSimilarity(),
        asVector(3, -2),
        asVector(3, -2), 2, 1.0);

    assertSimilar(new DistributedEuclideanDistanceVectorSimilarity(),
        asVector(3, 3),
        asVector(3, 3), 2, 1.0);

    assertSimilar(new DistributedEuclideanDistanceVectorSimilarity(),
        asVector(1, 2, 3),
        asVector(2, 5, 6), 3, 0.5598164905901122);

    assertSimilar(new DistributedEuclideanDistanceVectorSimilarity(),
        asVector(1, 0),
        asVector(0, 1), 2, 0.0);
  }
}
