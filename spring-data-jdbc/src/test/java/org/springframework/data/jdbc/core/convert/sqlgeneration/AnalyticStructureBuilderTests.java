/*
 * Copyright 2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.core.convert.sqlgeneration;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class AnalyticStructureBuilderTests {

	/**
	 * a simple table should result in a simple select.
	 */
	@Test
	void simpleTableWithColumns() {

		AnalyticStructureBuilder<String, Integer> builder = new AnalyticStructureBuilder<String, Integer>().addTable("table",
				td -> td.withId(0).withColumns(1, 2));

		assertThat(builder.getColumns()).extracting(c -> ((AnalyticStructureBuilder.BaseColumn) c).column)
				.containsExactlyInAnyOrder(0, 1, 2);
		assertThat(builder.getIdColumn()).extracting(c -> ((AnalyticStructureBuilder.BaseColumn) c).column)
				.isEqualTo(0);
	}


	@Test
	void tableWithSingleChild() {

		AnalyticStructureBuilder<String, Integer> builder =
				new AnalyticStructureBuilder<String, Integer>()
						.addTable("parent", td -> td.withId(0).withColumns(1, 2))
						.addChildTo("parent", "child", td -> td.withId(10).withColumns(11, 12))
				;

		assertThat(builder.getColumns()).extracting(c -> ((AnalyticStructureBuilder.BaseColumn) c).column)
				.containsExactlyInAnyOrder(0, 1, 2, 10, 11, 12);
		assertThat(builder.getIdColumn()).extracting(c -> ((AnalyticStructureBuilder.BaseColumn) c).column)
				.isEqualTo(0);
	}
}
