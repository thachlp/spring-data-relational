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

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

public class AnalyticStructureBuilderTests {

	/**
	 * A simple table should result in a simple select. Columns are represented by
	 * {@link org.springframework.data.jdbc.core.convert.sqlgeneration.AnalyticStructureBuilder.BaseColumn} since they are
	 * directly referenced.
	 */
	@Test
	void simpleTableWithColumns() {

		AnalyticStructureBuilder<String, Integer> builder = new AnalyticStructureBuilder<String, Integer>()
				.addTable("table", td -> td.withId(0).withColumns(1, 2));

		assertThat(builder.getColumns()).extracting(c -> ((AnalyticStructureBuilder.BaseColumn) c).column)
				.containsExactlyInAnyOrder(1, 2);
		assertThat(builder.getId()).extracting(c -> ((AnalyticStructureBuilder.BaseColumn) c).column).isEqualTo(0);
	}

	@Test
	void tableWithSingleChild() {

		AnalyticStructureBuilder<String, Integer> builder = new AnalyticStructureBuilder<String, Integer>()
				.addTable("parent", td -> td.withId(0).withColumns(1, 2))
				.addChildTo("parent", "child", td -> td.withId(10).withColumns(11, 12));

		AnalyticStructureBuilder.Select select = builder.getSelect();

		assertThat(select.getColumns()).extracting(c -> ((AnalyticStructureBuilder.DerivedColumn) c).getColumn())
				.containsExactlyInAnyOrder(1, 2, 11, 12);
		assertThat(select.getId()).extracting(c -> c.getColumn()).isEqualTo(0);
		assertThat(select.getFroms()).extracting(f -> f.getClass()).containsExactlyInAnyOrder(
				AnalyticStructureBuilder.TableDefinition.class, AnalyticStructureBuilder.AnalyticView.class);
	}

	@Test
	void tableWithMultipleChildren() {

		AnalyticStructureBuilder<String, Integer> builder = new AnalyticStructureBuilder<String, Integer>()
				.addTable("parent", td -> td.withId(0).withColumns(1, 2))
				.addChildTo("parent", "child1", td -> td.withId(10).withColumns(11, 12))
				.addChildTo("parent", "child2", td -> td.withId(20).withColumns(21, 22));

		AnalyticStructureBuilder.Select select = builder.getSelect();

		assertThat(select.getColumns()).extracting(c -> ((AnalyticStructureBuilder.DerivedColumn) c).getColumn())
				.containsExactlyInAnyOrder(1, 2, 11, 12, 21, 22);
		assertThat(select.getId()).extracting(c -> c.getColumn()).isEqualTo(0);
		assertThat(select.getFroms()).extracting(f -> f.getClass()).containsExactlyInAnyOrder(
				AnalyticStructureBuilder.AnalyticJoin.class, AnalyticStructureBuilder.AnalyticView.class);

		Set<AnalyticStructureBuilder<String, Integer>.TableDefinition> froms = collectFroms(select);
		assertThat(froms).extracting(td -> td.getTable()).containsExactlyInAnyOrder("parent", "child1", "child2");

	}

	private Set<AnalyticStructureBuilder<String, Integer>.TableDefinition> collectFroms(
			AnalyticStructureBuilder<String, Integer>.Select select) {

		Set<AnalyticStructureBuilder<String, Integer>.TableDefinition> froms = new HashSet<>();
		select.getFroms().forEach(s -> {
			if (s instanceof AnalyticStructureBuilder.AnalyticJoin) {
				froms.addAll(collectFroms(s));
			} else if (s instanceof AnalyticStructureBuilder.AnalyticView) {

				froms.add((AnalyticStructureBuilder.TableDefinition) s.getFroms().get(0));
			} else {
				froms.add((AnalyticStructureBuilder.TableDefinition) s);
			}
		});

		return froms;
	}
}
