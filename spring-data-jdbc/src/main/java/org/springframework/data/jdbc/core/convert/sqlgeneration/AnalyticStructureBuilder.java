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

import org.springframework.lang.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Builds the structure of an analytic query. The structure contains arbitrary objects for tables and columns
 */
class AnalyticStructureBuilder<T, C> {

	private TableDefinition table;

	AnalyticStructureBuilder addTable(T table, Function<TableDefinition, TableDefinition> tableDefinitionConfiguration) {
		this.table = tableDefinitionConfiguration.apply( new TableDefinition(table));
		return this;
	}

	List<? extends AnalyticColumn> getColummns() {

		ArrayList<AnalyticColumn> result = new ArrayList<>(table.columns);
		result.add(table.id);
		return result;
	}

	AnalyticColumn getIdColumn() {
		return table.id;
	}


	class TableDefinition {

		private final T table;
		private final AnalyticColumn id;
		private final List<? extends AnalyticColumn> columns;

		TableDefinition(T table, @Nullable AnalyticColumn id, List<? extends AnalyticColumn> columns) {

			this.table = table;
			this.id = id;
			this.columns = columns;
		}

		public TableDefinition(T table) {
			this(table, null, Collections.emptyList());
		}

		TableDefinition withId(C id) {
			return new TableDefinition(table, new BaseColumn(id), columns);
		}

		TableDefinition withColumns(C... columns) {

			return new TableDefinition(table, id, Arrays.stream(columns).map(BaseColumn::new).toList());
		}
	}

	interface AnalyticColumn {}

	class BaseColumn implements AnalyticColumn{
		final C column;
		BaseColumn(C column) {
			this.column = column;
		}

	}
}
