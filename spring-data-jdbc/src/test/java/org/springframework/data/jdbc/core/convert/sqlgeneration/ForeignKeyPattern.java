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

import java.util.Objects;

import org.jetbrains.annotations.NotNull;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

record ForeignKeyPattern<T, C> (T table, C name) implements Pattern {

	public static <C> ForeignKeyPattern<?, C> fk(C name) {
		return new ForeignKeyPattern<>(null, name);
	}

	public static <T, C> ForeignKeyPattern<T, C> fk(T table, C column) {
		return new ForeignKeyPattern<>(table, column);
	}

	@Override
	public boolean matches(AnalyticStructureBuilder<?, ?>.Select select,
			AnalyticStructureBuilder<?, ?>.AnalyticColumn actualColumn) {

		AnalyticStructureBuilder<?, ?>.ForeignKey foreignKey = extractForeignKey(actualColumn);
		if (foreignKey == null || !name.equals(foreignKey.getColumn())) {
			return false;
		}

		if (table == null) {
			return true;
		}

		AnalyticStructureBuilder<?, ?>.Select fkOwner = foreignKeyOwner(select, foreignKey);

		Assert.notNull(fkOwner, "the Foreign Key Owner should never be null");

		return unwrapParent(fkOwner).equals(table);
	}

	@NotNull
	private Object unwrapParent(AnalyticStructureBuilder<?, ?>.Select select) {

		Object parent = select.getParent();
		if (parent instanceof AnalyticStructureBuilder.Select parentSelect) {
			return unwrapParent(parentSelect);
		}
		return parent;
	}

	@Nullable
	private AnalyticStructureBuilder<?, ?>.Select foreignKeyOwner(AnalyticStructureBuilder<?, ?>.Select select,
			AnalyticStructureBuilder<?, ?>.ForeignKey foreignKey) {

		if (select instanceof AnalyticStructureBuilder<?, ?>.TableDefinition td) {
			if (td.getColumns().contains(foreignKey)) {
				return td;
			}
		} else if (select instanceof AnalyticStructureBuilder<?, ?>.AnalyticView av) {
			if (av.getForeignKey().equals(foreignKey)) {
				return av;
			}
		}

		return select.getFroms().stream() //
				.map(s -> foreignKeyOwner(s, foreignKey)) //
				.filter(Objects::nonNull) //
				.findFirst() //
				.orElse(null);
	}

	@Override
	public String render() {
		return "FK(" + name + ")";
	}

	@Nullable
	static AnalyticStructureBuilder<?, ?>.ForeignKey extractForeignKey(
			AnalyticStructureBuilder<?, ?>.AnalyticColumn column) {
		return AnalyticStructureBuilderAssert.extract(AnalyticStructureBuilder.ForeignKey.class, column);
	}

	@Override
	public String toString() {

		if (table == null) {
			return "FK(" + name + ")";
		}
		return "FK(" + table + ", " + name + ")";

	}
}
