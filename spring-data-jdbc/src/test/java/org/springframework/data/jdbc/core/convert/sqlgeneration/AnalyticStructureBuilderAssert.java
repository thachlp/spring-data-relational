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

import java.util.List;
import java.util.stream.Collectors;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.error.BasicErrorMessageFactory;
import org.assertj.core.internal.Failures;
import org.assertj.core.internal.StandardComparisonStrategy;

/**
 * Assertions for {@link AnalyticStructureBuilder}.
 * 
 * @param <T>
 * @param <C>
 */
public class AnalyticStructureBuilderAssert<T, C>
		extends AbstractAssert<AnalyticStructureBuilderAssert<T, C>, AnalyticStructureBuilder<T, C>> {

	AnalyticStructureBuilderAssert(AnalyticStructureBuilder<T, C> actual) {
		super(actual, AnalyticStructureBuilderAssert.class);
	}

	ColumnsAssert<T, C> columns() {
		return new ColumnsAssert<>(actual);
	}

	static class ColumnsAssert<T, C> extends AbstractAssert<ColumnsAssert<T, C>, AnalyticStructureBuilder<T, C>> {

		ColumnsAssert(AnalyticStructureBuilder<T, C> actual) {
			super(actual, ColumnsAssert.class);
		}

		ColumnsAssert<T, C> containsColumns(Object... expected) {

			Pattern[] patterns = new Pattern[expected.length];
			for (int i = 0; i < expected.length; i++) {

				Object object = expected[i];
				patterns[i] = object instanceof Pattern ? (Pattern) object : new BasePattern(object);
			}

			return containsPatterns(patterns);
		}

		ColumnsAssert<T, C> containsPatterns(Pattern... patterns) {

			List<? extends AnalyticStructureBuilder.AnalyticColumn> actualColumns = actual.getSelect().getColumns();

			for (Pattern pattern : patterns) {

				boolean found = false;
				for (AnalyticStructureBuilder.AnalyticColumn actualColumn : actualColumns) {

					if (pattern.matches(actualColumn)) {
						found = true;
						break;
					}
				}

				if (!found) {
					String actualColumnsDescription = actualColumns.stream().map(this::toString)
							.collect(Collectors.joining(", "));
					throw Failures.instance().failure(info, new ColumnsShouldContain(pattern, actualColumnsDescription));
				}
			}

			return this;
		}

		private String toString(Object c) {

			if (c instanceof AnalyticStructureBuilder.BaseColumn bc) {
				return bc.getColumn() + " (base column)";
			} else if (c instanceof AnalyticStructureBuilder.DerivedColumn dc) {
				return (C) extractColumn(dc.getBase()) + " (derived column)";
			}

			throw new IllegalStateException("Column " + c + " is neither a BaseColumn nor a Derived one");
		}

	}

	static Object extractColumn(Object c) {

		if (c instanceof AnalyticStructureBuilder.BaseColumn bc) {
			return bc.getColumn();
		} else if (c instanceof AnalyticStructureBuilder.DerivedColumn dc) {
			return extractColumn(dc.getBase());
		} else if (c instanceof AnalyticStructureBuilder.RowNumber rn) {
			return "RN";
		} else if (c instanceof AnalyticStructureBuilder.ForeignKey fk) {
			return "FK(" + fk.getColumn() + ")";
		} else if (c instanceof AnalyticStructureBuilder.Max max) {
			return "MAX(" + extractColumn(max.getLeft()) + ", " + extractColumn(max.getRight()) + ")";
		} else {
			return "unknown";
		}
	}

	private static class ColumnsShouldContain extends BasicErrorMessageFactory {

		private ColumnsShouldContain(Object expected, Object actual) {
			super("\n" + //
					"Expecting:\n" + //
					"  Column <%s>\n" + //
					"to exist. But was not found in\n" + //
					"  <%s>", expected, actual, StandardComparisonStrategy.instance());
		}
	}

}
