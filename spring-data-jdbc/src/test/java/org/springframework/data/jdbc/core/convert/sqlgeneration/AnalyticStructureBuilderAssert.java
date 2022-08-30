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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.error.BasicErrorMessageFactory;
import org.assertj.core.internal.Failures;
import org.assertj.core.internal.StandardComparisonStrategy;

import static org.assertj.core.api.Assertions.*;

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

	AnalyticStructureBuilderAssert<T, C> hasExactColumns(Object... expected) {

		Pattern[] patterns = new Pattern[expected.length];
		for (int i = 0; i < expected.length; i++) {

			Object object = expected[i];
			patterns[i] = object instanceof Pattern ? (Pattern) object : new BasePattern(object);
		}

		return containsPatternsExcactly(patterns);
	}

	AnalyticStructureBuilderAssert<T, C> hasId(C name) {

		assertThat(actual).matches(a -> new BasePattern(name).matches(a.getId()),"has Id " + name + ", but was " + actual.getId());
		return this;
	}

	AnalyticStructureBuilderAssert<T, C> containsPatternsExcactly(Pattern... patterns) {

		List<? extends AnalyticStructureBuilder.AnalyticColumn> availableColumns = actual.getSelect().getColumns();

		List<Pattern> notFound = new ArrayList<>();
		for (Pattern pattern : patterns) {

			boolean found = false;
			for (AnalyticStructureBuilder.AnalyticColumn actualColumn : availableColumns) {

				if (pattern.matches(actualColumn)) {
					found = true;
					availableColumns.remove(actualColumn);
					break;
				}
			}

			if (!found) {
				notFound.add(pattern);
			}
		}

		if (notFound.isEmpty() && availableColumns.isEmpty()) {
			return this;
		}

		throw Failures.instance().failure(info, ColumnsShouldContainExactly
				.columnsShouldContainExactly(actual.getSelect().getColumns(), patterns, notFound, availableColumns));
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

	private static class ColumnsShouldContainExactly extends BasicErrorMessageFactory {

		static ColumnsShouldContainExactly columnsShouldContainExactly(
				List<? extends AnalyticStructureBuilder.AnalyticColumn> actualColumns, Pattern[] expected,
				List<Pattern> notFound, List<? extends AnalyticStructureBuilder.AnalyticColumn> notExpected) {
			String actualColumnsDescription = actualColumns.stream().map(ColumnsShouldContainExactly::toString)
					.collect(Collectors.joining(", "));

			String expectedDescription = Arrays.stream(expected).map(Pattern::render).collect(Collectors.joining(", "));

			if (notFound.isEmpty()) {
				return new ColumnsShouldContainExactly(actualColumnsDescription, expectedDescription, notExpected);
			}
			if (notExpected.isEmpty()) {
				return new ColumnsShouldContainExactly(actualColumnsDescription, expectedDescription, notFound);
			}
			return new ColumnsShouldContainExactly(actualColumnsDescription, expectedDescription, notFound, notExpected);
		}

		private ColumnsShouldContainExactly(String actualColumns, String expected, Object notFound,
				List<? extends AnalyticStructureBuilder.AnalyticColumn> notExpected) {
			super("""

					Expecting:
					  <%s>
					  to contain exactly <%s>.
					  But <%s> were not found,
					  and <%s> where not expected.""", actualColumns, expected, notFound, notExpected,
					StandardComparisonStrategy.instance());
		}

		private ColumnsShouldContainExactly(String actualColumns, String expected,
				List<? extends AnalyticStructureBuilder.AnalyticColumn> notExpected) {
			super("""

					Expecting:
					  <%s>
					  to contain exactly <%s>.
					  But <%s> where not expected.""", actualColumns, expected, notExpected,
					StandardComparisonStrategy.instance());
		}

		private ColumnsShouldContainExactly(String actualColumns, String expected, Object notFound) {
			super("""

					Expecting:
					  <%s>
					  to contain exactly <%s>.
					  But <%s> were not found.""", actualColumns, expected, notFound, StandardComparisonStrategy.instance());
		}

		private static String toString(Object c) {

			if (c instanceof AnalyticStructureBuilder.BaseColumn bc) {
				return bc.getColumn().toString();
			} else if (c instanceof AnalyticStructureBuilder.DerivedColumn dc) {
				return extractColumn(dc.getBase()).toString();
			}

			throw new IllegalStateException("Column " + c + " is neither a BaseColumn nor a Derived one");
		}
	}
}
