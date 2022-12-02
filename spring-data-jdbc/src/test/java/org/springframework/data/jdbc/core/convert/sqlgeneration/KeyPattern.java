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

record KeyPattern<T, C> (C name) implements Pattern {

	public static <T, C> KeyPattern<T, C> key(C column) {
		return new KeyPattern<>(column);
	}

	@Override
	public boolean matches(AnalyticStructureBuilder<?, ?>.Select select,
			AnalyticStructureBuilder<?, ?>.AnalyticColumn actualColumn) {

		AnalyticStructureBuilder.KeyColumn keyColumn = extractKey(actualColumn);
		if (keyColumn == null) {
			return false;
		}

		Object column = keyColumn.getColumn();

		return name.equals(column);
	}

	@Override
	public String render() {
		return toString();
	}

	@Nullable
	static AnalyticStructureBuilder.KeyColumn extractKey(
			AnalyticStructureBuilder<?, ?>.AnalyticColumn column) {
		return AnalyticStructureBuilderSelectAssert.extract(AnalyticStructureBuilder.KeyColumn.class, column);
	}

	@Override
	public String toString() {

		return "KEY(" + name + ")";

	}
}
