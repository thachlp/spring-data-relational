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

record CoalescePattern<C> (Pattern left, Pattern right) implements Pattern {

	public static <C> CoalescePattern<C> coalesce(C left, Pattern right) {
		return new CoalescePattern(left instanceof Pattern leftPattern ? leftPattern : new BasePattern(left), right);
	}

	@Override
	public boolean matches(AnalyticStructureBuilder<?, ?>.Select select,
			AnalyticStructureBuilder<?, ?>.AnalyticColumn actualColumn) {

		AnalyticStructureBuilder<?, ?>.Coalesce coalesce = extractCoalesce(actualColumn);
		return coalesce != null && left.matches(select, coalesce.left) && right.matches(select, coalesce.right);
	}

	@Override
	public String render() {
		return "Coalesce(%s, %s)".formatted(left.render(), right.render());
	}

	@Nullable
	private AnalyticStructureBuilder.Coalesce extractCoalesce(AnalyticStructureBuilder<?, ?>.AnalyticColumn other) {
		return AnalyticStructureBuilderSelectAssert.extract(AnalyticStructureBuilder.Coalesce.class, other);
	}

	@Override
	public String toString() {
		return "Coalesce(" + left + ", " + right + ')';
	}
}
