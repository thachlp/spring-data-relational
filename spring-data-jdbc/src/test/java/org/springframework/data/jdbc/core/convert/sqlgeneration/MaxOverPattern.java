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

import java.util.Arrays;

record MaxOverPattern<C> (Pattern expression, Pattern ... partitionBy) implements Pattern {

	public static <C> MaxOverPattern<C> maxOver(C expression, Pattern ... partitionBy) {
		return new MaxOverPattern(expression instanceof Pattern expressionPattern ? expressionPattern : new BasePattern(expression), partitionBy);
	}

	@Override
	public boolean matches(AnalyticStructureBuilder<?, ?>.Select select,
			AnalyticStructureBuilder<?, ?>.AnalyticColumn actualColumn) {

		AnalyticStructureBuilder<?, ?>.MaxOver maxOver = extractMaxOver(actualColumn);
		if (maxOver == null)
			return false;

		if (!expression.matches(select, maxOver.expression)) return false;

		for (int i = 0; i < partitionBy.length; i++) {
			if (!partitionBy[i].matches(select, maxOver.partitionBy.get(i))) {
				return false;
			}
		}
		return true;
	}

	@Override
	public String render() {
		return "Max(%s) over (partition by %s)".formatted(expression.render(), Arrays.toString( partitionBy));
	}

	@Nullable
	private AnalyticStructureBuilder<?, ?>.MaxOver extractMaxOver(AnalyticStructureBuilder<?, ?>.AnalyticColumn other) {
		return AnalyticStructureBuilderSelectAssert.extract(AnalyticStructureBuilder.MaxOver.class, other);
	}

	@Override
	public String toString() {
		return render();
	}
}
