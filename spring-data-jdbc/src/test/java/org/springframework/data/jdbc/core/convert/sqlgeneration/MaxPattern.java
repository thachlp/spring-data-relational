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

record MaxPattern<C>(Pattern left, Pattern right) implements Pattern{

	public static <C> MaxPattern<C> max(C left, Pattern right){
		return new MaxPattern(new BasePattern(left), right);
	}

	@Override
	public boolean matches(AnalyticStructureBuilder.AnalyticColumn other) {
		final AnalyticStructureBuilder.Max max = extractMax(other);
		return max != null && left.matches(max.left) && right.matches(max.right);
	}

	@Override
	public String render() {
		return "MAX(%s, %s)".formatted(left.render(), right.render());
	}

	@Nullable
	static AnalyticStructureBuilder.Max extractMax(AnalyticStructureBuilder.AnalyticColumn column) {
		if (column instanceof AnalyticStructureBuilder.Max max) {
			return max;
		}

		if (column instanceof AnalyticStructureBuilder.DerivedColumn derivedColumn) {
			return extractMax(derivedColumn.getBase());
		}
		return null;
	}

}
