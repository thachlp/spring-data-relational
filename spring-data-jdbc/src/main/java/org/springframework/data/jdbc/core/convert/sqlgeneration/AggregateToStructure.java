/*
 * Copyright 2023 the original author or authors.
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

import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

public class AggregateToStructure {

	private final RelationalMappingContext context;

	public AggregateToStructure(RelationalMappingContext context) {
		this.context = context;
	}

	AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select createSelectStructure(
			RelationalPersistentEntity<?> aggregateRoot) {

		PersistentPropertyPathExtension rootPath = new PersistentPropertyPathExtension(context, aggregateRoot);

		AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension> builder = new AnalyticStructureBuilder<>();

		builder.addTable(aggregateRoot, td -> configureTableDefinition(rootPath, td));
		addReferencedEntities(builder, rootPath);

		return builder.build().getSelect();
	}

	private void addReferencedEntities(
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension> builder,
			PersistentPropertyPathExtension currentPath) {

		RelationalPersistentEntity<?> leafEntity = currentPath.getLeafEntity();

		leafEntity.doWithProperties((PropertyHandler<RelationalPersistentProperty>) p -> {
			if (p.isEntity()) {
				RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(p.getActualType());
				builder.addChildTo(p.getOwner(), currentPath.extendBy(p), entity,
						td2 -> configureTableDefinition(currentPath.extendBy(p), td2));
			}
		});
	}

	private AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.TableDefinition configureTableDefinition(
			PersistentPropertyPathExtension path,
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.TableDefinition td) {

		path.getLeafEntity().doWithProperties((PropertyHandler<RelationalPersistentProperty>) p -> {
			PersistentPropertyPathExtension propertyPath = path.extendBy((RelationalPersistentProperty) p);

			if (p.isIdProperty()) {
				td.withId(propertyPath);
			} else {
				if (!p.isEntity()) {
					td.withColumns(propertyPath);
				}
			}

		});

		if (path.isQualified()) {
			td.withKeyColumn(path);
		}

		return td;
	}
}
