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

import static java.util.Arrays.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Builds the structure of an analytic query. The structure contains arbitrary objects for tables and columns. There are
 * two kinds of parent child relationship:
 * <ol>
 * <li>there is the relationship on aggregate level: the purchase order is the parent of the line item. This
 * relationship is denoted by simply parent and child.</li>
 * <li>there is the parent child relationship inside the analytic query structure, that gets build by this builder.
 * Where a join combines two nodes. In this relationship the join is parent to the two nodes, where one node might
 * represent the purchase order and the other the line item. This kind or relationship shall be prefixed by "node". The
 * join {@literal nodeParent} is the {@literal nodeParent} of purchase order and line item.</li>
 * </ol>
 */
class AnalyticStructureBuilder<T, C> {

	private Select nodeRoot;
	private Select aggregateRootTable;
	private Map<Object, Select> nodeParentLookUp = new HashMap<>();

	AnalyticStructureBuilder<T, C> addTable(T table,
			Function<TableDefinition, TableDefinition> tableDefinitionConfiguration) {

		this.nodeRoot = createTable(table, tableDefinitionConfiguration);

		this.aggregateRootTable = this.nodeRoot;

		return this;
	}

	AnalyticStructureBuilder<T, C> addChildTo(T parent, T child,
			Function<TableDefinition, TableDefinition> tableDefinitionConfiguration) {

		Select nodeParent = findUltimateNodeParent(parent);

		List<Select> nodeParentChain = collectNodeParents(nodeParent);

		AnalyticJoin newNode = new AnalyticJoin(nodeParent, createTable(child, tableDefinitionConfiguration));

		this.nodeRoot = replace(newNode, nodeParentChain);

		return this;
	}

	private List<Select> collectNodeParents(Select node) {

		List<Select> result = new ArrayList<>();
		Select nodeParent = nodeParentLookUp.get(node);
		while (nodeParent != null) {
			result.add(nodeParent);
			nodeParent = nodeParentLookUp.get(nodeParent);
		}
		return result;
	}

	private Select replace(Select newNode, List<Select> nodes) {

		for (int i = nodes.size() - 1; i >= 0; i--) {
			Select oldNode = nodes.get(i);

			newNode = new AnalyticJoin((Select) oldNode.getParent(), newNode);
		}

		return newNode;
	}

	List<? extends AnalyticColumn> getColumns() {
		return nodeRoot.getColumns();
	}

	AnalyticColumn getId() {
		return nodeRoot.getId();
	}

	private Select findUltimateNodeParent(T parent) {

		Select nodeParent = (Select) nodeParentLookUp.get(parent);

		Assert.state(nodeParent != null, "There must be a node parent");
		Assert.state(nodeParent.getParent().equals(parent), "The object in question must be the parent of the node parent");

		return findUltimateNodeParent(nodeParent);
	}

	private Select findUltimateNodeParent(Select node) {

		Select nodeParent = (Select) nodeParentLookUp.get(node);

		if (nodeParent == null) {
			return node;
		} else if (!nodeParent.getParent().equals(node)) {
			return node;
		} else {
			return findUltimateNodeParent(nodeParent);
		}
	}

	private TableDefinition createTable(T table,
			Function<TableDefinition, TableDefinition> tableDefinitionConfiguration) {
		return tableDefinitionConfiguration.apply(new TableDefinition(table));
	}

	Select getSelect() {
		return nodeRoot;
	}

	abstract class Select {

		abstract List<? extends AnalyticColumn> getColumns();

		abstract AnalyticColumn getId();

		abstract List<Select> getFroms();

		abstract Object getParent();
	}

	class TableDefinition extends Select {

		private final T table;
		private final AnalyticColumn id;
		private final List<? extends AnalyticColumn> columns;

		TableDefinition(T table, @Nullable AnalyticColumn id, List<? extends AnalyticColumn> columns) {

			this.table = table;
			this.id = id;
			this.columns = Collections.unmodifiableList(columns);

			nodeParentLookUp.put(table, this);
		}

		TableDefinition(T table) {

			this(table, null, Collections.emptyList());

		}

		TableDefinition withId(C id) {
			return new TableDefinition(table, new BaseColumn(id), columns);
		}

		TableDefinition withColumns(C... columns) {

			return new TableDefinition(table, id, Arrays.stream(columns).map(BaseColumn::new).toList());
		}

		@Override
		public List<? extends AnalyticColumn> getColumns() {

			return columns;
		}

		@Override
		public AnalyticColumn getId() {
			return id;
		}

		@Override
		List<Select> getFroms() {
			return Collections.emptyList();
		}

		@Override
		Object getParent() {
			return table;
		}

		T getTable() {
			return table;
		}
	}

	abstract class AnalyticColumn {
		abstract C getColumn();
	}

	class BaseColumn extends AnalyticColumn {

		final C column;

		BaseColumn(C column) {
			this.column = column;
		}

		@Override
		C getColumn() {
			return column;
		}
	}

	class DerivedColumn extends AnalyticColumn {

		final AnalyticColumn column;

		DerivedColumn(AnalyticColumn column) {
			this.column = column;
		}

		@Override
		C getColumn() {
			return column.getColumn();
		}
	}

	class AnalyticJoin extends Select {

		private final Select parent;
		private final Select child;

		AnalyticJoin(Select parent, Select child) {

			this.parent = wrapInView(parent, true);
			this.child = wrapInView(child, false);

			nodeParentLookUp.put(parent, this);
			nodeParentLookUp.put(child, this);

		}

		private Select wrapInView(Select node, boolean isParent) {

			if (isParent) {
				if (node instanceof AnalyticView) {
					return (Select)node.getParent();
				}
			} else {
				if (node instanceof TableDefinition td) {
					return new AnalyticView(td);
				}
			}
			return node;
		}

		@Override
		public List<? extends AnalyticColumn> getColumns() {

			List<AnalyticColumn> result = new ArrayList<>();
			parent.getColumns().forEach(c -> result.add(new DerivedColumn(c)));
			child.getColumns().forEach(c -> result.add(new DerivedColumn(c)));

			return result;
		}

		@Override
		public AnalyticColumn getId() {
			return new DerivedColumn(parent.getId());
		}

		@Override
		List<Select> getFroms() {
			return asList(parent, child);
		}

		@Override
		Object getParent() {
			return parent;
		}
	}

	class AnalyticView extends Select {

		private final TableDefinition table;

		AnalyticView(TableDefinition table) {

			this.table = table;

			nodeParentLookUp.put(table, this);

		}

		@Override
		List<? extends AnalyticColumn> getColumns() {
			return table.getColumns();
		}

		@Override
		AnalyticColumn getId() {
			return table.getId();
		}

		@Override
		List<Select> getFroms() {
			return Collections.singletonList(table);
		}

		@Override
		Object getParent() {
			return table;
		}
	}
}