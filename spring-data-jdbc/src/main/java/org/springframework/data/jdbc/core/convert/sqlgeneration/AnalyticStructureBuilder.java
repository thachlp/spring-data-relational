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
	private final Map<Object, Select> nodeParentLookUp = new HashMap<>();

	AnalyticStructureBuilder<T, C> addTable(T table,
			Function<TableDefinition, TableDefinition> tableDefinitionConfiguration) {

		this.nodeRoot = createTable(table, tableDefinitionConfiguration);

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

	/**
	 * collects a list of nodes starting with the direct node parent of the node passed as an argument, going all the way
	 * up to the root.
	 */
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

		Select previousOldNode = null;

		for (Select oldNode : nodes) {

			Object parent = oldNode.getParent();
			if (previousOldNode == null || !previousOldNode.equals(parent)) {

				newNode = new AnalyticJoin((Select) parent, newNode);
			} else {
				newNode = new AnalyticJoin(newNode, ((AnalyticJoin) oldNode).getChild());
			}
			previousOldNode = oldNode;
		}

		return newNode;
	}

	List<? extends AnalyticColumn> getColumns() {
		return nodeRoot.getColumns();
	}

	AnalyticColumn getId() {
		return nodeRoot.getId();
	}

	/**
	 * Returns the node closest to the root of which the chain build by following the `parent` <i>(Note: not the node
	 * parent)</i> relationship leads to the node passed as an argument.
	 */
	private Select findUltimateNodeParent(T parent) {

		Select nodeParent = nodeParentLookUp.get(parent);

		Assert.state(nodeParent != null, "There must be a node parent");
		Assert.state(nodeParent.getParent().equals(parent), "The object in question must be the parent of the node parent");

		return findUltimateNodeParent(nodeParent);
	}

	private Select findUltimateNodeParent(Select node) {

		Select nodeParent = nodeParentLookUp.get(node);

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

		@Nullable
		abstract AnalyticColumn getId();

		abstract List<Select> getFroms();

		abstract Object getParent();
	}

	abstract class SingleTableSelect extends Select {

		abstract ForeignKey getForeignKey();
	}

	class TableDefinition extends SingleTableSelect {

		private final T table;
		private final AnalyticColumn id;
		private final List<? extends AnalyticColumn> columns;
		private final ForeignKey foreignKey;
		private final BaseColumn keyColumn;

		TableDefinition(T table, @Nullable AnalyticColumn id, List<? extends AnalyticColumn> columns, ForeignKey foreignKey,
				BaseColumn keyColumn) {

			this.table = table;
			this.id = id;
			this.foreignKey = foreignKey;
			this.columns = Collections.unmodifiableList(columns);
			this.keyColumn = keyColumn;

			nodeParentLookUp.put(table, this);
		}

		TableDefinition(T table) {
			this(table, null, Collections.emptyList(), null, null);
		}

		TableDefinition withId(C id) {
			return new TableDefinition(table, new BaseColumn(id), columns, foreignKey, keyColumn);
		}

		TableDefinition withColumns(C... columns) {

			return new TableDefinition(table, id, Arrays.stream(columns).map(BaseColumn::new).toList(), foreignKey,
					keyColumn);
		}

		TableDefinition withForeignKey(ForeignKey foreignKey) {
			return new TableDefinition(table, id, columns, foreignKey, keyColumn);
		}

		TableDefinition withKeyColumn(C key) {
			return new TableDefinition(table, id, columns, foreignKey, new BaseColumn(key));
		}

		@Override
		ForeignKey getForeignKey() {
			return foreignKey;
		}

		@Override
		public List<? extends AnalyticColumn> getColumns() {

			List<AnalyticColumn> allColumns = new ArrayList<>(columns);
			if (id != null) {
				allColumns.add(id);
			}
			if (foreignKey != null) {
				allColumns.add(foreignKey);
			}
			if (keyColumn != null) {
				allColumns.add(keyColumn);
			}

			return allColumns;
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

		@Override
		public String toString() {
			return "TD{" + table + '}';
		}

	}

	class AnalyticJoin extends Select {

		private final Select parent;
		private final Select child;
		private final JoinCondition joinCondition;
		private final List<AnalyticColumn> columnsFromJoin = new ArrayList();

		AnalyticJoin(Select parent, Select child) {

			this.parent = unwrapParent(parent);

			if (child instanceof TableDefinition td && parent.getId() != null) {
				ForeignKey foreignKey = new ForeignKey(parent.getId());
				child = td.withForeignKey(foreignKey);
				columnsFromJoin.add(new Max(parent.getId(), foreignKey));

			}
			this.child = wrapChildInView(child);

			this.joinCondition = new JoinCondition(parent.getId());

			nodeParentLookUp.put(this.parent, this);
			nodeParentLookUp.put(this.child, this);

		}

		private Select unwrapParent(Select node) {

			if (node instanceof AnalyticView) {
				return (Select) node.getParent();
			}
			return node;
		}

		private Select wrapChildInView(Select node) {

			if (node instanceof TableDefinition td) {
				return new AnalyticView(td);
			}
			return node;
		}

		@Override
		public List<? extends AnalyticColumn> getColumns() {

			List<AnalyticColumn> result = new ArrayList<>();
			parent.getColumns().forEach(c -> result.add(new DerivedColumn(c)));
			child.getColumns().forEach(c -> result.add(new DerivedColumn(c)));
			columnsFromJoin.forEach(c -> result.add(new DerivedColumn(c)));

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
		Select getParent() {
			return parent;
		}

		Select getChild() {
			return child;
		}

		@Override
		public String toString() {
			return "AJ {" + "p=" + parent + ", c=" + child + '}';
		}

		JoinCondition getJoinCondition() {
			return joinCondition;
		}

	}

	class JoinCondition {

		private final AnalyticColumn parent;

		JoinCondition(AnalyticColumn parent) {

			this.parent = parent;
		}

		AnalyticColumn getParent() {
			return parent;
		}
	}

	class AnalyticView extends SingleTableSelect {

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

		@Override
		public String toString() {
			return "AV{" + table + '}';
		}

		@Override
		ForeignKey getForeignKey() {
			return table.getForeignKey();
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

		AnalyticColumn getBase() {
			return column;
		}
	}

	class RowNumber extends AnalyticColumn {
		@Override
		C getColumn() {
			return null;
		}
	}

	class ForeignKey extends AnalyticColumn {

		final AnalyticColumn column;

		ForeignKey(AnalyticColumn column) {
			this.column = column;
		}

		@Override
		C getColumn() {
			return column.getColumn();
		}
	}

	class Max extends AnalyticColumn {

		final AnalyticColumn left;
		final AnalyticColumn right;

		Max(AnalyticColumn left, AnalyticColumn right) {

			this.left = left;
			this.right = right;
		}

		@Override
		C getColumn() {
			return null;
		}

		AnalyticColumn getLeft() {
			return left;
		}

		AnalyticColumn getRight() {
			return right;
		}
	}

}
