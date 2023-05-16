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
import static org.springframework.data.jdbc.core.convert.sqlgeneration.AnalyticStructureBuilder.Multiplicity.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Builds the structure of an analytic query. The structure contains arbitrary objects for tables and columns. There are
 * two kinds of parent child relationship:
 * <ol>
 * <li>there is the relationship on aggregate level: the purchase order is the parent of the line item. This
 * relationship is denoted simply by "parent" and "child".</li>
 * <li>there is the parent child relationship inside the analytic query structure, that gets build by this builder.
 * Where a join combines two nodes. In this relationship the join is parent to the two nodes, where one node might
 * represent the purchase order and the other the line item. This kind or relationship shall be prefixed by "node". The
 * join {@literal node} is the {@literal nodeParent} of purchase order and line item.</li>
 * </ol>
 */
class AnalyticStructureBuilder<T, C> implements AnalyticStructure<T, C> {

	/** The select that is getting build */
	private Select nodeRoot;
	private final Map<Object, Select> nodeParentLookUp = new HashMap<>();

	private T root;

	AnalyticStructureBuilder<T, C> addTable(T table,
			Function<TableDefinition, TableDefinition> tableDefinitionConfiguration) {

		this.root = table;
		this.nodeRoot = createTable(table, null, tableDefinitionConfiguration);

		return this;
	}

	AnalyticStructureBuilder<T, C> addChildTo(T parent, C fkInformation, T child,
			Function<TableDefinition, TableDefinition> tableDefinitionConfiguration) {

		Select nodeParent = findUltimateNodeParent(parent);

		List<Select> nodeParentChain = collectNodeParents(nodeParent);

		AnalyticJoin newNode = new AnalyticJoin(nodeParent,
				createTable(child, fkInformation, tableDefinitionConfiguration));

		if (nodeParentChain.isEmpty()) {
			nodeRoot = newNode;
		} else {
			Select oldNode = nodeParentChain.get(0);
			if (oldNode instanceof AnalyticJoin aj) {
				aj.setChild(newNode);
			}

		}
		return this;
	}

	AnalyticStructureBuilder<T, C> addSingleChildTo(T parent, C fkInformation, T child,
			Function<TableDefinition, TableDefinition> tableDefinitionConfiguration) {

		Select nodeParent = findUltimateNodeParent(parent);

		List<Select> nodeParentChain = collectNodeParents(nodeParent);

		AnalyticJoin newNode = new AnalyticJoin(nodeParent, createTable(child, fkInformation, tableDefinitionConfiguration),
				SINGLE);

		if (nodeParentChain.isEmpty()) {
			nodeRoot = newNode;
		} else {

			Select oldNode = nodeParentChain.get(0);
			if (oldNode instanceof AnalyticJoin aj) {
				aj.setChild(newNode);
			}
		}

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

	/**
	 * Returns the node closest to the root of which the chain build by following the `parent` <i>(Note: not the node
	 * parent)</i> relationship leads to the node passed as an argument. When this node is a join it represents the join
	 * that joins all child elements to this node.
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
		} else if (!nodeParent.getParent().equals(node)) { // getParent is NOT looking for the node parent, but the parent
																												// in the entity relationship
			return node;
		} else {
			return findUltimateNodeParent(nodeParent);
		}
	}

	private TableDefinition createTable(T table, C fkInformation,
			Function<TableDefinition, TableDefinition> tableDefinitionConfiguration) {
		return tableDefinitionConfiguration.apply(new TableDefinition(table, fkInformation));
	}

	@Override
	public Select getSelect() {
		return nodeRoot;
	}

	public AnalyticStructure<T, C> build() {

		buildForeignKeys();
		buildRowNumbers();

		return this;
	}

	private void buildRowNumbers() {
		nodeRoot.buildRowNumbers();
	}

	private void buildForeignKeys() {
		nodeRoot.buildForeignKeys(null, new ArrayList<>());
	}

	private AnalyticColumn derived(AnalyticColumn column) {

		if (column == null) {
			return null;
		}

		if (column instanceof DerivedColumn || column instanceof Literal) {
			return column;
		}
		return new DerivedColumn(column);
	}

	public T getRoot() {
		return root;
	}

	abstract class Select {

		abstract List<? extends AnalyticColumn> getColumns();

		@Nullable
		abstract List<AnalyticColumn> getId();

		abstract List<Select> getFroms();

		abstract Object getParent();

		protected AnalyticColumn getRowNumber() {
			return new Literal(1);
		}

		abstract void buildForeignKeys(TableDefinition parent, List<AnalyticColumn> keyColumns);

		abstract List<AnalyticColumn> getForeignKey();

		abstract void buildRowNumbers();

		public T getRoot() {
			return AnalyticStructureBuilder.this.getRoot();
		}
	}

	abstract class SingleTableSelect extends Select {

	}

	class TableDefinition extends SingleTableSelect {

		private final T table;
		private AnalyticColumn id;
		private List<AnalyticColumn> columns;
		private List<AnalyticColumn> foreignKey = new ArrayList<>();
		private KeyColumn keyColumn;

		private final C pathInformation;

		TableDefinition(T table, @Nullable C pathInformation, @Nullable AnalyticColumn id,
				List<? extends AnalyticColumn> columns, ForeignKey foreignKey, KeyColumn keyColumn) {

			this.table = table;
			this.pathInformation = pathInformation;
			this.id = id;
			if (foreignKey != null) {
				this.foreignKey.add(foreignKey);
			}
			this.columns = Collections.unmodifiableList(columns);
			this.keyColumn = keyColumn;

			nodeParentLookUp.put(table, this);
		}

		TableDefinition(T table, C pathInformation) {
			this(table, pathInformation, null, Collections.emptyList(), null, null);
		}

		TableDefinition withId(C id) {

			this.id = new BaseColumn(id);
			return this;
		}

		TableDefinition withColumns(C... columns) {

			this.columns = new ArrayList<>(this.columns);
			Arrays.stream(columns).map(BaseColumn::new).forEach(bc -> this.columns.add(bc));
			return this;
		}

		TableDefinition withKeyColumn(C key) {
			this.keyColumn = new KeyColumn(key);
			return this;
		}

		@Override
		List<AnalyticColumn> getForeignKey() {
			return foreignKey;
		}

		@Override
		void buildRowNumbers() {
			// tables don't have rownumbers, they are added in a view and propagated through joins
		}

		@Override
		public List<? extends AnalyticColumn> getColumns() {

			List<AnalyticColumn> allColumns = new ArrayList<>(columns);
			if (id != null) {
				allColumns.add(id);
			}
			allColumns.addAll(foreignKey);
			if (keyColumn != null) {
				allColumns.add(keyColumn);
			}

			return allColumns;
		}

		@Override
		public List<AnalyticColumn> getId() {
			
			if (id == null) {
				List<AnalyticColumn> derivedKeys = new ArrayList<>();
				derivedKeys.addAll(foreignKey);
				if (keyColumn != null) {
					derivedKeys.add(keyColumn);
				}
				return derivedKeys;
			}
			return Collections.singletonList(id);
		}

		@Override
		List<Select> getFroms() {
			return Collections.emptyList();
		}

		@Override
		Object getParent() {
			return table;
		}

		@Override
		// TODO parent seems to be superfluous
		public void buildForeignKeys(TableDefinition parent, List<AnalyticColumn> keyColumns) {

			keyColumns.stream().map(c -> {
				ForeignKey fk = new ForeignKey(c);
				fk.setOwner(this);
				return fk;
			}).forEach(foreignKey::add);
		}

		T getTable() {
			return table;
		}

		@Override
		public String toString() {
			return "TD{" + table + '}';
		}

		public C getPathInformation() {
			return pathInformation;
		}
	}

	class AnalyticJoin extends Select {

		private final Select parent;
		private AnalyticColumn rowNumber;
		private Select child;

		// TODO: this is really more of an effective id. Make sure to keep the rownumber separately
		private final List<AnalyticColumn> columnsFromJoin = new ArrayList();
		private final Multiplicity multiplicity;

		private final List<JoinCondition> conditions = new ArrayList<>();
		private List<AnalyticColumn> foreignKey = new ArrayList<>();

		AnalyticJoin(Select parent, Select child, Multiplicity multiplicity) {

			this.parent = wrapParent(unwrapParent(parent));
			// this.parent = unwrapParent(parent);
			this.child = wrapChildInView(child);
			this.multiplicity = multiplicity;

			nodeParentLookUp.put(this.parent, this);
			nodeParentLookUp.put(this.child, this);

		}

		private Select wrapParent(Select parent) {

			if (parent instanceof TableDefinition td && td.getTable().equals(getRoot())) {
				return new AnalyticView(td);
			}
			return parent;
		}

		AnalyticJoin(Select parent, Select child) {
			this(parent, child, MULTIPLE);
		}

		private Select unwrapParent(Select node) {

			if (node instanceof AnalyticView) {
				return (Select) node.getParent();
			}
			return node;
		}

		@Nullable
		private TableDefinition extractTableDefinition(Select select) {

			if (select instanceof TableDefinition td) {
				return td;
			}

			if (select instanceof AnalyticView av) {
				return (TableDefinition) av.getFroms().get(0);
			}

			return null;
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

			List<AnalyticColumn> ids = parent.getId();

			parent.getColumns().stream().filter(c -> !ids.contains(c))
					.forEach(c -> result.add(parent instanceof TableDefinition ? c : derived(c)));
			child.getColumns().forEach(c -> result.add(child instanceof TableDefinition ? c : derived(c)));
			columnsFromJoin.forEach(c -> result.add(c));
			foreignKey.forEach(c -> result.add(c));

			result.add(rowNumber);
			return result;
		}

		@Override
		List<AnalyticColumn> getId() {
			return parent.getId().stream().map(column -> derived(column)).toList();
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
		protected DerivedColumn getRowNumber() {
			return (DerivedColumn) derived(rowNumber != null ? rowNumber : super.getRowNumber());
		}

		@Override
		public void buildForeignKeys(TableDefinition parent, List<AnalyticColumn> keyColumns) {

			// add fk columns as requested to the parent
			this.parent.buildForeignKeys(parent, keyColumns);

			// add id columns of the parent of this to the child of this join
			// TODO add keys and possibly
			// foreign keys, if no id is present ꜜ
			List<AnalyticColumn> parentIds = this.parent.getId();
			child.buildForeignKeys(extractTableDefinition(this.parent), parentIds);

			// for each such generated fk column
			Iterator<AnalyticColumn> parentIdsIterator = parentIds.iterator();
			child.getForeignKey().forEach(fk -> {

				AnalyticColumn parentId = parentIdsIterator.next();
				// add a coalesce expression between the parent.id and the fk, to have a non-null id for this join
				columnsFromJoin.add(new Coalesce(derived(parentId), fk, parentId));
				// add the parent.id child.fk relation to the join condition
				conditions.add(new JoinCondition(derived(parentId), fk));
			});

			// create max expressions, so all rows relating to this.parent have populated fk columns
			this.parent.getForeignKey().forEach(fk -> {
				MaxOver maxOver = new MaxOver(fk, columnsFromJoin);
				this.foreignKey.add(maxOver);
			});
		}

		@Override
		List<AnalyticColumn> getForeignKey() {
			return foreignKey;
		}

		@Override
		void buildRowNumbers() {

			parent.buildRowNumbers();
			child.buildRowNumbers();

			if (foreignKey.isEmpty()) {
				rowNumber = new Coalesce(parent.getRowNumber(), child.getRowNumber());
			} else {
				rowNumber = new RowNumber(foreignKey, Collections.singletonList(new Coalesce(parent.getRowNumber(), child.getRowNumber())));
			}

			conditions.add(new JoinCondition(derived(parent.getRowNumber()), derived(child.getRowNumber())));

		}

		@Override
		public String toString() {
			String prefix = "AJ {" + parent + ", " + child + ", ";

			String conditionString = String.join(", ",
					conditions.stream().map(jc -> "eq(" + jc.left + ", " + jc.right + ")").toList());
			conditionString = conditionString.isEmpty() ? "<no-condition>" : conditionString;
			return prefix + conditionString + '}';
		}

		void setChild(AnalyticJoin newChild) {

			nodeParentLookUp.put(newChild, this);
			this.child = newChild;
		}

		public List<JoinCondition> getConditions() {
			return conditions;
		}
	}

	enum Multiplicity {
		SINGLE, MULTIPLE
	}

	class JoinCondition {

		private final AnalyticColumn left;
		private final AnalyticColumn right;

		JoinCondition(AnalyticColumn left, AnalyticColumn right) {

			this.left = left == null ? new Literal(1) : left;
			this.right = right;
		}

		AnalyticColumn getLeft() {
			return left;
		}

		AnalyticColumn getRight() {
			return right;
		}
	}

	class AnalyticView extends SingleTableSelect {

		private final TableDefinition table;
		private AnalyticColumn rowNumber;

		AnalyticView(TableDefinition table) {

			this.table = table;

			nodeParentLookUp.put(table, this);
		}

		@Override
		List<? extends AnalyticColumn> getColumns() {

			ArrayList<AnalyticColumn> allColumns = new ArrayList<>();
			table.getColumns().forEach(c -> allColumns.add(c));

			if(rowNumber != null) {
				allColumns.add(rowNumber);
			}

			return allColumns;
		}

		@Override
		List<AnalyticColumn> getId() {
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
		List<AnalyticColumn> getForeignKey() {
			return table.getForeignKey().stream().map(AnalyticStructureBuilder.this::derived).collect(Collectors.toList());
		}

		@Override
		void buildRowNumbers() {

			if (table.table.equals(getRoot())) {
				return;
			}

			// TODO: table.getForeignKey seems to return the wrong (or at least changed value)
			// - Why?
			// - Is it really wrong?
			// Relevant test: AnalyticStructureBuilderTests.middleSingleChildHasNoId
			//RN([FK(parent, grannyId)]) turned into RN( partition by FK(child, FK(parent, grannyId)) order by FK(child, FK(parent, grannyId)))
			// note that the former is pattern syntax while the later is AnalyticStructure syntax

			List<AnalyticColumn> orderBy;
			if (table.keyColumn != null) {
				orderBy = Collections.singletonList(table.keyColumn);
			} else {
				if (table.getId().isEmpty()) {
					throw new IllegalStateException("A collection needs either a key (List or Map) or the elements need an id");
				}
				orderBy = table.getId();
			}
			rowNumber = new RowNumber(table.getForeignKey(), orderBy);

		}

		@Override
		public void buildForeignKeys(TableDefinition parent, List<AnalyticColumn> keyColumns) {
			table.buildForeignKeys(parent, keyColumns);
		}

		@Override
		protected AnalyticColumn getRowNumber() {
			return derived(rowNumber);
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

		@Override
		public String toString() {
			return column.toString();
		}
	}

	class DerivedColumn extends AnalyticColumn {

		final AnalyticColumn column;

		DerivedColumn(AnalyticColumn column) {

			Assert.notNull(column, "Can't create a derived column for null");

			this.column = column;
		}

		@Override
		C getColumn() {
			return column.getColumn();
		}

		AnalyticColumn getBase() {
			return column;
		}

		@Override
		public String toString() {
			return column.toString();
		}
	}

	class RowNumber extends AnalyticColumn {
		private final List<? extends AnalyticColumn> partitionBy;
		private final List<AnalyticColumn> orderBy;

		RowNumber(List<? extends AnalyticColumn> partitionBy, List<AnalyticColumn> orderBy) {

			this.partitionBy = partitionBy;
			this.orderBy = orderBy;
		}

		@Override
		C getColumn() {
			return null;
		}

		@Override
		public String toString() {
			return "RN( partition by " + partitionBy.stream().map(Object::toString).collect(Collectors.joining(", "))
					+ " order by " + orderBy.stream().map(Object::toString).collect(Collectors.joining(", ")) + ')';
		}

		public List<? extends AnalyticColumn> getPartitionBy() {
			return partitionBy;
		}

		public List<? extends AnalyticColumn> getOrderBy() {
			return orderBy;
		}
	}

	/**
	 * A Foreign Key referencing a different table. The referenced table is implicitly contained in the {@literal column}.
	 * The {@literal owner} references the table this foreign key is contained in.
	 */
	class ForeignKey extends AnalyticColumn {

		final AnalyticColumn column;
		private TableDefinition owner;

		ForeignKey(AnalyticColumn column) {
			this.column = column;
		}

		@Override
		C getColumn() {
			return column.getColumn();
		}

		AnalyticColumn getForeignKeyColumn() {
			return column;
		}

		void setOwner(TableDefinition owner) {
			this.owner = owner;
		}

		TableDefinition getOwner() {
			return owner;
		}

		@Override
		public String toString() {
			return "FK(" + owner.getTable() + ", " + column + ')';
		}
	}

	class KeyColumn extends AnalyticColumn {

		private final C column;

		KeyColumn(C column) {
			this.column = column;
		}

		@Override
		C getColumn() {
			return column;
		}

		@Override
		public String toString() {
			return "KEY(" + column + ')';
		}
	}

	interface WithAliasHint {
		// I would love to return an AnalyticColumn, but interfaces have to be static and therefore can't reference nested
		// classes
		Object hint();
	}

	class Coalesce extends AnalyticColumn implements WithAliasHint {

		final AnalyticColumn left;
		final AnalyticColumn right;
		private final Object aliasHint;

		Coalesce(AnalyticColumn left, AnalyticColumn right, Object aliasHint) {

			this.left = left == null ? new Literal(1) : left;
			this.right = right;
			this.aliasHint = aliasHint;
		}

		Coalesce(AnalyticColumn left, AnalyticColumn right) {
			this(left, right, null);
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

		@Override
		public Object hint() {
			return aliasHint;
		}

		@Override
		public String toString() {
			return "Coalesce(" + left + ", " + right + ')';
		}
	}

	class MaxOver extends AnalyticColumn {

		final AnalyticColumn expression;
		final List<AnalyticColumn> partitionBy;

		MaxOver(AnalyticColumn expression, AnalyticColumn partitionBy) {
			this(expression, Collections.singletonList(partitionBy));

		}

		public MaxOver(AnalyticColumn expression, List<AnalyticColumn> partitionBy) {
			this.expression = expression;
			this.partitionBy = partitionBy;
		}

		@Override
		C getColumn() {
			return null;
		}

		AnalyticColumn getExpression() {
			return expression;
		}

		List<AnalyticColumn> getPartitionBy() {
			return partitionBy;
		}

		@Override
		public String toString() {
			return "Max(" + expression + ") over (partition by, " + partitionBy + ')';
		}
	}

	class Literal extends AnalyticColumn {

		private final Object value;

		Literal(Object value) {
			this.value = value;
		}

		public Object getValue() {
			return value;
		}

		@Override
		public String toString() {
			return "lit(" + value + ')';
		}

		@Override
		C getColumn() {
			return null;
		}
	}
}
