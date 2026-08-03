use loro::{
    Container, LoroCounter, LoroDoc, LoroList, LoroMap, LoroMovableList, LoroText, LoroTree,
    TreeParentId,
};

use crate::protocol::oll;

use super::{
    super::ReplicaError,
    catalog::invalid,
    crdt_build::{
        apply_tree_create, populate_list, populate_map, populate_movable_list, populate_text,
        populate_tree, tree_child_count, tree_node_by_api_id, validate_user_key,
    },
    crdt_read::{proto_scalar_to_loro, resolve_container},
    mutation::{loro_failure, usize_index, validate_range},
};

pub(super) fn apply_crdt_operation(
    doc: &LoroDoc,
    operation: oll::crdt_operation::Operation,
) -> Result<(), ReplicaError> {
    match operation {
        oll::crdt_operation::Operation::MapSet(operation) => {
            validate_user_key(&operation.key)?;
            let value = operation
                .value
                .ok_or_else(|| invalid("map-set value must be specified"))?;
            let Container::Map(map) = resolve_container(doc, operation.target)? else {
                return Err(invalid("map-set target is not a map"));
            };
            set_map_value(&map, &operation.key, value)
        }
        oll::crdt_operation::Operation::MapDelete(operation) => {
            validate_user_key(&operation.key)?;
            let Container::Map(map) = resolve_container(doc, operation.target)? else {
                return Err(invalid("map-delete target is not a map"));
            };
            map.delete(&operation.key).map_err(loro_failure)
        }
        oll::crdt_operation::Operation::ListInsert(operation) => {
            let index = usize_index(operation.index, "list index")?;
            let target = resolve_container(doc, operation.target)?;
            match target {
                Container::List(list) => {
                    if index > list.len() {
                        return Err(invalid("list-insert index is out of bounds"));
                    }
                    for (offset, value) in operation.values.into_iter().enumerate() {
                        insert_list_value(&list, index + offset, value)?;
                    }
                    Ok(())
                }
                Container::MovableList(list) => {
                    if index > list.len() {
                        return Err(invalid("list-insert index is out of bounds"));
                    }
                    for (offset, value) in operation.values.into_iter().enumerate() {
                        insert_movable_value(&list, index + offset, value)?;
                    }
                    Ok(())
                }
                _ => Err(invalid("list-insert target is not a list")),
            }
        }
        oll::crdt_operation::Operation::ListDelete(operation) => {
            let index = usize_index(operation.index, "list index")?;
            let count = usize_index(operation.count, "list count")?;
            match resolve_container(doc, operation.target)? {
                Container::List(list) => {
                    validate_range(index, count, list.len(), "list-delete range")?;
                    list.delete(index, count).map_err(loro_failure)
                }
                Container::MovableList(list) => {
                    validate_range(index, count, list.len(), "list-delete range")?;
                    list.delete(index, count).map_err(loro_failure)
                }
                _ => Err(invalid("list-delete target is not a list")),
            }
        }
        oll::crdt_operation::Operation::ListMove(operation) => {
            let index = usize_index(operation.index, "list index")?;
            let count = usize_index(operation.count, "list count")?;
            let destination = usize_index(operation.destination, "list destination")?;
            let Container::MovableList(list) = resolve_container(doc, operation.target)? else {
                return Err(invalid("list-move target is not a movable list"));
            };
            validate_range(index, count, list.len(), "list-move range")?;
            if destination > list.len().saturating_sub(count) {
                return Err(invalid("list-move destination is out of bounds"));
            }
            if destination < index {
                for offset in 0..count {
                    list.mov(index + offset, destination + offset)
                        .map_err(loro_failure)?;
                }
            } else if destination > index {
                for offset in (0..count).rev() {
                    list.mov(index + offset, destination + offset)
                        .map_err(loro_failure)?;
                }
            }
            Ok(())
        }
        oll::crdt_operation::Operation::TextInsert(operation) => {
            let index = usize_index(operation.scalar_index, "text scalar index")?;
            let Container::Text(text) = resolve_container(doc, operation.target)? else {
                return Err(invalid("text-insert target is not text"));
            };
            if index > text.len_unicode() {
                return Err(invalid("text-insert index is out of bounds"));
            }
            text.insert(index, &operation.text).map_err(loro_failure)
        }
        oll::crdt_operation::Operation::TextDelete(operation) => {
            let index = usize_index(operation.scalar_index, "text scalar index")?;
            let count = usize_index(operation.scalar_count, "text scalar count")?;
            let Container::Text(text) = resolve_container(doc, operation.target)? else {
                return Err(invalid("text-delete target is not text"));
            };
            validate_range(index, count, text.len_unicode(), "text-delete range")?;
            text.delete(index, count).map_err(loro_failure)
        }
        oll::crdt_operation::Operation::TextMark(operation) => {
            let start = usize_index(operation.start_scalar, "text mark start")?;
            let end = usize_index(operation.end_scalar, "text mark end")?;
            validate_user_key(&operation.name)?;
            let value = proto_scalar_to_loro(
                operation
                    .value
                    .ok_or_else(|| invalid("text-mark value must be specified"))?,
            )?;
            let Container::Text(text) = resolve_container(doc, operation.target)? else {
                return Err(invalid("text-mark target is not text"));
            };
            if end < start {
                return Err(invalid("text-mark range is reversed"));
            }
            validate_range(start, end - start, text.len_unicode(), "text-mark range")?;
            text.mark(start..end, &operation.name, value)
                .map_err(loro_failure)
        }
        oll::crdt_operation::Operation::TextUnmark(operation) => {
            let start = usize_index(operation.start_scalar, "text unmark start")?;
            let end = usize_index(operation.end_scalar, "text unmark end")?;
            validate_user_key(&operation.name)?;
            let Container::Text(text) = resolve_container(doc, operation.target)? else {
                return Err(invalid("text-unmark target is not text"));
            };
            if end < start {
                return Err(invalid("text-unmark range is reversed"));
            }
            validate_range(start, end - start, text.len_unicode(), "text-unmark range")?;
            text.unmark(start..end, &operation.name)
                .map_err(loro_failure)
        }
        oll::crdt_operation::Operation::CounterIncrement(operation) => {
            if !operation.delta.is_finite() {
                return Err(invalid("counter increment must be finite"));
            }
            let Container::Counter(counter) = resolve_container(doc, operation.target)? else {
                return Err(invalid("counter-increment target is not a counter"));
            };
            counter.increment(operation.delta).map_err(loro_failure)
        }
        oll::crdt_operation::Operation::TreeCreateNode(operation) => {
            apply_tree_create(doc, operation)
        }
        oll::crdt_operation::Operation::TreeDeleteNode(operation) => {
            let Container::Tree(tree) = resolve_container(doc, operation.target)? else {
                return Err(invalid("tree-delete target is not a tree"));
            };
            let node = tree_node_by_api_id(&tree, &operation.node_id)?;
            tree.delete(node).map_err(loro_failure)
        }
        oll::crdt_operation::Operation::TreeMoveNode(operation) => {
            let Container::Tree(tree) = resolve_container(doc, operation.target)? else {
                return Err(invalid("tree-move target is not a tree"));
            };
            tree.enable_fractional_index(0);
            let node = tree_node_by_api_id(&tree, &operation.node_id)?;
            let parent = operation
                .parent_id
                .as_deref()
                .map(|id| tree_node_by_api_id(&tree, id).map(TreeParentId::Node))
                .transpose()?
                .unwrap_or(TreeParentId::Root);
            let index = usize_index(operation.index, "tree index")?;
            let children = tree_child_count(&tree, parent, "tree-move parent")?;
            if index > children {
                return Err(invalid("tree-move index is out of bounds"));
            }
            tree.mov_to(node, parent, index).map_err(loro_failure)
        }
        oll::crdt_operation::Operation::TreeSetMetadata(operation) => {
            validate_user_key(&operation.key)?;
            let Container::Tree(tree) = resolve_container(doc, operation.target)? else {
                return Err(invalid("tree-metadata target is not a tree"));
            };
            let node = tree_node_by_api_id(&tree, &operation.node_id)?;
            let metadata = tree.get_meta(node).map_err(loro_failure)?;
            match operation.value {
                Some(value) => metadata
                    .insert(&operation.key, proto_scalar_to_loro(value)?)
                    .map_err(loro_failure),
                None => metadata.delete(&operation.key).map_err(loro_failure),
            }
        }
    }
}

pub(super) fn set_map_value(
    map: &LoroMap,
    key: &str,
    value: oll::CrdtValue,
) -> Result<(), ReplicaError> {
    match value
        .kind
        .ok_or_else(|| invalid("CRDT value kind must be specified"))?
    {
        oll::crdt_value::Kind::Scalar(value) => map
            .insert(key, proto_scalar_to_loro(value)?)
            .map_err(loro_failure),
        oll::crdt_value::Kind::Text(value) => {
            let child = map
                .insert_container(key, LoroText::new())
                .map_err(loro_failure)?;
            populate_text(&child, value)
        }
        oll::crdt_value::Kind::List(value) if value.movable => {
            let child = map
                .insert_container(key, LoroMovableList::new())
                .map_err(loro_failure)?;
            populate_movable_list(&child, value.values)
        }
        oll::crdt_value::Kind::List(value) => {
            let child = map
                .insert_container(key, LoroList::new())
                .map_err(loro_failure)?;
            populate_list(&child, value.values)
        }
        oll::crdt_value::Kind::Map(value) => {
            let child = map
                .insert_container(key, LoroMap::new())
                .map_err(loro_failure)?;
            populate_map(&child, value)
        }
        oll::crdt_value::Kind::Tree(value) => {
            let child = map
                .insert_container(key, LoroTree::new())
                .map_err(loro_failure)?;
            populate_tree(&child, value)
        }
        oll::crdt_value::Kind::Counter(value) => {
            if !value.value.is_finite() {
                return Err(invalid("counter value must be finite"));
            }
            let child = map
                .insert_container(key, LoroCounter::new())
                .map_err(loro_failure)?;
            child.increment(value.value).map_err(loro_failure)
        }
    }
}

pub(super) fn insert_list_value(
    list: &LoroList,
    index: usize,
    value: oll::CrdtValue,
) -> Result<(), ReplicaError> {
    match value
        .kind
        .ok_or_else(|| invalid("CRDT value kind must be specified"))?
    {
        oll::crdt_value::Kind::Scalar(value) => list
            .insert(index, proto_scalar_to_loro(value)?)
            .map_err(loro_failure),
        oll::crdt_value::Kind::Text(value) => {
            let child = list
                .insert_container(index, LoroText::new())
                .map_err(loro_failure)?;
            populate_text(&child, value)
        }
        oll::crdt_value::Kind::List(value) if value.movable => {
            let child = list
                .insert_container(index, LoroMovableList::new())
                .map_err(loro_failure)?;
            populate_movable_list(&child, value.values)
        }
        oll::crdt_value::Kind::List(value) => {
            let child = list
                .insert_container(index, LoroList::new())
                .map_err(loro_failure)?;
            populate_list(&child, value.values)
        }
        oll::crdt_value::Kind::Map(value) => {
            let child = list
                .insert_container(index, LoroMap::new())
                .map_err(loro_failure)?;
            populate_map(&child, value)
        }
        oll::crdt_value::Kind::Tree(value) => {
            let child = list
                .insert_container(index, LoroTree::new())
                .map_err(loro_failure)?;
            populate_tree(&child, value)
        }
        oll::crdt_value::Kind::Counter(value) => {
            if !value.value.is_finite() {
                return Err(invalid("counter value must be finite"));
            }
            let child = list
                .insert_container(index, LoroCounter::new())
                .map_err(loro_failure)?;
            child.increment(value.value).map_err(loro_failure)
        }
    }
}

pub(super) fn insert_movable_value(
    list: &LoroMovableList,
    index: usize,
    value: oll::CrdtValue,
) -> Result<(), ReplicaError> {
    match value
        .kind
        .ok_or_else(|| invalid("CRDT value kind must be specified"))?
    {
        oll::crdt_value::Kind::Scalar(value) => list
            .insert(index, proto_scalar_to_loro(value)?)
            .map_err(loro_failure),
        oll::crdt_value::Kind::Text(value) => {
            let child = list
                .insert_container(index, LoroText::new())
                .map_err(loro_failure)?;
            populate_text(&child, value)
        }
        oll::crdt_value::Kind::List(value) if value.movable => {
            let child = list
                .insert_container(index, LoroMovableList::new())
                .map_err(loro_failure)?;
            populate_movable_list(&child, value.values)
        }
        oll::crdt_value::Kind::List(value) => {
            let child = list
                .insert_container(index, LoroList::new())
                .map_err(loro_failure)?;
            populate_list(&child, value.values)
        }
        oll::crdt_value::Kind::Map(value) => {
            let child = list
                .insert_container(index, LoroMap::new())
                .map_err(loro_failure)?;
            populate_map(&child, value)
        }
        oll::crdt_value::Kind::Tree(value) => {
            let child = list
                .insert_container(index, LoroTree::new())
                .map_err(loro_failure)?;
            populate_tree(&child, value)
        }
        oll::crdt_value::Kind::Counter(value) => {
            if !value.value.is_finite() {
                return Err(invalid("counter value must be finite"));
            }
            let child = list
                .insert_container(index, LoroCounter::new())
                .map_err(loro_failure)?;
            child.increment(value.value).map_err(loro_failure)
        }
    }
}
