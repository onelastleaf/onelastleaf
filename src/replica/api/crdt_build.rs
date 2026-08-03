use std::collections::{BTreeSet, HashMap};

use loro::{
    Container, LoroDoc, LoroList, LoroMap, LoroMovableList, LoroText, LoroTree, LoroValue, TreeID,
    TreeParentId, ValueOrContainer,
};

use crate::protocol::oll;

use super::{
    super::ReplicaError,
    TREE_NODE_ID_KEY,
    catalog::invalid,
    crdt_mutation::{insert_list_value, insert_movable_value, set_map_value},
    crdt_read::{proto_scalar_to_loro, resolve_container},
    mutation::{loro_failure, usize_index, validate_range},
};

pub(super) fn populate_map(map: &LoroMap, value: oll::CrdtMap) -> Result<(), ReplicaError> {
    let mut entries = value.entries.into_iter().collect::<Vec<_>>();
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    for (key, value) in entries {
        validate_user_key(&key)?;
        set_map_value(map, &key, value)?;
    }
    Ok(())
}

pub(super) fn populate_list(
    list: &LoroList,
    values: Vec<oll::CrdtValue>,
) -> Result<(), ReplicaError> {
    for (index, value) in values.into_iter().enumerate() {
        insert_list_value(list, index, value)?;
    }
    Ok(())
}

pub(super) fn populate_movable_list(
    list: &LoroMovableList,
    values: Vec<oll::CrdtValue>,
) -> Result<(), ReplicaError> {
    for (index, value) in values.into_iter().enumerate() {
        insert_movable_value(list, index, value)?;
    }
    Ok(())
}

pub(super) fn populate_text(text: &LoroText, value: oll::CrdtText) -> Result<(), ReplicaError> {
    text.insert(0, &value.text).map_err(loro_failure)?;
    let length = value.text.chars().count();
    let mut marks = value.marks;
    marks.sort_by(|left, right| {
        (left.start_scalar, left.end_scalar, &left.name).cmp(&(
            right.start_scalar,
            right.end_scalar,
            &right.name,
        ))
    });
    for mark in marks {
        validate_user_key(&mark.name)?;
        let start = usize_index(mark.start_scalar, "text mark start")?;
        let end = usize_index(mark.end_scalar, "text mark end")?;
        if end < start {
            return Err(invalid("text mark range is reversed"));
        }
        validate_range(start, end - start, length, "text mark range")?;
        text.mark(
            start..end,
            &mark.name,
            proto_scalar_to_loro(
                mark.value
                    .ok_or_else(|| invalid("text mark value must be specified"))?,
            )?,
        )
        .map_err(loro_failure)?;
    }
    Ok(())
}

pub(super) fn populate_tree(tree: &LoroTree, value: oll::CrdtTree) -> Result<(), ReplicaError> {
    tree.enable_fractional_index(0);
    let mut pending = value.nodes;
    let mut seen = BTreeSet::new();
    for node in &pending {
        validate_tree_node_id(&node.node_id)?;
        if !seen.insert(node.node_id.clone()) {
            return Err(invalid("CRDT tree repeats a node_id"));
        }
    }
    let mut created = HashMap::<String, TreeID>::new();
    while !pending.is_empty() {
        pending.sort_by(|left, right| {
            (
                left.parent_id.as_deref(),
                left.index_in_parent,
                left.node_id.as_str(),
            )
                .cmp(&(
                    right.parent_id.as_deref(),
                    right.index_in_parent,
                    right.node_id.as_str(),
                ))
        });
        let mut progress = false;
        let mut remaining = Vec::new();
        for node in pending {
            let parent = match node.parent_id.as_deref() {
                None => Some(TreeParentId::Root),
                Some(parent) => created.get(parent).copied().map(TreeParentId::Node),
            };
            let Some(parent) = parent else {
                remaining.push(node);
                continue;
            };
            let index = usize_index(
                node.index_in_parent
                    .ok_or_else(|| invalid("tree node index_in_parent must be specified"))?,
                "tree node index",
            )?;
            let child_count = tree_child_count(tree, parent, "tree node parent")?;
            if index > child_count {
                return Err(invalid("tree node index is out of bounds"));
            }
            let tree_id = tree.create_at(parent, index).map_err(loro_failure)?;
            write_tree_node_metadata(tree, tree_id, &node.node_id, node.metadata)?;
            created.insert(node.node_id, tree_id);
            progress = true;
        }
        if !progress {
            return Err(invalid(
                "CRDT tree contains a missing parent or parent cycle",
            ));
        }
        pending = remaining;
    }
    Ok(())
}

pub(super) fn apply_tree_create(
    doc: &LoroDoc,
    operation: oll::TreeCreateNode,
) -> Result<(), ReplicaError> {
    validate_tree_node_id(&operation.node_id)?;
    let Container::Tree(tree) = resolve_container(doc, operation.target)? else {
        return Err(invalid("tree-create target is not a tree"));
    };
    tree.enable_fractional_index(0);
    if tree_api_ids(&tree)?
        .values()
        .any(|node_id| node_id == &operation.node_id)
    {
        return Err(ReplicaError::AlreadyExists(
            "tree node_id already exists".to_owned(),
        ));
    }
    let parent = operation
        .parent_id
        .as_deref()
        .map(|id| tree_node_by_api_id(&tree, id).map(TreeParentId::Node))
        .transpose()?
        .unwrap_or(TreeParentId::Root);
    let index = usize_index(operation.index, "tree index")?;
    let child_count = tree_child_count(&tree, parent, "tree-create parent")?;
    if index > child_count {
        return Err(invalid("tree-create index is out of bounds"));
    }
    let tree_id = tree.create_at(parent, index).map_err(loro_failure)?;
    write_tree_node_metadata(&tree, tree_id, &operation.node_id, operation.metadata)
}

fn write_tree_node_metadata(
    tree: &LoroTree,
    tree_id: TreeID,
    node_id: &str,
    metadata: HashMap<String, oll::CrdtScalar>,
) -> Result<(), ReplicaError> {
    let map = tree.get_meta(tree_id).map_err(loro_failure)?;
    map.insert(TREE_NODE_ID_KEY, node_id)
        .map_err(loro_failure)?;
    let mut metadata = metadata.into_iter().collect::<Vec<_>>();
    metadata.sort_by(|left, right| left.0.cmp(&right.0));
    for (key, value) in metadata {
        validate_user_key(&key)?;
        map.insert(&key, proto_scalar_to_loro(value)?)
            .map_err(loro_failure)?;
    }
    Ok(())
}

pub(super) fn tree_api_ids(tree: &LoroTree) -> Result<HashMap<TreeID, String>, ReplicaError> {
    let mut ids = HashMap::new();
    let mut unique = BTreeSet::new();
    for tree_id in tree.nodes() {
        if tree.is_node_deleted(&tree_id).map_err(loro_failure)? {
            continue;
        }
        let metadata = tree.get_meta(tree_id).map_err(loro_failure)?;
        let Some(ValueOrContainer::Value(LoroValue::String(node_id))) =
            metadata.get(TREE_NODE_ID_KEY)
        else {
            return Err(ReplicaError::CorruptStore(
                "tree node has no stable API identity".to_owned(),
            ));
        };
        let node_id = node_id.as_ref().to_owned();
        validate_tree_node_id(&node_id).map_err(|_| {
            ReplicaError::CorruptStore("tree node has an invalid stable API identity".to_owned())
        })?;
        if !unique.insert(node_id.clone()) {
            return Err(ReplicaError::CorruptStore(
                "tree repeats a stable API node identity".to_owned(),
            ));
        }
        ids.insert(tree_id, node_id);
    }
    Ok(ids)
}

pub(super) fn tree_node_by_api_id(tree: &LoroTree, node_id: &str) -> Result<TreeID, ReplicaError> {
    validate_tree_node_id(node_id)?;
    tree_api_ids(tree)?
        .into_iter()
        .find_map(|(tree_id, candidate)| (candidate == node_id).then_some(tree_id))
        .ok_or_else(|| invalid("tree node_id does not exist"))
}

pub(super) fn tree_child_count(
    tree: &LoroTree,
    parent: TreeParentId,
    field: &str,
) -> Result<usize, ReplicaError> {
    match parent {
        TreeParentId::Root => Ok(tree.children_num(parent).unwrap_or(0)),
        TreeParentId::Node(node) if tree.contains(node) => {
            if tree.is_node_deleted(&node).map_err(loro_failure)? {
                Err(invalid(&format!("{field} is deleted")))
            } else {
                Ok(tree.children_num(parent).unwrap_or(0))
            }
        }
        TreeParentId::Node(_) | TreeParentId::Deleted | TreeParentId::Unexist => {
            Err(invalid(&format!("{field} does not exist")))
        }
    }
}

pub(super) fn validate_tree_node_id(node_id: &str) -> Result<(), ReplicaError> {
    if node_id.is_empty() || node_id.len() > 512 || node_id.contains('\0') {
        Err(invalid(
            "tree node_id must be non-empty, NUL-free, and at most 512 bytes",
        ))
    } else {
        Ok(())
    }
}

pub(super) fn validate_user_key(key: &str) -> Result<(), ReplicaError> {
    if key == TREE_NODE_ID_KEY || key.contains('\0') {
        Err(invalid("CRDT map and metadata keys must be NUL-free"))
    } else {
        Ok(())
    }
}
