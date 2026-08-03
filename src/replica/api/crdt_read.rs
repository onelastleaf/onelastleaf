use std::collections::HashMap;

use loro::{
    Container, LoroDoc, LoroText, LoroTree, LoroValue, TextDelta, TreeParentId, ValueOrContainer,
};

use crate::protocol::oll;

use super::{
    super::ReplicaError,
    TREE_NODE_ID_KEY,
    catalog::invalid,
    crdt_build::{tree_api_ids, tree_node_by_api_id, validate_tree_node_id, validate_user_key},
    mutation::{loro_failure, usize_index},
};

pub(super) fn validate_object_path(path: &oll::CrdtObjectPath) -> Result<(), ReplicaError> {
    for segment in &path.segments {
        match segment
            .kind
            .as_ref()
            .ok_or_else(|| invalid("CRDT path segment must be specified"))?
        {
            oll::crdt_path_segment::Kind::MapKey(key) => validate_user_key(key)?,
            oll::crdt_path_segment::Kind::ListIndex(_) => {}
            oll::crdt_path_segment::Kind::TreeNodeId(node_id) => {
                validate_tree_node_id(node_id)?;
            }
        }
    }
    Ok(())
}

pub(super) fn resolve_value(
    doc: &LoroDoc,
    path: &oll::CrdtObjectPath,
) -> Result<ValueOrContainer, ReplicaError> {
    validate_object_path(path)?;
    let mut value = ValueOrContainer::Container(Container::Map(doc.get_map("data")));
    for segment in &path.segments {
        let kind = segment.kind.as_ref().expect("validated above");
        value = match (value, kind) {
            (
                ValueOrContainer::Container(Container::Map(map)),
                oll::crdt_path_segment::Kind::MapKey(key),
            ) => map
                .get(key)
                .ok_or_else(|| invalid("CRDT map path key does not exist"))?,
            (
                ValueOrContainer::Container(Container::List(list)),
                oll::crdt_path_segment::Kind::ListIndex(index),
            ) => list
                .get(usize_index(*index, "list_index")?)
                .ok_or_else(|| invalid("CRDT list path index is out of bounds"))?,
            (
                ValueOrContainer::Container(Container::MovableList(list)),
                oll::crdt_path_segment::Kind::ListIndex(index),
            ) => list
                .get(usize_index(*index, "list_index")?)
                .ok_or_else(|| invalid("CRDT list path index is out of bounds"))?,
            (
                ValueOrContainer::Container(Container::Tree(tree)),
                oll::crdt_path_segment::Kind::TreeNodeId(node_id),
            ) => {
                let tree_id = tree_node_by_api_id(&tree, node_id)?;
                ValueOrContainer::Container(Container::Map(
                    tree.get_meta(tree_id).map_err(loro_failure)?,
                ))
            }
            _ => return Err(invalid("CRDT object path has a container-kind mismatch")),
        };
    }
    Ok(value)
}

pub(super) fn resolve_container(
    doc: &LoroDoc,
    path: Option<oll::CrdtObjectPath>,
) -> Result<Container, ReplicaError> {
    let path = path.unwrap_or_default();
    match resolve_value(doc, &path)? {
        ValueOrContainer::Container(container) => Ok(container),
        ValueOrContainer::Value(_) => Err(invalid("CRDT operation target is a scalar")),
    }
}

pub(super) fn value_or_container_to_proto(
    value: ValueOrContainer,
) -> Result<oll::CrdtValue, ReplicaError> {
    match value {
        ValueOrContainer::Value(value) => Ok(oll::CrdtValue {
            kind: Some(oll::crdt_value::Kind::Scalar(loro_scalar_to_proto(&value)?)),
        }),
        ValueOrContainer::Container(container) => container_to_proto(container),
    }
}

pub(super) fn container_to_proto(container: Container) -> Result<oll::CrdtValue, ReplicaError> {
    let kind = match container {
        Container::Map(map) => {
            let mut entries = HashMap::new();
            let mut failure = None;
            map.for_each(|key, value| {
                if failure.is_some() || key == TREE_NODE_ID_KEY {
                    return;
                }
                match value_or_container_to_proto(value) {
                    Ok(value) => {
                        entries.insert(key.to_owned(), value);
                    }
                    Err(error) => failure = Some(error),
                }
            });
            if let Some(error) = failure {
                return Err(error);
            }
            oll::crdt_value::Kind::Map(oll::CrdtMap { entries })
        }
        Container::List(list) => {
            let mut values = Vec::with_capacity(list.len());
            for index in 0..list.len() {
                values.push(value_or_container_to_proto(list.get(index).ok_or_else(
                    || ReplicaError::CorruptStore("Loro list index disappeared".to_owned()),
                )?)?);
            }
            oll::crdt_value::Kind::List(oll::CrdtList {
                values,
                movable: false,
            })
        }
        Container::MovableList(list) => {
            let mut values = Vec::with_capacity(list.len());
            for index in 0..list.len() {
                values.push(value_or_container_to_proto(list.get(index).ok_or_else(
                    || ReplicaError::CorruptStore("Loro movable-list index disappeared".to_owned()),
                )?)?);
            }
            oll::crdt_value::Kind::List(oll::CrdtList {
                values,
                movable: true,
            })
        }
        Container::Text(text) => oll::crdt_value::Kind::Text(text_to_proto(&text)?),
        Container::Tree(tree) => oll::crdt_value::Kind::Tree(tree_to_proto(&tree)?),
        Container::Counter(counter) => oll::crdt_value::Kind::Counter(oll::CrdtCounter {
            value: counter.get(),
        }),
        Container::Unknown(_) => {
            return Err(ReplicaError::CorruptStore(
                "document contains an unknown Loro container".to_owned(),
            ));
        }
    };
    Ok(oll::CrdtValue { kind: Some(kind) })
}

fn text_to_proto(text: &LoroText) -> Result<oll::CrdtText, ReplicaError> {
    let mut offset = 0_u64;
    let mut marks = Vec::new();
    for delta in text.to_delta() {
        let TextDelta::Insert { insert, attributes } = delta else {
            return Err(ReplicaError::CorruptStore(
                "materialized Loro text contains a non-insert delta".to_owned(),
            ));
        };
        let length = u64::try_from(insert.chars().count())
            .map_err(|_| ReplicaError::Internal("text length overflow".to_owned()))?;
        if let Some(attributes) = attributes {
            let mut attributes = attributes.into_iter().collect::<Vec<_>>();
            attributes.sort_by(|left, right| left.0.cmp(&right.0));
            for (name, value) in attributes {
                marks.push(oll::CrdtTextMark {
                    start_scalar: offset,
                    end_scalar: offset
                        .checked_add(length)
                        .ok_or_else(|| ReplicaError::Internal("text range overflow".to_owned()))?,
                    name,
                    value: Some(loro_scalar_to_proto(&value)?),
                });
            }
        }
        offset = offset
            .checked_add(length)
            .ok_or_else(|| ReplicaError::Internal("text length overflow".to_owned()))?;
    }
    Ok(oll::CrdtText {
        text: text.to_string(),
        marks,
    })
}

fn tree_to_proto(tree: &LoroTree) -> Result<oll::CrdtTree, ReplicaError> {
    let ids = tree_api_ids(tree)?;
    let mut nodes = Vec::new();
    for node in tree.get_nodes(false) {
        let node_id = ids.get(&node.id).ok_or_else(|| {
            ReplicaError::CorruptStore("tree node has no stable API identity".to_owned())
        })?;
        let parent_id = match node.parent {
            TreeParentId::Root => None,
            TreeParentId::Node(parent) => Some(
                ids.get(&parent)
                    .ok_or_else(|| {
                        ReplicaError::CorruptStore(
                            "tree node parent has no stable API identity".to_owned(),
                        )
                    })?
                    .clone(),
            ),
            TreeParentId::Deleted | TreeParentId::Unexist => {
                return Err(ReplicaError::CorruptStore(
                    "live tree node has an invalid parent".to_owned(),
                ));
            }
        };
        let metadata = tree.get_meta(node.id).map_err(loro_failure)?;
        let mut values = HashMap::new();
        let mut failure = None;
        metadata.for_each(|key, value| {
            if key == TREE_NODE_ID_KEY || failure.is_some() {
                return;
            }
            let ValueOrContainer::Value(value) = value else {
                failure = Some(ReplicaError::CorruptStore(
                    "tree metadata contains a child container".to_owned(),
                ));
                return;
            };
            match loro_scalar_to_proto(&value) {
                Ok(value) => {
                    values.insert(key.to_owned(), value);
                }
                Err(error) => failure = Some(error),
            }
        });
        if let Some(error) = failure {
            return Err(error);
        }
        nodes.push(oll::CrdtTreeNode {
            node_id: node_id.clone(),
            parent_id,
            index_in_parent: Some(
                u64::try_from(node.index)
                    .map_err(|_| ReplicaError::Internal("tree index overflow".to_owned()))?,
            ),
            metadata: values,
        });
    }
    nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(oll::CrdtTree { nodes })
}

fn loro_scalar_to_proto(value: &LoroValue) -> Result<oll::CrdtScalar, ReplicaError> {
    let kind = match value {
        LoroValue::Null => {
            oll::crdt_scalar::Kind::NullValue(prost_types::NullValue::NullValue as i32)
        }
        LoroValue::Bool(value) => oll::crdt_scalar::Kind::BoolValue(*value),
        LoroValue::I64(value) => oll::crdt_scalar::Kind::IntegerValue(*value),
        LoroValue::Double(value) => oll::crdt_scalar::Kind::NumberValue(*value),
        LoroValue::String(value) => oll::crdt_scalar::Kind::StringValue(value.as_ref().to_owned()),
        LoroValue::Binary(value) => oll::crdt_scalar::Kind::BytesValue(value.as_slice().to_vec()),
        LoroValue::List(_) | LoroValue::Map(_) | LoroValue::Container(_) => {
            return Err(ReplicaError::CorruptStore(
                "CRDT scalar position contains a non-scalar Loro value".to_owned(),
            ));
        }
    };
    Ok(oll::CrdtScalar { kind: Some(kind) })
}

pub(super) fn proto_scalar_to_loro(value: oll::CrdtScalar) -> Result<LoroValue, ReplicaError> {
    match value
        .kind
        .ok_or_else(|| invalid("CRDT scalar kind must be specified"))?
    {
        oll::crdt_scalar::Kind::BoolValue(value) => Ok(value.into()),
        oll::crdt_scalar::Kind::IntegerValue(value) => Ok(value.into()),
        oll::crdt_scalar::Kind::NumberValue(value) => Ok(value.into()),
        oll::crdt_scalar::Kind::StringValue(value) => Ok(value.into()),
        oll::crdt_scalar::Kind::BytesValue(value) => Ok(value.into()),
        oll::crdt_scalar::Kind::NullValue(value)
            if prost_types::NullValue::try_from(value).ok()
                == Some(prost_types::NullValue::NullValue) =>
        {
            Ok(LoroValue::Null)
        }
        oll::crdt_scalar::Kind::NullValue(_) => {
            Err(invalid("CRDT null scalar has an invalid enum value"))
        }
    }
}
