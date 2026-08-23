use onelastleaf_plugin_sdk::protocol::config_value;

#[test]
fn echo_preserves_argument_order() {
    let value = __OLL_PLUGIN_MODULE__::echo(vec!["one".into(), "two".into()]);
    assert!(matches!(
        value.kind,
        Some(config_value::Kind::StringValue(value)) if value == "one two"
    ));
}
