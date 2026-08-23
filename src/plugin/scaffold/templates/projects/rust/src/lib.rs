use onelastleaf_plugin_sdk::protocol::{ConfigValue, config_value};

pub fn echo(arguments: Vec<String>) -> ConfigValue {
    ConfigValue {
        kind: Some(config_value::Kind::StringValue(arguments.join(" "))),
    }
}
