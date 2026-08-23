use std::path::Path;

use include_dir::{Dir, include_dir};

use crate::plugin::PluginLanguage;

static PROJECTS: Dir<'_> =
    include_dir!("$CARGO_MANIFEST_DIR/src/plugin/scaffold/templates/projects");

pub(super) struct GeneratedFile {
    pub(super) path: String,
    pub(super) contents: String,
}

pub(super) fn render(
    language: PluginLanguage,
    plugin_id: &str,
    plugin_name: &str,
) -> Vec<GeneratedFile> {
    let template = PROJECTS
        .get_dir(template_name(language))
        .expect("every PluginLanguage has an embedded project template");
    let plugin_module = plugin_name.replace('-', "_");
    let mut output = Vec::new();
    render_directory(
        template,
        template.path(),
        plugin_id,
        plugin_name,
        &plugin_module,
        &mut output,
    );
    output.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    output
}

fn render_directory(
    directory: &Dir<'_>,
    root: &Path,
    plugin_id: &str,
    plugin_name: &str,
    plugin_module: &str,
    output: &mut Vec<GeneratedFile>,
) {
    for file in directory.files() {
        let relative = file
            .path()
            .strip_prefix(root)
            .expect("an embedded template file belongs to its language root");
        let path = relative
            .to_str()
            .expect("embedded template paths are UTF-8")
            .replace("__OLL_PLUGIN_NAME__", plugin_name)
            .replace("__OLL_PLUGIN_MODULE__", plugin_module);
        let contents = file
            .contents_utf8()
            .expect("embedded project templates are UTF-8")
            .replace("__OLL_PLUGIN_ID__", plugin_id)
            .replace("__OLL_PLUGIN_NAME__", plugin_name)
            .replace("__OLL_PLUGIN_MODULE__", plugin_module);
        output.push(GeneratedFile { path, contents });
    }
    for child in directory.dirs() {
        render_directory(child, root, plugin_id, plugin_name, plugin_module, output);
    }
}

const fn template_name(language: PluginLanguage) -> &'static str {
    match language {
        PluginLanguage::Dotnet => "dotnet",
        PluginLanguage::Cpp => "cpp",
        PluginLanguage::Go => "go",
        PluginLanguage::Java => "java",
        PluginLanguage::Kotlin => "kotlin",
        PluginLanguage::Scala => "scala",
        PluginLanguage::Clojure => "clojure",
        PluginLanguage::Javascript => "javascript",
        PluginLanguage::Typescript => "typescript",
        PluginLanguage::Python => "python",
        PluginLanguage::Rust => "rust",
        PluginLanguage::Swift => "swift",
        PluginLanguage::Elixir => "elixir",
        PluginLanguage::Haskell => "haskell",
    }
}
