#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Extensions<'a> {
    pub security_contexts: Option<Vec<SecurityContext<'a>>>,
    pub supplementary_groups: Option<Vec<u32>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecurityContext<'a> {
    pub name: &'a [u8],
    pub label: &'a [u8],
}
