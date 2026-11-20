#[derive(Clone, Debug, PartialEq, Eq)]
pub(in super::super) struct Extensions<'a> {
    pub(in super::super) security_contexts: Option<Vec<SecurityContext<'a>>>,
    pub(in super::super) supplementary_groups: Option<Vec<u32>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in super::super) struct SecurityContext<'a> {
    pub(in super::super) name: &'a [u8],
    pub(in super::super) label: &'a [u8],
}
