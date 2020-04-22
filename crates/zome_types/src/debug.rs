use holochain_serialized_bytes::prelude::*;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, SerializedBytes)]
pub struct DebugMsg {
    // TODO: Consider either replacing with `Cow<'static, str>` or with
    // `&'a str` and using `#[serde(borrow)]`
    module_path: String,
    // TODO: Consider either replacing with `Cow<'static, str>` or with
    // `&'a str` and using `#[serde(borrow)]`
    file: String,
    line: u32,
    msg: String,
}

impl DebugMsg {
    pub fn new(module_path: String, file: String, line: u32, msg: String) -> Self {
        Self {
            module_path,
            file,
            line,
            msg,
        }
    }

    pub fn msg(&self) -> &str {
        &self.msg
    }

    pub fn module_path(&self) -> &str {
        &self.module_path
    }

    pub fn file(&self) -> &str {
        &self.file
    }

    pub fn line(&self) -> u32 {
        self.line
    }
}

/// Returns a [`DebugMsg`][] combining the message passed `debug_msg!` with
/// the source code location in which it's called.
///
/// # Examples
///
/// Basic usage
///
/// ```rust
/// // Due to doc-test weirdness, this comment is technically on line 4.
/// let message: DebugMsg = debug_msg!("info: operation complete");
///
/// assert_eq!(message.msg(), "info: operation complete");
/// assert_eq!(message.file(), "src/debug.rs");
/// assert_eq!(message.line(), 5);
/// # use sx_zome_types::{debug::DebugMsg, debug_msg};
/// ```
///
/// Advanced formatting
///
/// ```rust
/// let operation = "frobnicate";
///
/// // Due to doc-test weirdness, this comment is technically on line 6.
/// let message: DebugMsg = debug_msg!(
///     "info: operation complete: {}",
///     operation
/// );
///
/// assert_eq!(message.msg(), "info: operation complete: frobnicate");
/// assert_eq!(message.file(), "src/debug.rs");
/// assert_eq!(message.line(), 7);
/// # use sx_zome_types::{debug::DebugMsg, debug_msg};
/// ```
///
/// [`DebugMsg`]: struct.DebugMsg.html
#[macro_export]
macro_rules! debug_msg {
    ( $msg:expr ) => {
        debug_msg!("{}", $msg);
    };
    ( $msg:expr, $($tail:expr),* ) => {{
        $crate::debug::DebugMsg::new(module_path!().to_string(), file!().to_string(), line!(), format!($msg, $($tail),*))
    }};
}
