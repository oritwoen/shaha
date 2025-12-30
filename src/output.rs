use std::sync::atomic::{AtomicBool, Ordering};

static QUIET: AtomicBool = AtomicBool::new(false);

pub fn set_quiet(quiet: bool) {
    QUIET.store(quiet, Ordering::Relaxed);
}

pub fn is_quiet() -> bool {
    QUIET.load(Ordering::Relaxed)
}

#[macro_export]
macro_rules! status {
    ($($arg:tt)*) => {
        if !$crate::output::is_quiet() {
            eprintln!($($arg)*);
        }
    };
}
