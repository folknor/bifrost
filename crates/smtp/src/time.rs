use std::time::SystemTime;

#[cfg(all(feature = "web", target_arch = "wasm32"))]
pub(crate) fn now() -> SystemTime {
    fn to_std_systemtime(time: web_time::SystemTime) -> std::time::SystemTime {
        let duration = time
            .duration_since(web_time::SystemTime::UNIX_EPOCH)
            .unwrap();
        SystemTime::UNIX_EPOCH + duration
    }

    to_std_systemtime(web_time::SystemTime::now())
}

#[cfg(not(all(feature = "web", target_arch = "wasm32")))]
#[allow(dead_code)]
pub(crate) fn now() -> SystemTime {
    SystemTime::now()
}
