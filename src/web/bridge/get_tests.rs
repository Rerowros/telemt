use std::io::Write;
use std::process::{Command, Stdio};

/// Exercises the generated GET bridge page with deterministic network doubles.
pub(super) fn run(page: &super::BridgePage) {
    let mut child = Command::new("node")
        .args(["-e", include_str!("get_tests.js"), "--", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Node.js 22+ is required for executable WEB bridge regressions");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(page.body.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "GET bridge regressions failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
