use std::{env, path::PathBuf, process::Command};

fn main() {
    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("linux")
        || !matches!(
            env::var("CARGO_CFG_TARGET_ARCH").as_deref(),
            Ok("aarch64" | "x86_64")
        )
    {
        return;
    }
    let source = "src/tools/shell/linux/relay.rs";
    println!("cargo:rerun-if-changed={source}");
    println!("cargo:rerun-if-env-changed=RUSTC_LINKER");
    let output = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo sets OUT_DIR"));
    let mut rustc = Command::new(env::var_os("RUSTC").expect("Cargo sets RUSTC"));
    rustc
        .args([source, "--edition=2021", "--target"])
        .arg(env::var_os("TARGET").expect("Cargo sets TARGET"))
        .args([
            "-C",
            "opt-level=s",
            "-C",
            "target-feature=+crt-static",
            "-A",
            "linker_messages",
            "-C",
            "strip=symbols",
            "-C",
            "panic=abort",
            "-o",
        ])
        .arg(output.join("argentic-shell-relay"));
    if let Some(linker) = env::var_os("RUSTC_LINKER") {
        let mut option = std::ffi::OsString::from("linker=");
        option.push(linker);
        rustc.arg("-C").arg(option);
    }
    assert!(rustc.status().expect("run relay compiler").success());
}
