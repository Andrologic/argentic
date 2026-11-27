use std::{env, path::PathBuf, process::Command};

fn main() {
    println!("cargo:rustc-check-cfg=cfg(argentic_shell_relay)");
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
    let build = |crt_static: &str| {
        let mut rustc = Command::new(env::var_os("RUSTC").expect("Cargo sets RUSTC"));
        rustc
            .args([source, "--edition=2021", "--target"])
            .arg(env::var_os("TARGET").expect("Cargo sets TARGET"))
            .arg("-C")
            .arg(format!("target-feature={crt_static}crt-static"))
            .args([
                "-C",
                "opt-level=s",
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

        rustc.output().is_ok_and(|result| result.status.success())
    };
    if build("+") {
        println!("cargo:rustc-cfg=argentic_shell_relay");
        return;
    }

    // A sysroot without static libc can still run a dynamic relay under
    // Bubblewrap, which mounts the system libraries used by Bash.
    if build("-") {
        println!("cargo:rustc-cfg=argentic_shell_relay");
    } else {
        println!("cargo:warning=Could not build the Argentic shell network relay. Install a C linker and target libc development files, then run `cargo clean -p argentic-harness` and rebuild.");
    }
}
