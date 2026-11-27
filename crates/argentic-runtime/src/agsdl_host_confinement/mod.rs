//! OS enforcement of a native-authorized ceiling. Permission interpretation belongs
//! to agsdl_permissions; this module only lowers explicit kernel access constraints.
use std::path::Path;
use std::process::Stdio;

use sha2::{Digest, Sha256};

use crate::process::{background_tokio_command, ContainedBackgroundProcess};

/// Emitted by the trusted wrapper only after Seatbelt has installed its profile.
const START_ATTESTATION: &[u8] = b"MACRO_SEATBELT_STARTED_V1\n";
pub(crate) async fn attest(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> std::io::Result<()> {
    use tokio::io::AsyncReadExt;
    let mut actual = vec![0; START_ATTESTATION.len()];
    reader.read_exact(&mut actual).await?;
    if actual != START_ATTESTATION {
        return Err(std::io::Error::other(
            "Seatbelt startup attestation mismatch",
        ));
    }
    Ok(())
}

use argentic_harness::seatbelt::{access, BASE_PROFILE, PROCESS_DEFAULTS};
pub(crate) use argentic_harness::seatbelt::{FileAccess, PathScope};

/// One already-resolved ceiling from the native permission authority. These are
/// not authored presets/rules and are deliberately not deserializable over IPC.
#[derive(Debug, Clone, Default)]
pub(crate) struct OsCeiling {
    pub read: FileAccess,
    pub write: FileAccess,
    pub network: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct SandboxPolicy {
    policy_digest: String,
    profile: String,
    profile_digest: String,
}

impl SandboxPolicy {
    /// Called only by the native policy adapter after evaluating shell admission
    /// and proving these ceilings are <= all agent/capture/current ceilings.
    /// No runtime-library or device-path exemptions are added here.
    pub(crate) fn from_authorized_ceilings(
        policy_digest: String,
        ceilings: &[OsCeiling],
    ) -> Result<Self, String> {
        if policy_digest.is_empty() || ceilings.is_empty() {
            return Err("Missing native policy identity or ceilings".into());
        }
        let mut profile = String::from(BASE_PROFILE);
        profile.push_str("(allow process-exec process-fork)\n");
        profile.push_str(PROCESS_DEFAULTS);
        for (operation, read) in [("file-read*", true), ("file-write*", false)] {
            let mut filters = Vec::new();
            for ceiling in ceilings {
                match access(if read { &ceiling.read } else { &ceiling.write })? {
                    Some(filter) => filters.push(filter),
                    None => {
                        filters.clear();
                        break;
                    }
                }
            }
            if !filters.is_empty() {
                profile.push_str(&format!(
                    "(allow {operation} (require-all {}))\n",
                    filters.join(" ")
                ));
            }
        }
        if ceilings.iter().all(|ceiling| ceiling.network) {
            profile.push_str("(allow network*)\n");
        }
        let profile_digest = format!("{:x}", Sha256::digest(profile.as_bytes()));
        Ok(Self {
            policy_digest,
            profile,
            profile_digest,
        })
    }

    pub(crate) fn policy_digest(&self) -> &str {
        &self.policy_digest
    }
    pub(crate) fn profile_digest(&self) -> &str {
        &self.profile_digest
    }

    pub(crate) fn command(
        &self,
        program: &Path,
        args: &[String],
        cwd: &Path,
        env: &std::collections::BTreeMap<String, String>,
    ) -> Result<tokio::process::Command, String> {
        if !cfg!(all(target_os = "macos", target_arch = "aarch64")) {
            return Err(format!(
                "Seatbelt backend is not qualified on {}/{}",
                std::env::consts::OS,
                std::env::consts::ARCH
            ));
        }
        if !program.is_absolute() || !cwd.is_absolute() {
            return Err("Sandbox program and workspace must be absolute".into());
        }
        let mut command = background_tokio_command("/usr/bin/sandbox-exec");
        command
            .args(["-p", &self.profile])
            .arg("/bin/sh")
            .args([
                "-c",
                "printf 'MACRO_SEATBELT_STARTED_V1\\n'; exec \"$@\"",
                "macro-sandbox-start",
            ])
            .arg(program)
            .args(args);
        command.current_dir(cwd).env_clear().envs(env);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        Ok(command)
    }

    pub(crate) fn spawn(
        &self,
        program: &Path,
        args: &[String],
        cwd: &Path,
        env: &std::collections::BTreeMap<String, String>,
    ) -> Result<ContainedBackgroundProcess, String> {
        ContainedBackgroundProcess::spawn(self.command(program, args, cwd, env)?)
            .map_err(|e| format!("Confined process could not start: {e}"))
    }
}

#[cfg(test)]
pub(crate) mod tests;
