//! Synthetic protocol peers only. Never selects the installed CLI or a real home.
use super::*;
use crate::process::{background_contained_tokio_command, ContainedBackgroundProcess};
use std::process::Stdio;

const SERVER: &str = r#"
import json,sys,base64,os
source,mode,settings,bootstrap,profile=sys.argv[1:]
settings=json.loads(settings)
methods=[]; account='synthetic-account'; refreshed=False; replacement_committed=False; authed=False; exercise=None
def emit(v): print(json.dumps(v),flush=True)
for line in sys.stdin:
 m=json.loads(line); method=m.get('method'); i=m.get('id'); p=m.get('params',{})
 if method is None:
  if i=='approval':
   emit({'method':'test/approvalObserved','params':{'decision':m['result']['decision']}}); continue
  assert i=='auth-1'
  credential=json.load(open(source))['tokens']
  assert m['result']=={'accessToken':credential['access_token'],'chatgptAccountId':credential['account_id']}
  refreshed=True
  emit({'id':exercise,'result':{'ok':True}})
  continue
 methods.append(method)
 if i is None: continue
 if method=='initialize': result={}
 elif method=='config/read':
  observed=json.loads(json.dumps(settings))
  if (authed and mode=='post-login-config') or (refreshed and mode=='post-refresh-config'): observed['features']['hooks']=True
  if mode=='missing-source-denial': del observed['permissions']['macro-agsdl']['filesystem'][source]
  result={'layers':[{'name':{'type':'sessionFlags'},'config':observed}],'config':observed}
 elif method=='skills/list': result={'data':[{'cwd':bootstrap,'skills':[],'errors':[]}]}
 elif method=='command/exec': result={'exitCode':0}
 elif method=='account/login/start':
  assert methods[:6]==['initialize','initialized','config/read','skills/list','command/exec','account/login/start']
  credential=json.load(open(source))['tokens']
  assert p=={'type':'chatgptAuthTokens','accessToken':credential['access_token'],'chatgptAccountId':account}
  if mode=='login-error':
   emit({'id':i,'error':{'code':401,'message':p['accessToken'],'data':{'auth':json.load(open(source))}}}); continue
  if mode=='login-timeout': continue
  authed=True
  if mode=='persist-auth':
   with open(profile+'/codex/auth.json','w') as f: json.dump(p,f)
  result={'type':'chatgptAuthTokens'}
 elif method=='account/read':
  if refreshed and mode=='refresh-timeout': continue
  # The first read after the refresh reply sees the old auth. Commit only
  # after forming this read's result, without timing or scheduling assumptions.
  committed=refreshed
  if refreshed and mode.startswith('delayed-'):
   committed=replacement_committed
   replacement_committed=True
  routing={'chatgptAccountId':account,'backendOrigin':'https://chatgpt.com','accountRoutingOverride':'NO_CONSTRAINT'}
  if refreshed and mode=='route-change': routing['backendOrigin']='https://changed.invalid'
  if mode=='missing-routing': routing={}
  email='other@example.invalid' if committed and mode in ('user-change','delayed-user-change') else 'synthetic@example.invalid'
  if mode in ('no-email','user-id-no-email','managed-read-no-email') or (committed and mode in ('refresh-no-email','delayed-no-email')): email=None
  if mode=='user-id-email-change' and refreshed: email='new@example.invalid'
  result={'account':{'type':'chatgpt','email':email},'workspaceRouting':routing}
  if mode.startswith('user-id-'):
   result['userId']='other-user' if refreshed and mode=='user-id-change' else 'synthetic-user'
 elif method=='accountSession/list':
  assert p=={'refreshWorkspaceMetadata':False}
  if mode=='managed-unsupported':
   emit({'id':i,'error':{'code':-32601,'message':'unsupported method'}}); continue
  if mode=='managed-experimental-disabled':
   emit({'id':i,'error':{'code':-32600,'message':'accountSession/list requires experimentalApi capability'}}); continue
  if mode=='managed-other-invalid-request':
   emit({'id':i,'error':{'code':-32600,'message':'accountSession/list received invalid parameters'}}); continue
  if mode=='managed-failed':
   emit({'id':i,'error':{'code':-32603,'message':'failed'}}); continue
  assert mode.startswith('managed-')
  result={'activeSessionId':'active','sessions':[
   {'sessionId':'inactive','isActive':False,'selectedWorkspaceAccountId':account,'userId':'ignored-user'},
   {'sessionId':'active','isActive':True,'selectedWorkspaceAccountId':'other-account' if mode=='managed-other-workspace' else account,'email':None if mode=='managed-session-no-email' else 'synthetic@example.invalid','userId':None if mode=='managed-null' else 'synthetic-user'}
  ]}
 elif method=='test/refreshReceived': result=refreshed
 elif method=='exercise':
  if mode=='rpc-unsupported':
   emit({'id':'unknown','method':'item/commandExecution/requestApproval','params':{'message':'SYNTHETIC_NATIVE_SECRET'}}); continue
  exercise=i
  if mode!='same-token':
   record=json.load(open(source))
   claims={'exp':1 if mode=='expired-token' else 18446744073709551615,'generation':'rotated'}
   if mode not in ('refresh-no-email','delayed-no-email'):
    claims['https://api.openai.com/profile']={'email':'other@example.invalid' if mode in ('user-change','delayed-user-change') else 'synthetic@example.invalid'}
   if mode.startswith('user-id-'):
    claims['https://api.openai.com/auth']={'chatgpt_user_id':'other-user' if mode=='user-id-change' else 'synthetic-user'}
   payload=base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip('=')
   record['tokens']['access_token']='e30.'+payload+'.synthetic'
   if mode=='source-change': record['tokens']['account_id']='other-account'
   with open(source+'.next','w') as f: json.dump(record,f)
   os.chmod(source+'.next',0o600); os.replace(source+'.next',source)
  emit({'id':'auth-1','method':'account/chatgptAuthTokens/refresh','params':{'reason':'unauthorized','previousAccountId':account if mode!='foreign-callback' else 'other-account'}})
  continue
 else: raise Exception('unexpected method')
 emit({'id':i,'result':result})
"#;

struct Fixture {
    _temp: tempfile::TempDir,
    root: PathBuf,
    source: PathBuf,
    profile: Profile,
    process: ContainedBackgroundProcess,
    client: Client,
}
impl Fixture {
    fn new(mode: &str) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        for name in ["workspace", "profile", "bootstrap", "profile/codex", "bin"] {
            std::fs::create_dir(root.join(name)).unwrap();
        }
        let source = root.join(match mode {
            "package-auth-path" => "bin/ordinary.json",
            "bootstrap-auth-path" => "bootstrap/ordinary.json",
            _ => "ordinary.json",
        });
        auth::fixture_file(&source, "synthetic-account", u64::MAX, "initial");
        let profile = Profile {
            mcp: None,
            runtime: None,
            executable: root.join("bin/codex"),
            directory: root.join("profile"),
            home: root.join("profile/home"),
            bootstrap: root.join("bootstrap"),
            digest: "synthetic".into(),
        };
        let auth_source = (!mode.ends_with("-auth-path")).then_some(source.as_path());
        let settings = profile
            .settings_with_auth(&root.join("workspace"), true, auth_source)
            .unwrap();
        let mut command = background_contained_tokio_command("/usr/bin/python3");
        command
            .args(["-u", "-c", SERVER])
            .arg(&source)
            .arg(mode)
            .arg(settings.to_string())
            .arg(&profile.bootstrap)
            .arg(&profile.directory)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut process = ContainedBackgroundProcess::spawn(command).unwrap();
        let mut client = Client::new(
            process.take_stdin().unwrap(),
            process.take_stdout().unwrap(),
        );
        client.use_auth_source(Some(source.clone()), None);
        Self {
            _temp: temp,
            root,
            source,
            profile,
            process,
            client,
        }
    }
    async fn bootstrap(&mut self) -> Result<()> {
        self.client.initialize().await?;
        self.client
            .preflight(
                &self.profile,
                self.root.join("workspace").to_str().unwrap(),
                true,
            )
            .await
    }
    async fn end(mut self) {
        drop(self.client);
        self.process.terminate_bounded().await.unwrap();
    }
}

#[tokio::test]
async fn external_auth_refuses_sources_in_native_read_grants() {
    for mode in ["package-auth-path", "bootstrap-auth-path"] {
        let mut f = Fixture::new(mode);
        let command =
            f.profile
                .app_server_command(&f.root.join("workspace"), true, Some(&f.source));
        assert_eq!(command.unwrap_err().code, "CODEX_AUTH_SOURCE_UNSUPPORTED");
        assert_eq!(
            f.bootstrap().await.unwrap_err().code,
            "CODEX_AUTH_SOURCE_UNSUPPORTED"
        );
        f.end().await;
    }
    let mut f = Fixture::new("missing-source-denial");
    assert_eq!(
        f.bootstrap().await.unwrap_err().code,
        "CODEX_PROFILE_UNQUALIFIED"
    );
    f.end().await;
}

#[tokio::test]
async fn external_auth_rejects_glob_characters_in_source_paths() {
    let f = Fixture::new("ok");
    for name in [
        "auth[old].json",
        "auth].json",
        "auth*.json",
        "old?/auth.json",
    ] {
        let error = f
            .profile
            .app_server_command(&f.root.join("workspace"), true, Some(&f.root.join(name)))
            .unwrap_err();
        assert_eq!(error.code, "CODEX_AUTH_SOURCE_UNSUPPORTED");
        assert!(error.message.contains("glob"), "{error:?}");
    }
    f.end().await;
}

#[tokio::test]
async fn external_auth_bootstrap_and_refresh_preserve_private_config_and_rpc_order() {
    for notification in [false, true] {
        let mut f = Fixture::new("ok");
        let source_before = std::fs::read(&f.source).unwrap();
        let settings = f.profile.settings(&f.root.join("workspace"), true).unwrap();
        f.bootstrap().await.unwrap();
        assert_eq!(std::fs::read(&f.source).unwrap(), source_before);
        auth::no_private_auth(&f.profile.directory).unwrap();
        if notification {
            let id = f.client.begin_request("exercise", json!({})).await.unwrap();
            let callback = f.client.notification().await.unwrap();
            assert!(f.client.handle_auth_request(&callback).await.unwrap());
            assert_eq!(f.client.response(id).await.unwrap(), json!({"ok":true}));
        } else {
            assert_eq!(
                f.client.request("exercise", json!({})).await.unwrap(),
                json!({"ok":true})
            );
        }
        f.client.check_auth_current().await.unwrap();
        assert_eq!(
            f.profile.settings(&f.root.join("workspace"), true).unwrap(),
            settings
        );
        assert_eq!(std::fs::read_dir(&f.profile.directory).unwrap().count(), 1);
        assert_eq!(
            std::fs::read_dir(f.profile.directory.join("codex"))
                .unwrap()
                .count(),
            0
        );
        f.end().await;
    }
}

#[tokio::test]
async fn external_auth_rejects_foreign_captured_accounts_and_missing_native_routing() {
    let mut f = Fixture::new("ok");
    f.client.expected_account = Some("different-captured-digest".into());
    assert_eq!(
        f.bootstrap().await.unwrap_err().code,
        "CODEX_AUTH_ACCOUNT_CHANGED"
    );
    f.end().await;
    let mut f = Fixture::new("post-login-config");
    assert_eq!(
        f.bootstrap().await.unwrap_err().code,
        "CODEX_PROFILE_UNQUALIFIED"
    );
    f.end().await;
    let mut f = Fixture::new("missing-routing");
    assert_eq!(f.bootstrap().await.unwrap_err().code, "CODEX_AUTH_FAILED");
    f.end().await;
}

#[tokio::test]
async fn external_auth_errors_are_typed_and_never_retain_echoed_credentials() {
    let mut f = Fixture::new("login-error");
    let token = auth::read(&f.source).await.unwrap().access_token;
    let error = f.bootstrap().await.unwrap_err();
    assert_eq!(error.code, "CODEX_AUTH_FAILED");
    let public = serde_json::to_string(&error).unwrap();
    assert!(!public.contains(&token));
    assert!(!public.contains("SYNTHETIC_REFRESH"));
    assert!(!public.contains("auth.json"));
    assert!(f
        .client
        .pending
        .iter()
        .all(|v| !v.to_string().contains(&token)));
    // A failed bootstrap cannot become a successful retry on the same client.
    assert_eq!(
        f.client
            .bootstrap_auth(&f.profile, &f.root.join("workspace"), true)
            .await
            .unwrap_err()
            .code,
        "CODEX_AUTH_FAILED"
    );
    f.end().await;
}

#[tokio::test]
async fn diagnostic_rpc_refusal_exposes_only_its_fixed_stage() {
    let mut f = Fixture::new("rpc-unsupported");
    f.bootstrap().await.unwrap();
    let error = f.client.request("exercise", json!({})).await.unwrap_err();
    assert_eq!(error.code, "CODEX_RPC_SERVER_REQUEST_UNQUALIFIED");
    assert!(!serde_json::to_string(&error)
        .unwrap()
        .contains("SYNTHETIC_NATIVE_SECRET"));
    f.end().await;
}

#[tokio::test]
async fn external_auth_refresh_refuses_rejected_tokens_foreign_callbacks_and_account_drift() {
    for (mode, code) in [
        ("same-token", "CODEX_AUTH_REFRESH_REQUIRED"),
        ("foreign-callback", "CODEX_AUTH_ACCOUNT_CHANGED"),
        ("source-change", "CODEX_AUTH_ACCOUNT_CHANGED"),
        ("route-change", "CODEX_AUTH_ACCOUNT_CHANGED"),
        ("user-change", "CODEX_AUTH_ACCOUNT_CHANGED"),
        ("expired-token", "CODEX_AUTH_EXPIRED"),
        ("post-refresh-config", "CODEX_PROFILE_UNQUALIFIED"),
    ] {
        let mut f = Fixture::new(mode);
        f.bootstrap().await.unwrap();
        assert_eq!(
            f.client
                .request("exercise", json!({}))
                .await
                .unwrap_err()
                .code,
            code,
            "{mode}"
        );
        assert!(f.client.check_auth_current().await.is_err());
        auth::no_private_auth(&f.profile.directory).unwrap();
        f.end().await;
    }
}

#[tokio::test]
async fn external_auth_prohibits_private_persistence_and_stale_human_replies() {
    let mut f = Fixture::new("ok");
    f.bootstrap().await.unwrap();
    let reply = f.client.reply_sender030();
    auth::fixture_file(&f.source, "different-account", u64::MAX, "new-account");
    assert_eq!(
        reply
            .answer_request030(json!("approval"), json!({"decision":"accept"}))
            .await
            .unwrap_err()
            .code,
        "CODEX_AUTH_ACCOUNT_CHANGED"
    );
    f.end().await;
    let mut f = Fixture::new("ok");
    std::fs::write(f.profile.directory.join("codex/auth.json"), "synthetic").unwrap();
    assert_eq!(
        f.bootstrap().await.unwrap_err().code,
        "CODEX_AUTH_PROFILE_CONFLICT"
    );
    f.end().await;
    let mut f = Fixture::new("persist-auth");
    assert_eq!(
        f.bootstrap().await.unwrap_err().code,
        "CODEX_AUTH_PROFILE_CONFLICT"
    );
    f.end().await;
}

#[tokio::test]
async fn external_auth_wait_rechecks_stop_before_physical_approval() {
    let mut f = Fixture::new("ok");
    f.bootstrap().await.unwrap();
    let auth = f.client.external_auth.as_ref().unwrap().clone();
    let held = auth.0.lock().await;
    let reply = f.client.reply_sender030();
    let cancel = tokio_util::sync::CancellationToken::new();
    let signal = cancel.clone();
    let send = tokio::spawn(async move {
        reply
            .answer_request_checked030(
                json!("approval"),
                || json!({"decision":if signal.is_cancelled() {"decline"} else {"accept"}}),
            )
            .await
    });
    tokio::task::yield_now().await;
    assert!(!send.is_finished());
    cancel.cancel();
    drop(held);
    send.await.unwrap().unwrap();
    let event = f.client.notification().await.unwrap();
    assert_eq!(event["params"]["decision"], "decline");
    f.end().await;
}

#[tokio::test]
async fn managed_account_uses_direct_or_active_session_user_id() {
    let mut direct = Fixture::new("user-id-no-email");
    direct.client.use_auth_source(None, None);
    direct.client.initialize().await.unwrap();
    let stable = direct.client.current_account_digest().await.unwrap();
    direct.end().await;

    for mode in ["managed-user-id", "managed-null", "managed-unsupported"] {
        let mut f = Fixture::new(mode);
        f.client.use_auth_source(None, None);
        f.client.initialize().await.unwrap();
        let digest = f.client.current_account_digest().await.unwrap();
        assert_eq!(digest == stable, mode == "managed-user-id", "{mode}");
        f.end().await;
    }

    for (mode, code) in [
        ("managed-other-workspace", "CODEX_ACCOUNT_UNQUALIFIED"),
        ("managed-failed", "CODEX_REQUEST_FAILED"),
    ] {
        let mut f = Fixture::new(mode);
        f.client.use_auth_source(None, None);
        f.client.initialize().await.unwrap();
        assert_eq!(
            f.client.current_account_digest().await.unwrap_err().code,
            code
        );
        f.end().await;
    }
}

#[tokio::test]
async fn external_auth_binds_user_id_even_without_email_or_when_email_changes() {
    for mode in ["user-id-no-email", "user-id-email-change", "user-id-change"] {
        let mut f = Fixture::new(mode);
        f.bootstrap().await.unwrap();
        let digest = f.client.current_account_digest().await.unwrap();
        let result = f.client.request("exercise", json!({})).await;
        if mode == "user-id-change" {
            assert_eq!(result.unwrap_err().code, "CODEX_AUTH_ACCOUNT_CHANGED");
        } else {
            assert_eq!(result.unwrap(), json!({"ok":true}));
            assert_eq!(f.client.current_account_digest().await.unwrap(), digest);
        }
        f.end().await;
    }
}

#[tokio::test]
async fn external_auth_reports_missing_identity_at_login_and_refresh() {
    for mode in ["no-email", "refresh-no-email"] {
        let mut f = Fixture::new(mode);
        let result = if mode == "no-email" {
            f.bootstrap().await
        } else {
            f.bootstrap().await.unwrap();
            f.client.request("exercise", json!({})).await.map(|_| ())
        };
        let error = result.unwrap_err();
        assert_eq!(error.code, "CODEX_AUTH_IDENTITY_UNAVAILABLE");
        assert!(error.message.contains("Sign in again"), "{error:?}");
        assert!(f.client.check_auth_current().await.is_err());
        f.end().await;
    }
}

#[tokio::test]
async fn external_auth_deadlines_leave_process_drainage_owned() {
    for mode in ["login-timeout", "refresh-timeout"] {
        let mut f = Fixture::new(mode);
        let result = if mode == "login-timeout" {
            f.bootstrap().await
        } else {
            f.bootstrap().await.unwrap();
            f.client.request("exercise", json!({})).await.map(|_| ())
        };
        assert_eq!(result.unwrap_err().code, "CODEX_AUTH_TIMEOUT");
        assert!(
            !f.client
                .external_auth
                .as_ref()
                .unwrap()
                .0
                .lock()
                .await
                .valid
        );
        f.end().await;
    }
}

#[tokio::test]
async fn external_auth_rejects_replacement_before_a_stale_account_read() {
    for (mode, code) in [
        ("delayed-user-change", "CODEX_AUTH_ACCOUNT_CHANGED"),
        ("delayed-no-email", "CODEX_AUTH_IDENTITY_UNAVAILABLE"),
    ] {
        let mut f = Fixture::new(mode);
        f.bootstrap().await.unwrap();
        let reply = f.client.reply_sender030();
        let result = f.client.request("exercise", json!({})).await;
        assert_eq!(result.unwrap_err().code, code, "{mode}");
        assert!(f.client.check_auth_current().await.is_err());
        assert!(reply
            .answer_request030(json!("approval"), json!({"decision":"accept"}))
            .await
            .is_err());
        assert_eq!(
            f.client
                .request("test/refreshReceived", json!({}))
                .await
                .unwrap(),
            json!(false)
        );
        f.end().await;
    }
}

#[tokio::test]
async fn managed_account_requires_matching_emails_to_bind_the_active_session() {
    for mode in ["managed-read-no-email", "managed-session-no-email"] {
        let mut f = Fixture::new(mode);
        f.client.use_auth_source(None, None);
        f.client.initialize().await.unwrap();
        assert_eq!(
            f.client.current_account_digest().await.unwrap_err().code,
            "CODEX_ACCOUNT_UNQUALIFIED",
            "{mode}"
        );
        f.end().await;
    }
}

#[tokio::test]
async fn managed_account_falls_back_only_for_unsupported_session_listing() {
    for mode in [
        "managed-experimental-disabled",
        "managed-other-invalid-request",
    ] {
        let mut f = Fixture::new(mode);
        f.client.use_auth_source(None, None);
        f.client.initialize().await.unwrap();
        let result = f.client.current_account_digest().await;
        if mode == "managed-experimental-disabled" {
            assert!(result.unwrap().starts_with(ACCOUNT_DIGEST_PREFIX));
        } else {
            assert_eq!(result.unwrap_err().code, "CODEX_REQUEST_FAILED");
        }
        f.end().await;
    }
}
