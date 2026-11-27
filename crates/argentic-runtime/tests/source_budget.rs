//! Dedicated binary so the allocator measures pulls through the public source
//! and calculation APIs without instrumenting the library or other tests.
use argentic_core::{
    validate_document_json_with_budget, SourceBudget, GRAPH_LIMIT_CODE, MAX_SOURCE_BYTES,
};
use argentic_runtime::{
    calculation, persistence::dto::EngineSourceRevision, source::OpenedSource, Engine,
};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    io::Write,
    path::Path,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
    time::Duration,
};

struct Counting;
#[global_allocator]
static ALLOCATOR: Counting = Counting;

static ACTIVE: AtomicBool = AtomicBool::new(false);
static TOTAL: AtomicUsize = AtomicUsize::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn allocated(size: usize) {
    let live = LIVE.fetch_add(size, Relaxed) + size;
    if ACTIVE.load(Relaxed) {
        TOTAL.fetch_add(size, Relaxed);
        PEAK.fetch_max(live, Relaxed);
    }
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            allocated(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            allocated(layout.size());
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let ptr = unsafe { System.realloc(ptr, layout, size) };
        if !ptr.is_null() {
            // Count the whole request and allow for a moving realloc holding
            // both buffers briefly, even if the system allocator grew in place.
            allocated(size);
            LIVE.fetch_sub(layout.size(), Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Relaxed);
        unsafe { System.dealloc(ptr, layout) };
    }
}

#[cfg(unix)]
fn cpu_now() -> Duration {
    let mut time: libc::timespec = unsafe { std::mem::zeroed() };
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut time) },
        0
    );
    Duration::new(time.tv_sec as u64, time.tv_nsec as u32)
}

/// GetProcessTimes advances in 15.625 ms ticks, too coarse for the short
/// samples compared below. Process cycle counts are precise. Only ratios of
/// these values are compared, so one cycle stands for one nanosecond.
#[cfg(windows)]
fn cpu_now() -> Duration {
    use windows_sys::Win32::System::{
        Threading::GetCurrentProcess, WindowsProgramming::QueryProcessCycleTime,
    };
    let mut cycles = 0u64;
    assert_ne!(
        unsafe { QueryProcessCycleTime(GetCurrentProcess(), &mut cycles) },
        0
    );
    Duration::from_nanos(cycles)
}

struct AllocationSample {
    total: usize,
    peak: usize,
    cpu: Duration,
}

fn allocation_sample<T>(operation: impl FnOnce() -> T) -> (T, AllocationSample) {
    TOTAL.store(0, Relaxed);
    let base = LIVE.load(Relaxed);
    PEAK.store(base, Relaxed);
    let start = cpu_now();
    ACTIVE.store(true, Relaxed);
    let result = operation();
    ACTIVE.store(false, Relaxed);
    let sample = AllocationSample {
        total: TOTAL.load(Relaxed),
        peak: PEAK.load(Relaxed).saturating_sub(base),
        cpu: cpu_now() - start,
    };
    (result, sample)
}

struct Sample {
    cpu: Duration,
    work: usize,
    charged: usize,
}

fn measure<T>(name: &str, operation: impl Fn(&SourceBudget) -> T) -> (T, Sample) {
    let mut fastest = Duration::MAX;
    let mut largest_ratio = 0.0_f64;
    // One test owns the whole binary. The minimum of three CPU samples
    // reduces noise from scheduling and first-use initialization.
    for attempt in 0..3 {
        let budget = SourceBudget::default();
        let (result, AllocationSample { total, peak, cpu }) =
            allocation_sample(|| operation(&budget));
        let (work, charged) = budget.usage();
        // k=2 allows allocator request growth and bookkeeping estimates.
        // The peak live check uses the budget itself, with no multiplier.
        assert!(
            total <= 2 * charged,
            "{name} sample {attempt}: {total} allocated, {charged} charged"
        );
        largest_ratio = largest_ratio.max(total as f64 / charged as f64);
        assert!(peak <= MAX_SOURCE_BYTES, "{name}: {peak} peak live bytes");
        fastest = fastest.min(cpu);
        if attempt == 2 {
            eprintln!("{name}: {total} allocated / {charged} charged ({:.3}x; max {largest_ratio:.3}x), {peak} peak, {work} work, {fastest:?}", total as f64 / charged as f64);
            return (
                result,
                Sample {
                    cpu: fastest,
                    work,
                    charged,
                },
            );
        }
    }
    unreachable!()
}

fn scales(label: &str, small: Sample, large: Sample) {
    assert!(large.work > small.work, "{label}: work did not grow");
    let charged_ratio = large.work as f64 / small.work as f64;
    let cpu_ratio = large.cpu.as_secs_f64() / small.cpu.as_secs_f64();
    eprintln!("{label}: CPU {cpu_ratio:.2}x, work {charged_ratio:.2}x");
    let allowed = small.cpu.mul_f64(4.0 * charged_ratio);
    assert!(
        large.cpu <= allowed,
        "{label}: CPU {:?} -> {:?} ({cpu_ratio:.2}x) vs work {charged_ratio:.2}x",
        small.cpu,
        large.cpu
    );
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn revision(authored: &str) -> Value {
    json!({"owner":{"projectId":"p","planId":"pipeline","branch":"main"},"revision":"1","sourceSha256":digest(authored.as_bytes())})
}

fn pipeline(sections: usize, evidence_bytes: usize) -> (String, String) {
    let mut agents = Map::new();
    let mut steps = Map::new();
    let mut proofs = Vec::new();
    let receipt = r#"{"fixture":true}"#;
    for section in 0..sections {
        let first = section * 10;
        let join = format!("join{section}");
        let approve = format!("approve{section}");
        let branches: Vec<_> = (first + 1..first + 10)
            .map(|index| format!("a{index}"))
            .collect();
        for index in first..first + 10 {
            let name = format!("a{index}");
            let definition =
                json!({"harness":"codex","harnessVersion":"^1.0.0","permissions":"read-only"});
            proofs.push(
                json!({"agentName":name,"agentDeclarationJson":definition.to_string(),
                "settingsJson":"{}","permissionsJson":receipt,"scopeJson":receipt,
                "evidenceJson":receipt,"provenanceJson":receipt}),
            );
            agents.insert(name.clone(), definition);
            let next = if index == first {
                json!(branches)
            } else {
                json!(join)
            };
            steps.insert(
                name.clone(),
                json!({"type":"agent","agent":name,"next":next}),
            );
        }
        steps.insert(join, json!({"type":"join","next":approve}));
        let next = if section + 1 == sections {
            json!([])
        } else {
            json!(format!("a{}", first + 10))
        };
        steps.insert(
            approve,
            json!({"type":"approval","title":"Continue?","next":{"yes":next,"no":[]}}),
        );
    }
    let authored = json!({"contract":"agsdl-0.4.0","id":"pipeline","agents":agents,
        "flow":{"entry":"a0","steps":steps}})
    .to_string();
    let native = |name| json!({"name":name,"support":"native","evidenceJson":receipt});
    let mut capabilities = [
        "result-submission",
        "continuity",
        "permission-enforcement",
        "contained-stop",
        "response-attribution",
        "delivery.queue",
        "resources",
    ]
    .map(native);
    capabilities[0]["evidenceJson"] = json!({"kind":"harness-version-range","harness":"codex",
        "version":"1.0.0","range":"^1.0.0","satisfied":true,
        "receipt":{"detail":"e".repeat(evidence_bytes)}})
    .to_string()
    .into();
    let registry = json!({"format":"macro-agsdl-registry/030","version":"fixture","modules":[],
        "agents":proofs,"harnesses":[{"harness":"codex","version":"1.0.0","installation":"available",
        "adapterId":"codex-app-server-v1","qualified":true,"capabilities":capabilities,
        "models":[{"name":"test-model","efforts":[],"defaultEffort":null}],"defaultModel":"test-model"}],
        "hostCapabilities":[native("approval")],"startEntry":{"messageJson":"{}"}}).to_string();
    (authored, registry)
}

fn asset_project(root: &Path, attachment_size: usize, mut document: Value) {
    let mut content = Map::new();
    for i in 0..4 {
        let path = format!("attachment-{i}.txt");
        std::fs::write(root.join(&path), vec![b'x'; attachment_size]).unwrap();
        content.insert(format!("asset{i}"), json!({"uri":path}));
    }
    document["projectFormat"] = "agsdl-project-1".into();
    document["content"] = Value::Object(content);
    std::fs::write(root.join("agsdl.json"), document.to_string()).unwrap();
}

fn deflated_archive(project: &OpenedSource) -> Vec<u8> {
    use flate2::{write::DeflateEncoder, Compression};

    let files: Vec<_> = project.selected_files().unwrap().collect();
    let index: Map<_, _> = files.iter().map(|file| (file.path.to_string(), json!({
        "sha256":digest(file.bytes), "size":file.bytes.len(), "executable":file.executable
    }))).collect();
    let index = json!({"archiveFormat":"agsdl-archive-1","files":index}).to_string();
    let count = files.len() + 1;
    let mut zip = Vec::new();
    let mut directory = Vec::new();
    for (name, bytes) in files
        .iter()
        .map(|file| (format!("project/{}", file.path), file.bytes))
        .chain(std::iter::once(("archive.json".into(), index.as_bytes())))
    {
        let offset = zip.len();
        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(bytes).unwrap();
        let compressed = encoder.finish().unwrap();
        let mut header = [0; 30];
        header[..4].copy_from_slice(b"PK\x03\x04");
        header[4..6].copy_from_slice(&20_u16.to_le_bytes());
        header[8..10].copy_from_slice(&8_u16.to_le_bytes());
        header[14..18].copy_from_slice(&crc32fast::hash(bytes).to_le_bytes());
        header[18..22].copy_from_slice(&(compressed.len() as u32).to_le_bytes());
        header[22..26].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
        header[26..28].copy_from_slice(&(name.len() as u16).to_le_bytes());
        zip.extend(header);
        zip.extend(name.as_bytes());
        zip.extend(compressed);

        let mut entry = [0; 46];
        entry[..4].copy_from_slice(b"PK\x01\x02");
        entry[6..30].copy_from_slice(&header[4..28]);
        entry[42..46].copy_from_slice(&(offset as u32).to_le_bytes());
        directory.extend(entry);
        directory.extend(name.as_bytes());
    }
    let mut end = [0; 22];
    end[..4].copy_from_slice(b"PK\x05\x06");
    end[8..10].copy_from_slice(&(count as u16).to_le_bytes());
    end[10..12].copy_from_slice(&(count as u16).to_le_bytes());
    end[12..16].copy_from_slice(&(directory.len() as u32).to_le_bytes());
    end[16..20].copy_from_slice(&(zip.len() as u32).to_le_bytes());
    zip.extend(directory);
    zip.extend(end);
    zip
}

fn settings(rows: usize, fields: usize, escaped: bool) -> (String, String) {
    let properties: Map<_, _> = (0..fields)
        .map(|i| {
            let label = if escaped {
                format!("{i}{}", "\u{1}".repeat(158))
            } else {
                format!("field{i}")
            };
            (label, json!({"type":"string"}))
        })
        .collect();
    let required: Vec<_> = properties.keys().cloned().collect();
    let module = json!({"contract":"agsdl-0.3.0","name":"example","version":"1.0.0",
        "harnessVersions":"*","properties":{},"permissions":{},
        "settings":{"type":"object","properties":{"rows":{"type":"array","items":{
        "type":"object","properties":properties,"required":required}}}}})
    .to_string();
    let doc = json!({"contract":"agsdl-0.4.0","id":"settings","agents":{"a":{
        "harness":"example","permissions":"read-only","harnessSettings":{"rows":vec![json!({});rows]}}}}).to_string();
    (doc, module)
}

fn escaped_warnings() -> (String, String) {
    let module = json!({"contract":"agsdl-0.3.0","name":"example","version":"1.0.0",
        "harnessVersions":"*","properties":{"model.name":{"support":"approximate",
        "note":"\u{1}".repeat(1500)}},"permissions":{}})
    .to_string();
    let agents: Map<_, _> = (0..220)
        .map(|i| {
            (
                format!("a{i}"),
                json!({
                    "harness":"example","permissions":"read-only","model":{"name":"example"}
                }),
            )
        })
        .collect();
    let authored = json!({"contract":"agsdl-0.4.0","id":"escaped","agents":agents}).to_string();
    (authored, module)
}

fn selections(count: usize, digits: usize) -> String {
    let resources: Map<_, _> = (0..count)
        .map(|i| (format!("r{i}"), json!({"from":["results","data",0]})))
        .collect();
    json!({"contract":"agsdl-0.4.0","id":"selections","agents":{"a":{
        "harness":"codex","permissions":"read-only","results":{"data":{"schema":{
        "type":"array","maxItems":"NUMBER","items":{"type":"string"}}}}
    }},"flow":{"entry":"source","steps":{
        "source":{"type":"agent","agent":"a","next":{"step":"sink","message":{"resources":resources}}},
        "sink":{"type":"agent","agent":"a","instance":"new"}
    }}}).to_string().replace("\"NUMBER\"", &"9".repeat(digits))
}

fn generated(name: &str) -> String {
    match name {
        "depth" => format!("{}null{}", "[".repeat(4096), "]".repeat(4096)),
        "shape" => {
            let agents: Map<_, _> = (0..1400).map(|i| (format!("a{i}"), json!(false))).collect();
            json!({"contract":"agsdl-0.4.0","id":"shape","agents":agents}).to_string()
        }
        "approval paths" => {
            let approval = |next| json!({"type":"approval","title":"Continue?","next":next});
            let mut steps = Map::new();
            steps.insert(
                "entry".into(),
                approval(json!({"left":"l0","right":"r0","direct":{
                    "step":"end","message":{"prompt":"Review."}
                }})),
            );
            for i in 0..26 {
                let next = if i == 25 {
                    json!({"yes":"end","no":"end"})
                } else {
                    json!({"yes":format!("l{}",i+1),"no":format!("r{}",i+1)})
                };
                steps.insert(format!("l{i}"), approval(next.clone()));
                steps.insert(format!("r{i}"), approval(next));
            }
            steps.insert("end".into(), json!({"type":"agent","agent":"a"}));
            json!({"contract":"agsdl-0.4.0","id":"approval-paths","agents":{"a":{
                "harness":"codex","permissions":"read-only"}},"flow":{"entry":"entry","steps":steps}}).to_string()
        }
        "long fanout" => {
            let mut steps = Map::new();
            steps.insert(
                "origin".into(),
                json!({"type":"agent","agent":"origin","next":[{
                    "step":"s0","each":["instructions"],"max":"NUMBER"
                }]}),
            );
            for i in 0..250 {
                let next = if i == 249 {
                    json!([])
                } else {
                    json!(format!("s{}", i + 1))
                };
                steps.insert(
                    format!("s{i}"),
                    json!({"type":"agent","agent":"worker","maxVisits":1,"next":next}),
                );
            }
            json!({"contract":"agsdl-0.4.0","id":"long-fanout","agents":{
                "origin":{"harness":"codex","permissions":"read-only","instructs":true},
                "worker":{"harness":"codex","permissions":"read-only"}
            },"flow":{"entry":"origin","steps":steps}})
            .to_string()
            .replace("\"NUMBER\"", &"9".repeat(128 * 1024))
        }
        "warnings" => {
            let agents: Map<_, _> = (0..1100)
                .map(|i| {
                    (
                        format!("a{i}"),
                        json!({
                            "harness":"example","permissions":"read-only"
                        }),
                    )
                })
                .collect();
            json!({"contract":"agsdl-0.4.0","id":"warnings","agents":agents}).to_string()
        }
        "permissions" => json!({"contract":"agsdl-0.4.0","id":"permissions","agents":{"a":{
            "harness":"codex","completionCheck":{"command":"x".repeat(4000)},
            "permissions":{"rules":[{"action":"shell","effect":"allow","match":"x*".repeat(2000)}]}
        }}})
        .to_string(),
        "overlap" => {
            let names: Vec<_> = (0..128).map(|i| format!("branch{i}")).collect();
            let mut steps: Map<_, _> = names
                .iter()
                .map(|name| {
                    (
                        name.clone(),
                        json!({
                            "type":"agent","agent":"a","next":"shared"
                        }),
                    )
                })
                .collect();
            let targets: Vec<_> = names.iter().map(String::as_str).chain(["join"]).collect();
            steps.insert(
                "start".into(),
                json!({"type":"agent","agent":"a","next":targets}),
            );
            steps.insert(
                "shared".into(),
                json!({"type":"agent","agent":"a","next":"join"}),
            );
            steps.insert("join".into(), json!({"type":"join"}));
            json!({"contract":"agsdl-0.4.0","id":"overlap","agents":{"a":{"harness":"codex","permissions":"read-only"}},
                "flow":{"entry":"start","steps":steps}}).to_string()
        }
        "expansion" => {
            let compose = |name: String, route: Value| json!({"type":"compose","composition":name,"next":{"done":route}});
            let compositions: Map<_, _> = (0..14).map(|i| {
                let output = json!({"output":"done"});
                let steps = if i == 13 {
                    json!({"one":{"type":"approval","title":"Continue?","next":{"yes":output,"no":[]}}})
                } else {
                    let body = format!("body{}", i + 1);
                    json!({"one":compose(body.clone(),json!("two")),"two":compose(body,output)})
                };
                (format!("body{i}"),json!({"entry":"one","outputs":["done"],"steps":steps}))
            }).collect();
            json!({"contract":"agsdl-0.4.0","id":"expansion","agents":{},"compositions":compositions,
                "flow":{"entry":"start","steps":{"start":compose("body0".into(),json!([]))}}}).to_string()
        }
        _ => unreachable!(),
    }
}

#[allow(clippy::result_large_err, reason = "Measure the API’s by-value error")]
fn measure_skill_tree() {
    let project = tempfile::tempdir().unwrap();
    let skill = project.path().join("skills/review");
    std::fs::create_dir_all(&skill).unwrap();
    std::fs::write(
        skill.join("SKILL.md"),
        "---\nname: review\ndescription: Review.\n---\n",
    )
    .unwrap();
    std::fs::write(
        project.path().join("agsdl.json"),
        r#"{"projectFormat":"agsdl-project-1","contract":"agsdl-0.4.0","id":"skill-budget","agents":{},"skills":{"review":{"uri":"skills/review/SKILL.md"}}}"#,
    ).unwrap();
    for index in 0..500 {
        std::fs::create_dir_all(skill.join(format!("branch-{index:03}")).join("empty")).unwrap();
    }
    let (opened, _) = measure(
        "skill tree with 1,000 empty or intermediate directories",
        |budget| OpenedSource::open_path_with_budget(project.path(), budget.clone()).unwrap(),
    );
    assert_eq!(opened.selected_files().unwrap().len(), 2);

    // Exhaust almost all the budget before opening. The walk must stop
    // before enumerating the whole tree or allocating its path collection.
    const REMAINING: usize = 256 * 1024;
    let budget = SourceBudget::default();
    budget.bytes(MAX_SOURCE_BYTES - REMAINING, "").unwrap();
    let (result, AllocationSample { total, peak, .. }) =
        allocation_sample(|| OpenedSource::open_path_with_budget(project.path(), budget));
    let error = result.err().expect("the walk exceeds the remaining budget");
    assert_eq!(error.code, GRAPH_LIMIT_CODE);
    assert_eq!(error.source.as_ref().unwrap().path, "skills/review");
    eprintln!(
        "nearly exhausted skill budget: {total} allocated, {peak} peak, {REMAINING} available"
    );
    assert!(
        total <= 2 * REMAINING,
        "{total} allocated with {REMAINING} available"
    );
    assert!(
        peak <= REMAINING,
        "{peak} peak bytes with {REMAINING} available"
    );
}

fn measure_public_pipeline(engine: &Engine, runtime: &tokio::runtime::Runtime) {
    let measure_size = |sections, asset_bytes| {
        let project = tempfile::tempdir().unwrap();
        let (authored, registry) = pipeline(sections, 16 * 1024);
        asset_project(
            project.path(),
            asset_bytes,
            serde_json::from_str(&authored).unwrap(),
        );
        let manifest = project.path().join("agsdl.json");
        let mut archive = Vec::new();
        OpenedSource::open_path(project.path())
            .unwrap()
            .write_archive(&mut archive)
            .unwrap();
        [
            ("folder", Some(project.path())),
            ("manifest", Some(manifest.as_path())),
            ("archive", None),
        ]
        .map(|(kind, path)| {
            let mut sample = AllocationSample {
                total: 0,
                peak: 0,
                cpu: Duration::MAX,
            };
            for _ in 0..3 {
                let (result, actual) = allocation_sample(|| {
                    let opened = match path {
                        Some(path) => OpenedSource::open_path(path),
                        None => OpenedSource::open_archive_bytes(&archive),
                    }
                    .unwrap();
                    let authored = opened.authored_json();
                    runtime
                        .block_on(engine.compile_document(
                            source_revision(authored),
                            authored.to_owned(),
                            registry.clone(),
                        ))
                        .unwrap()
                });
                let result: Value = serde_json::from_str(&result.result_json).unwrap();
                assert_eq!(
                    result["runtime"]["supported"], true,
                    "{kind}: {}",
                    result["runtime"]
                );
                assert_eq!(
                    result["plan"]["steps"].as_array().unwrap().len(),
                    sections * 12
                );
                sample.cpu = sample.cpu.min(actual.cpu);
                sample.total = sample.total.max(actual.total);
                sample.peak = sample.peak.max(actual.peak);
            }
            // The public methods each have their own allocation budget.
            assert!(
                sample.total <= 2 * MAX_SOURCE_BYTES,
                "{kind}: {} allocated",
                sample.total
            );
            assert!(
                sample.peak <= 2 * MAX_SOURCE_BYTES,
                "{kind}: {} peak",
                sample.peak
            );
            eprintln!(
                "public {kind}, {sections} sections: {} allocated, {} peak, {:?} process CPU",
                sample.total, sample.peak, sample.cpu
            );
            (kind, sample)
        })
    };
    runtime.block_on(async { tokio::task::spawn_blocking(|| ()).await.unwrap() });
    let small = measure_size(2, 64 * 1024);
    let large = measure_size(20, 640 * 1024);
    for ((kind, small), (_, large)) in small.into_iter().zip(large) {
        // Both the workflow and attachments grow by a factor of ten.
        assert!(
            large.total > small.total && large.total <= 15 * small.total,
            "{kind}: {} -> {} allocation bytes",
            small.total,
            large.total
        );
        assert!(
            large.cpu <= 40 * small.cpu,
            "{kind}: {:?} -> {:?} process CPU",
            small.cpu,
            large.cpu
        );
    }
}

fn has_limit(report: &str) -> bool {
    let json: Value = serde_json::from_str(report).unwrap();
    json["diagnostics"].as_array().unwrap().last().unwrap()["code"] == GRAPH_LIMIT_CODE
}

#[test]
fn source_operations_keep_allocation_and_work_proportional() {
    // Initialize the embedded schemas and regexes before measuring per-input
    // work. Their one-time allocation is independent of the supplied source.
    validate_document_json_with_budget(
        br#"{"contract":"agsdl-0.4.0","id":"warm","agents":{"a":{"harness":"codex","permissions":"read-only"}}}"#,
        None, &SourceBudget::default(),
    ).unwrap();

    measure_skill_tree();

    let small = tempfile::tempdir().unwrap();
    let large = tempfile::tempdir().unwrap();
    let sizes = [256 * 1024, 2560 * 1024];
    for (root, size) in [small.path(), large.path()].into_iter().zip(sizes) {
        asset_project(
            root,
            size,
            json!({
                "contract":"agsdl-0.4.0","id":"assets",
                "agents":{"a":{"harness":"codex","permissions":"read-only"}}
            }),
        );
    }
    let assets = |path, label| {
        let (opened, stats) = measure(label, |budget| {
            OpenedSource::open_path_with_budget(path, budget.clone()).unwrap()
        });
        assert_eq!(opened.selected_files().unwrap().len(), 5);
        (opened, stats)
    };
    let (small_source, small_open) = assets(small.path(), "folder 1 MiB");
    let (large_source, large_open) = assets(large.path(), "folder 10 MiB");
    assert!(large_open.charged - small_open.charged >= 4 * (sizes[1] - sizes[0]));
    scales("folder assets", small_open, large_open);
    for (kind, pack) in [
        (
            "stored",
            (|source: &OpenedSource| {
                let mut bytes = Vec::new();
                source.write_archive(&mut bytes).unwrap();
                bytes
            }) as fn(&OpenedSource) -> Vec<u8>,
        ),
        ("deflate", deflated_archive),
    ] {
        let archive = |source: &OpenedSource, size| {
            let bytes = pack(source);
            let label = format!("{kind} archive {size}");
            let (opened, stats) = measure(&label, |budget| {
                OpenedSource::open_archive_bytes_with_budget(&bytes, budget.clone()).unwrap()
            });
            assert_eq!(opened.authored_json(), source.authored_json());
            stats
        };
        let small = archive(&small_source, "1 MiB");
        let large = archive(&large_source, "10 MiB");
        assert!(large.charged - small.charged >= 4 * (sizes[1] - sizes[0]));
        scales(kind, small, large);
    }

    for name in [
        "depth",
        "expansion",
        "warnings",
        "overlap",
        "permissions",
        "shape",
        "approval paths",
        "long fanout",
    ] {
        let input = generated(name);
        let (report, _) = measure(name, |budget| {
            validate_document_json_with_budget(input.as_bytes(), None, budget).unwrap()
        });
        match name {
            "approval paths" => assert!(report.contains("\"code\":\"PROMPT\""), "{report}"),
            "long fanout" => assert_eq!(report.matches("\"code\":\"FANOUT_VISITS\"").count(), 250),
            _ => assert!(has_limit(&report), "{name}: {report}"),
        }
    }
    let (doc, module) = escaped_warnings();
    let (report, _) = measure("escaped report", |budget| {
        validate_document_json_with_budget(doc.as_bytes(), Some(&[module.as_bytes()]), budget)
            .unwrap()
    });
    assert!(has_limit(&report));
    assert!(report.len() <= 512 * 1024);

    let mut scaled = Vec::new();
    for (rows, fields, escaped) in [
        (4, 32, false),
        (40, 32, false),
        (48, 32, false),
        (220, 1, true),
    ] {
        let (doc, module) = settings(rows, fields, escaped);
        let (report, stats) = measure(
            &format!("settings {rows}×{fields}, escaped={escaped}"),
            |budget| {
                validate_document_json_with_budget(
                    doc.as_bytes(),
                    Some(&[module.as_bytes()]),
                    budget,
                )
                .unwrap()
            },
        );
        let json: Value = serde_json::from_str(&report).unwrap();
        assert!(report.len() <= 512 * 1024);
        assert_eq!(has_limit(&report), rows * fields > 1024);
        let diagnostics = json["diagnostics"].as_array().unwrap();
        let missing = diagnostics
            .iter()
            .filter(|item| item["code"] == "HARNESS_SETTINGS")
            .count();
        let others = diagnostics.len() - missing - usize::from(has_limit(&report));
        assert_eq!(missing, (rows * fields).min(1024 - others));
        if rows == 4 || rows == 40 {
            scaled.push(stats);
        }
    }
    let large = scaled.pop().unwrap();
    scales("settings objects", scaled.pop().unwrap(), large);

    let selection = |count, digits| {
        let doc = selections(count, digits);
        let (report, stats) = measure(
            &format!("{count} selections against {digits} digits"),
            |budget| validate_document_json_with_budget(doc.as_bytes(), None, budget).unwrap(),
        );
        assert!(!report.contains("MESSAGE_PATH"), "{report}");
        assert!(!report.contains(GRAPH_LIMIT_CODE), "{report}");
        stats
    };
    let short = selection(40, 16);
    let long = selection(40, 8192);
    assert!(long.work - short.work > 40 * ((8192 - 16) / 8));
    scales("exact numeric selections", selection(4, 8192), long);

    let compile = |sections, evidence_bytes| {
        let (doc, registry) = pipeline(sections, evidence_bytes);
        let source = revision(&doc).to_string();
        let (result, stats) = measure(&format!("compile {} agents", sections * 10), |budget| {
            calculation::compile_with_budget(&doc, &source, &registry, budget).unwrap()
        });
        let json: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(json["runtime"]["supported"], true, "{}", json["runtime"]);
        assert_eq!(
            json["plan"]["steps"].as_array().unwrap().len(),
            sections * 12
        );
        stats
    };
    scales("pipeline", compile(2, 16 * 1024), compile(20, 16 * 1024));
    compile(4, 256 * 1024);

    // The same native host entry point a desktop caller uses, with a registry
    // property longer than the core lexer allows and a valid oversized project.
    let engine = Engine::new();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    measure_public_pipeline(&engine, &runtime);

    let (doc, registry) = pipeline(1, 32);
    let mut invalid: Value = serde_json::from_str(&registry).unwrap();
    invalid["x".repeat(4097)] = true.into();
    let failure = runtime
        .block_on(engine.compile_document(source_revision(&doc), doc.clone(), invalid.to_string()))
        .err()
        .expect("registry key limit");
    assert_eq!(
        (failure.code.as_str(), failure.pointer.as_str()),
        (GRAPH_LIMIT_CODE, "/registry")
    );

    // The input check only scans this registry a fixed number of times. The
    // native decoder revisits each enclosing raw array and spends the budget.
    let mut nested = Value::String("a".repeat(1_600_000));
    for _ in 0..90 {
        nested = Value::Array(vec![nested]);
    }
    let mut deep: Value = serde_json::from_str(&registry).unwrap();
    deep["extra"] = nested;
    let failure = runtime
        .block_on(engine.compile_document(source_revision(&doc), doc.clone(), deep.to_string()))
        .err()
        .expect("native decoding work limit");
    assert_eq!(
        (failure.code.as_str(), failure.pointer.as_str()),
        (GRAPH_LIMIT_CODE, "/registry")
    );

    let mut registry_with_decimal: Value = serde_json::from_str(&registry).unwrap();
    registry_with_decimal["harnesses"][0]["observationExtension"] =
        serde_json::from_str("1.0").unwrap();
    let public = runtime
        .block_on(engine.compile_document(
            source_revision(&doc),
            doc.clone(),
            registry_with_decimal.to_string(),
        ))
        .unwrap();
    let public: Value = serde_json::from_str(&public.result_json).unwrap();
    assert_eq!(
        public["runtime"]["supported"], true,
        "{}",
        public["runtime"]
    );

    let project = tempfile::tempdir().unwrap();
    let mut content = Map::new();
    for i in 0..4 {
        let name = format!("part{i}.json");
        content.insert(format!("part{i}"), json!({"file":name}));
        std::fs::write(
            project.path().join(name),
            json!({"value":"x".repeat(1_020_000)}).to_string(),
        )
        .unwrap();
    }
    std::fs::write(project.path().join("agsdl.json"), json!({"projectFormat":"agsdl-project-1",
        "contract":"agsdl-0.4.0","id":"pipeline","agents":{"a":{"harness":"codex","permissions":"read-only"}},
        "content":content}).to_string()).unwrap();
    let (opened, _) = measure("four large components", |budget| {
        OpenedSource::open_path_with_budget(project.path(), budget.clone()).unwrap()
    });
    let doc = opened.authored_json().to_owned();
    assert!(doc.len() > 4_000_000);
    let failure = runtime
        .block_on(engine.compile_document(source_revision(&doc), doc, registry))
        .err()
        .expect("compile transport limit");
    assert_eq!(
        (failure.code.as_str(), failure.pointer.as_str()),
        ("INPUT_LIMIT", "/source")
    );
}

fn source_revision(doc: &str) -> EngineSourceRevision {
    serde_json::from_value(revision(doc)).unwrap()
}
