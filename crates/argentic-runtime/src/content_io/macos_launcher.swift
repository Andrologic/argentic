// Native launch bridge for the AgSDL content helper. The host separately
// admits reads and owns cancellation, termination and reap.
import Darwin
import Foundation
import Security

private let helperArgument = "--macro-internal-agsdl-content-read"

@_cdecl("agsdl_content_macos_available")
public func agsdlContentMacosAvailable() -> Int32 {
    if #available(macOS 14.4, *) { return 1 }
    return 0
}

#if AGSDL_CONTENT_TESTING
// The harness uses this boundary to check inheritance during an unrelated exec.
var controlPipeCreatedHook: ((Int32, Int32) -> Void)?

private let qaSwapLock = NSLock()
private var qaPendingSwap: (target: String, replacement: String)?

// Arms one swap for the dedicated QA image. The replacement is applied only
// after spawn setup, immediately before the kernel loads the executable.
@_cdecl("agsdl_content_macos_qa_arm_swap")
public func agsdlContentMacosQaArmSwap(
    _ target: UnsafePointer<CChar>?, _ replacement: UnsafePointer<CChar>?
) -> Int32 {
    guard let target, let replacement,
          let targetPath = String(validatingUTF8: target),
          let replacementPath = String(validatingUTF8: replacement),
          targetPath.hasPrefix("/"), replacementPath.hasPrefix("/") else { return 1 }
    qaSwapLock.lock()
    defer { qaSwapLock.unlock() }
    guard qaPendingSwap == nil else { return 1 }
    qaPendingSwap = (targetPath, replacementPath)
    return 0
}

private func applyQaSwap(_ executablePath: String) -> Bool {
    qaSwapLock.lock()
    let swap = qaPendingSwap
    qaPendingSwap = nil
    qaSwapLock.unlock()
    guard let swap else { return true }
    guard swap.target == executablePath else { return false }
    return rename(swap.replacement, swap.target) == 0
}
#endif

// O_CLOEXEC is applied by openat atomically, including on macOS before pipe2.
// Keep both ends above stdio so the spawn dup2 actions cannot clobber a source.
private func openPipeEnd(_ root: Int32, _ flags: Int32) -> Int32 {
    let fd = openat(root, "pipe", flags | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC)
    if fd < 0 || fd >= 3 { return fd }
    defer { close(fd) }
    return fcntl(fd, F_DUPFD_CLOEXEC, 3)
}

// The FIFO is removed before the descriptors leave this function.
@available(macOS 13.0, *)
private func makePipe() -> (Int32, Int32)? {
    let path = (NSTemporaryDirectory() as NSString).appendingPathComponent("macro-content-XXXXXX")
    var template = Array(path.utf8CString)
    guard mkdtemp(&template) != nil else { return nil }
    let directory = String(cString: template)
    defer { _ = rmdir(directory) }
    let root = open(directory, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
    guard root >= 0 else { return nil }
    defer { close(root) }
    guard mkfifoat(root, "pipe", mode_t(S_IRUSR | S_IWUSR)) == 0 else { return nil }
    defer { _ = unlinkat(root, "pipe", 0) }
    let reader = openPipeEnd(root, O_RDONLY)
    guard reader >= 0 else { return nil }
    let writer = openPipeEnd(root, O_WRONLY)
    guard writer >= 0 else { close(reader); return nil }
    var readInfo = stat()
    var writeInfo = stat()
    guard fstat(reader, &readInfo) == 0, fstat(writer, &writeInfo) == 0,
          readInfo.st_mode & S_IFMT == S_IFIFO,
          writeInfo.st_mode & S_IFMT == S_IFIFO,
          readInfo.st_dev == writeInfo.st_dev, readInfo.st_ino == writeInfo.st_ino else {
        close(reader)
        close(writer)
        return nil
    }
    // Do not hand out a writer if the FIFO still has a filesystem name: an
    // additional writer could otherwise prevent cancellation by EOF.
    guard unlinkat(root, "pipe", 0) == 0, fstat(reader, &readInfo) == 0,
          readInfo.st_nlink == 0 else {
        close(reader)
        close(writer)
        return nil
    }
    return (reader, writer)
}

// Signing information may come from mutable on-disk code. It is only a
// candidate until dynamic validation binds its hash to the running process.
private func runningCodeRequirement() -> SecRequirement? {
    var selfCode: SecCode?
    guard SecCodeCopySelf(SecCSFlags(), &selfCode) == errSecSuccess,
          let selfCode else { return nil }
    var staticCode: SecStaticCode?
    guard SecCodeCopyStaticCode(selfCode, SecCSFlags(), &staticCode) == errSecSuccess,
          let code = staticCode else { return nil }
    var information: CFDictionary?
    guard SecCodeCopySigningInformation(code, SecCSFlags(rawValue: kSecCSSigningInformation),
                                        &information) == errSecSuccess,
          let dictionary = information as? [String: Any],
          let cdhash = dictionary[kSecCodeInfoUnique as String] as? Data,
          !cdhash.isEmpty else { return nil }
    let hex = cdhash.map { String(format: "%02x", $0) }.joined()
    let expression = "cdhash H\"\(hex)\"" as CFString
    var requirement: SecRequirement?
    guard SecRequirementCreateWithString(expression, SecCSFlags(), &requirement) == errSecSuccess,
          let requirement,
          SecCodeCheckValidity(selfCode, SecCSFlags(), requirement) == errSecSuccess else {
        return nil
    }
    return requirement
}

private func hasRequiredCode(_ pid: pid_t, _ requirement: SecRequirement) -> Bool {
    let attributes = [kSecGuestAttributePid as String: NSNumber(value: pid)] as CFDictionary
    var guest: SecCode?
    return SecCodeCopyGuestWithAttributes(nil, attributes, SecCSFlags(), &guest) == errSecSuccess
        && guest.map { SecCodeCheckValidity($0, SecCSFlags(), requirement) == errSecSuccess } == true
}

private func terminateAndReap(_ pid: pid_t) {
    _ = kill(pid, SIGKILL)
    var status: Int32 = 0
    while waitpid(pid, &status, 0) == -1 && errno == EINTR {}
}

// These status values are internal to the native bridge.
// 0: launched; 1: invalid input; 2: unsupported OS or host signing identity;
// 3: pipe setup failed; 4: spawn failed; 6: QA swap failed; 7: helper identity refused.
@_cdecl("agsdl_content_macos_launch")
public func agsdlContentMacosLaunch(
    _ executable: UnsafePointer<CChar>?,
    _ component: UnsafePointer<CChar>?,
    _ parentFD: Int32,
    _ result: UnsafeMutablePointer<pid_t>?,
    _ outputFD: UnsafeMutablePointer<Int32>?,
    _ controlFD: UnsafeMutablePointer<Int32>?
) -> Int32 {
    guard let result, let outputFD, let controlFD else { return 1 }
    result.pointee = -1
    outputFD.pointee = -1
    controlFD.pointee = -1
    guard let executable, let component, parentFD >= 3,
          let executablePath = String(validatingUTF8: executable),
          let name = String(validatingUTF8: component),
          executablePath.hasPrefix("/"), !name.isEmpty,
          name != ".", name != "..", !name.contains("/") else { return 1 }
    guard #available(macOS 14.4, *), let requirement = runningCodeRequirement() else { return 2 }

    guard let (controlRead, controlWrite) = makePipe() else { return 3 }
    guard let (outputRead, outputWrite) = makePipe() else {
        close(controlRead)
        close(controlWrite)
        return 3
    }
    var transferred = false
    defer {
        close(controlRead)
        if !transferred {
            close(controlWrite)
            close(outputRead)
        }
    }
    var writerOpen = true
    defer { if writerOpen { close(outputWrite) } }
    #if AGSDL_CONTENT_TESTING
    controlPipeCreatedHook?(controlRead, controlWrite)
    #endif
    // The helper writes synchronously. The supervisor keeps its reader nonblocking.
    guard fcntl(outputWrite, F_SETFL, 0) == 0 else { return 3 }

    var attributes: posix_spawnattr_t?
    guard posix_spawnattr_init(&attributes) == 0 else { return 4 }
    defer { posix_spawnattr_destroy(&attributes) }
    var actions: posix_spawn_file_actions_t?
    guard posix_spawn_file_actions_init(&actions) == 0 else { return 4 }
    defer { posix_spawn_file_actions_destroy(&actions) }
    // Like Process, give the child default dispositions and an empty mask.
    var emptyMask = sigset_t()
    var allSignals = sigset_t()
    sigemptyset(&emptyMask)
    sigfillset(&allSignals)
    let flags = Int16(POSIX_SPAWN_START_SUSPENDED | POSIX_SPAWN_CLOEXEC_DEFAULT
        | POSIX_SPAWN_SETPGROUP | POSIX_SPAWN_SETSIGMASK | POSIX_SPAWN_SETSIGDEF)
    guard posix_spawnattr_setflags(&attributes, flags) == 0,
          posix_spawnattr_setpgroup(&attributes, 0) == 0,
          posix_spawnattr_setsigmask(&attributes, &emptyMask) == 0,
          posix_spawnattr_setsigdefault(&attributes, &allSignals) == 0,
          posix_spawn_file_actions_adddup2(&actions, parentFD, STDIN_FILENO) == 0,
          posix_spawn_file_actions_adddup2(&actions, outputWrite, STDOUT_FILENO) == 0,
          posix_spawn_file_actions_adddup2(&actions, controlRead, STDERR_FILENO) == 0 else { return 4 }

    var argv = [strdup(executable), strdup(helperArgument), strdup("0"), strdup(name), nil]
    defer { for arg in argv { free(arg) } }
    guard argv.dropLast().allSatisfy({ $0 != nil }) else { return 4 }
    var envp: [UnsafeMutablePointer<CChar>?] = [nil]
    #if AGSDL_CONTENT_TESTING
    guard applyQaSwap(executablePath) else { return 6 }
    #endif

    var pid: pid_t = 0
    guard posix_spawn(&pid, executable, &actions, &attributes, &argv, &envp) == 0 else { return 4 }
    close(outputWrite)
    writerOpen = false
    // The kernel has selected and loaded the image, but the task cannot execute
    // user code until resumed. Check the child's dynamic identity before SIGCONT.
    guard hasRequiredCode(pid, requirement) else {
        terminateAndReap(pid)
        #if AGSDL_CONTENT_TESTING
        var byte: UInt8 = 0
        // The child and parent writers are closed. A refused image must emit no bytes.
        guard read(outputRead, &byte, 1) == 0 else { return 4 }
        #endif
        return 7
    }
    guard kill(pid, SIGCONT) == 0 else {
        terminateAndReap(pid)
        return 4
    }
    result.pointee = pid
    outputFD.pointee = outputRead
    controlFD.pointee = controlWrite
    transferred = true
    return 0
}
