using System;
using System.Collections.Generic;
using System.IO;

namespace CloudRedirect.Services.Patching
{
    /// <summary>
    /// Installs CloudRedirect autoload under OpenSteamTool. Rewrites the
    /// "OpenSteamTool.dll" string in OST's hijack DLLs (xinput1_4.dll, dwmapi.dll)
    /// to "cr_loader.dll"; the same call site then loads our chain-loader stub,
    /// which loads the real OpenSteamTool.dll (unchanged at its natural filename)
    /// and cloud_redirect.dll.
    /// </summary>
    internal static class OstLoaderPatcher
    {
        const string LoaderName = "cr_loader.dll";
        const string LoaderResource = "cr_loader.dll";

        static readonly string[] HijackCandidates = { "xinput1_4.dll", "dwmapi.dll" };

        // "OpenSteamTool.dll" -- 17 bytes (no null). Used to locate the slot.
        static readonly byte[] OstStringBytes =
            System.Text.Encoding.ASCII.GetBytes("OpenSteamTool.dll");

        // "cr_loader.dll\0" -- 14 bytes. Written into the 18-byte slot
        // occupied by "OpenSteamTool.dll\0"; trailing bytes are zeroed.
        static readonly byte[] LoaderStringBytes =
            System.Text.Encoding.ASCII.GetBytes("cr_loader.dll\0");

        const int OstSlotBytes = 18; // "OpenSteamTool.dll" + null

        static List<string> FindHijackDlls(string steamPath)
        {
            var result = new List<string>();
            foreach (var name in HijackCandidates)
            {
                var path = Path.Combine(steamPath, name);
                if (!File.Exists(path)) continue;
                byte[] data;
                try { data = File.ReadAllBytes(path); }
                catch (IOException) { continue; }
                bool hasOstString = Signatures.ScanForBytes(data, 0, data.Length, OstStringBytes) >= 0;
                bool hasLoaderMarker = Signatures.ScanForBytes(data, 0, data.Length, LoaderStringBytes) >= 0;
                if (hasOstString || hasLoaderMarker) result.Add(path);
            }
            return result;
        }

        public static PatchResult Apply(string steamPath, Action<string> log = null)
        {
            var result = new PatchResult();

            var loaderBytes = LoadLoaderResource();
            if (loaderBytes == null || loaderBytes.Length == 0)
                return result.Fail("cr_loader.dll resource missing from launcher build.");

            var hijacks = FindHijackDlls(steamPath);
            if (hijacks.Count == 0)
                return result.Fail("OpenSteamTool hijack DLLs not found in Steam folder.");

            int patched = 0, already = 0;
            foreach (var path in hijacks)
            {
                var name = Path.GetFileName(path);
                try
                {
                    var bytes = File.ReadAllBytes(path);

                    if (Signatures.ScanForBytes(bytes, 0, bytes.Length, LoaderStringBytes) >= 0)
                    {
                        log?.Invoke($"  {name}: string already patched");
                        already++;
                        continue;
                    }

                    int strOffset = Signatures.ScanForBytes(bytes, 0, bytes.Length, OstStringBytes);
                    if (strOffset < 0)
                    {
                        log?.Invoke($"  {name}: OpenSteamTool.dll string not found, skipping");
                        continue;
                    }
                    // Slot must include the trailing null.
                    if (strOffset + OstSlotBytes > bytes.Length ||
                        bytes[strOffset + OstStringBytes.Length] != 0)
                    {
                        log?.Invoke($"  {name}: string not null-terminated where expected, skipping");
                        continue;
                    }

                    // Overwrite the full 18-byte slot so trailing bytes of the
                    // original "OpenSteamTool.dll" don't linger past the new null.
                    var slot = new byte[OstSlotBytes];
                    Buffer.BlockCopy(LoaderStringBytes, 0, slot, 0, LoaderStringBytes.Length);
                    Buffer.BlockCopy(slot, 0, bytes, strOffset, OstSlotBytes);

                    FileUtils.AtomicWriteAllBytes(path, bytes);
                    log?.Invoke($"  {name}: string patched -> cr_loader.dll");
                    patched++;
                }
                catch (Exception ex)
                {
                    return result.Fail($"{name}: {ex.Message}");
                }
            }

            if (patched == 0 && already == 0)
                return result.Fail("No OST hijack DLL had a patchable string.");

            try
            {
                var loaderDest = Path.Combine(steamPath, LoaderName);
                FileUtils.AtomicWriteAllBytes(loaderDest, loaderBytes);
                log?.Invoke($"  {LoaderName}: deployed ({loaderBytes.Length} bytes)");
            }
            catch (Exception ex)
            {
                return result.Fail($"{LoaderName} deploy failed: {ex.Message}");
            }

            result.Succeeded = true;
            result.DllPatched = patched > 0;
            return result;
        }

        static byte[] LoadLoaderResource()
        {
            using var stream = typeof(OstLoaderPatcher).Assembly
                .GetManifestResourceStream(LoaderResource);
            if (stream == null) return null;
            using var ms = new MemoryStream(checked((int)stream.Length));
            stream.CopyTo(ms);
            return ms.ToArray();
        }
    }
}
