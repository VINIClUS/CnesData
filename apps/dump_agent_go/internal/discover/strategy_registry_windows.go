//go:build windows

package discover

import (
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows/registry"
)

// RegistryHits queries:
//   - HKLM\<RegistryVendorKey> → InstallLocation
//   - HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\* filtered
//     by DisplayName ~ Profile.UninstallDisplayMatch → InstallLocation
//
// Returns paths only. Caller scores via Score() after Stat checks.
// WOW64_64KEY + WOW64_32KEY views are both queried.
func RegistryHits(p Profile) []RegistryHit {
	hits := append([]RegistryHit(nil), vendorKeyHit(p)...)
	hits = append(hits, uninstallKeyHits(p)...)
	return hits
}

func vendorKeyHit(p Profile) []RegistryHit {
	if p.RegistryVendorKey == "" {
		return nil
	}
	loc := readInstallLocation(p.RegistryVendorKey)
	if loc == "" {
		return nil
	}
	return []RegistryHit{{Path: composePath(p, loc)}}
}

func uninstallKeyHits(p Profile) []RegistryHit {
	if p.UninstallDisplayMatch == "" {
		return nil
	}
	const root = `SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall`
	hits := append([]RegistryHit(nil),
		walkUninstallRoot(p, root, registry.WOW64_64KEY)...)
	hits = append(hits, walkUninstallRoot(p, root, registry.WOW64_32KEY)...)
	return hits
}

func walkUninstallRoot(p Profile, root string, view uint32) []RegistryHit {
	k, err := registry.OpenKey(registry.LOCAL_MACHINE, root,
		registry.READ|view)
	if err != nil {
		return nil
	}
	defer k.Close()
	subs, err := k.ReadSubKeyNames(0)
	if err != nil {
		return nil
	}
	var hits []RegistryHit
	for _, sub := range subs {
		if hit, ok := scanUninstallEntry(p, root+`\`+sub, view); ok {
			hits = append(hits, hit)
		}
	}
	return hits
}

func scanUninstallEntry(p Profile, fullPath string, view uint32) (RegistryHit, bool) {
	k, err := registry.OpenKey(registry.LOCAL_MACHINE, fullPath,
		registry.READ|view)
	if err != nil {
		return RegistryHit{}, false
	}
	defer k.Close()
	dn, _, err := k.GetStringValue("DisplayName")
	if err != nil || !strings.Contains(strings.ToUpper(dn),
		strings.ToUpper(p.UninstallDisplayMatch)) {
		return RegistryHit{}, false
	}
	loc, _, err := k.GetStringValue("InstallLocation")
	if err != nil || loc == "" {
		return RegistryHit{}, false
	}
	return RegistryHit{Path: composePath(p, loc)}, true
}

func readInstallLocation(keyPath string) string {
	for _, view := range []uint32{registry.WOW64_64KEY, registry.WOW64_32KEY} {
		if v := readInstallLocationView(keyPath, view); v != "" {
			return v
		}
	}
	return ""
}

func readInstallLocationView(keyPath string, view uint32) string {
	k, err := registry.OpenKey(registry.LOCAL_MACHINE, keyPath,
		registry.READ|view)
	if err != nil {
		return ""
	}
	defer k.Close()
	loc, _, _ := k.GetStringValue("InstallLocation")
	return loc
}

func composePath(p Profile, installDir string) string {
	if p.Source == SourceSIA {
		return installDir
	}
	if p.PrimaryFilename == "" {
		return installDir
	}
	return filepath.Join(installDir, p.PrimaryFilename)
}
