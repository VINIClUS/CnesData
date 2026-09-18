package discover

// Profile holds per-source discovery hints used by registry, FS-template,
// and FS-walk strategies.
type Profile struct {
	Source                SourceID
	RegistryVendorKey     string
	UninstallDisplayMatch string
	PrimaryFilename       string
	FileExtension         string
	FSTemplates           []string
	SIAExpectedDBFs       []string
}

// ProfileFor returns the discovery Profile for the given source.
func ProfileFor(s SourceID) Profile {
	switch s {
	case SourceCNES:
		return Profile{
			Source:                SourceCNES,
			RegistryVendorKey:     `SOFTWARE\Datasus\CNES`,
			UninstallDisplayMatch: "CNES",
			PrimaryFilename:       "CNES.GDB",
			FileExtension:         ".GDB",
			FSTemplates: []string{
				`<DRIVE>\Datasus\CNES\CNES.GDB`,
				`<DRIVE>\CNES\CNES.GDB`,
				`<DRIVE>\BASE\CNES.GDB`,
			},
		}
	case SourceSIHD:
		return Profile{
			Source:                SourceSIHD,
			RegistryVendorKey:     `SOFTWARE\Datasus\SIHD`,
			UninstallDisplayMatch: "SIHD",
			PrimaryFilename:       "SIHD.GDB",
			FileExtension:         ".GDB",
			FSTemplates: []string{
				`<DRIVE>\Datasus\SIHD\SIHD.GDB`,
				`<DRIVE>\SIHD\SIHD.GDB`,
			},
		}
	case SourceBPA:
		return Profile{
			Source:                SourceBPA,
			RegistryVendorKey:     `SOFTWARE\Datasus\BPAMAG`,
			UninstallDisplayMatch: "BPA",
			PrimaryFilename:       "BPAMAG.GDB",
			FileExtension:         ".GDB",
			FSTemplates: []string{
				`<DRIVE>\Datasus\BPAMAG\BPAMAG.GDB`,
				`<DRIVE>\BPAMAG\BPAMAG.GDB`,
				`<DRIVE>\BPA\BPAMAG.GDB`,
			},
		}
	case SourceSIA:
		return Profile{
			Source:                SourceSIA,
			RegistryVendorKey:     `SOFTWARE\Datasus\SIA`,
			UninstallDisplayMatch: "SIA",
			PrimaryFilename:       "",
			FileExtension:         ".DBF",
			FSTemplates: []string{
				`<DRIVE>\Datasus\SIA`,
				`<DRIVE>\SIA`,
			},
			SIAExpectedDBFs: []string{"S_APA", "S_BPI", "S_BPIHST", "S_CDN", "CADMUN"},
		}
	default:
		return Profile{Source: SourceUnknown}
	}
}
