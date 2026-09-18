package discover

import "os"

// OSFS uses the real filesystem.
type OSFS struct{}

// Stat implements FS via os.Stat.
func (OSFS) Stat(path string) (FileInfo, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return FileInfo{Exists: false}, nil
		}
		return FileInfo{}, err
	}
	return FileInfo{
		Exists: true,
		Size:   info.Size(),
		IsDir:  info.IsDir(),
	}, nil
}

// ReadDir implements FS via os.ReadDir.
func (OSFS) ReadDir(path string) ([]string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names, nil
}
