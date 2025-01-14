package interstate

import (
	"errors"
	"fmt"
	"os"
)

func fileExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	if info.IsDir() {
		return false, fmt.Errorf("path is a directory")
	}

	return true, nil
}
