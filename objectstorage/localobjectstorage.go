package objectstorage

import (
	"io"
	"os"
	"path"
	"strings"

	"github.com/google/uuid"
	"github.com/rptynan/delta-lake/utils"
)

type fileObjectStorage struct {
	basedir string
}

func NewFileObjectStorage(basedir string) *fileObjectStorage {
	return &fileObjectStorage{basedir}
}

func (fos *fileObjectStorage) PutIfAbsent(name string, bytes []byte) error {
	tmpfilename := path.Join(fos.basedir, uuid.New().String())
	f, err := os.OpenFile(tmpfilename, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	written := 0
	bufSize := 1024 * 16
	for written < len(bytes) {
		toWrite := min(written+bufSize, len(bytes))
		n, err := f.Write(bytes[written:toWrite])
		if err != nil {
			removeErr := os.Remove(tmpfilename)
			utils.Assert(removeErr == nil, "could not remove")
			return err
		}

		written += n
	}

	err = f.Sync()
	if err != nil {
		removeErr := os.Remove(tmpfilename)
		utils.Assert(removeErr == nil, "could not remove")
		return err
	}

	err = f.Close()
	if err != nil {
		removeErr := os.Remove(tmpfilename)
		utils.Assert(removeErr == nil, "could not remove")
		return err
	}

	filename := path.Join(fos.basedir, name)
	err = os.Link(tmpfilename, filename)
	if err != nil {
		removeErr := os.Remove(tmpfilename)
		utils.Assert(removeErr == nil, "could not remove")
		return err
	}

	return nil
}

func (fos *fileObjectStorage) listPrefix(prefix string) ([]string, error) {
	dir := path.Join(fos.basedir)
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}

	var files []string
	for err != io.EOF {
		var names []string
		names, err = f.Readdirnames(100)
		if err != nil && err != io.EOF {
			return nil, err
		}

		for _, n := range names {
			if prefix == "" || strings.HasPrefix(n, prefix) {
				files = append(files, n)
			}
		}
	}
	err = f.Close()
	return files, err
}

func (fos *fileObjectStorage) read(name string) ([]byte, error) {
	filename := path.Join(fos.basedir, name)
	return os.ReadFile(filename)
}
