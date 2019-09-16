package xdag

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
)

const (
	xdagStoreFolder  = "storage"
	xdagStoreTestNet = "storage-testnet"
	storeFileExt     = ".dat"
)

// IsTestNet whether using testnet mode
var IsTestNet = true

func makeDir1(t uint64) string {
	var dir string
	if IsTestNet {
		dir = xdagStoreTestNet
	} else {
		dir = xdagStoreFolder
	}
	subdir := fmt.Sprintf("%02x", uint8(t>>40))

	return path.Join(dir, subdir)
}

func makeDir2(t uint64) string {

	dir := makeDir1(t)
	subdir := fmt.Sprintf("%02x", uint8(t>>32))

	return path.Join(dir, subdir)
}

func makeDir3(t uint64) string {

	dir := makeDir2(t)
	subdir := fmt.Sprintf("%02x", uint8(t>>24))

	return path.Join(dir, subdir)
}

func makeFile(t uint64) string {

	dir := makeDir3(t)
	subdir := fmt.Sprintf("%02x", uint8(t>>16))

	return path.Join(dir, subdir) + storeFileExt
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	if err != nil {
		return false
	}

	return true
}

// LoadBlocks loads blocks from XDAG storage, ignore check sum
func LoadBlocks(startTime, endTime uint64) ([]RawBlock, error) {
	var blocks []RawBlock
	var mask uint64

	for startTime < endTime {
		path := makeFile(startTime)
		file, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
		defer file.Close()
		if file != nil && err == nil {
			// fmt.Println(path)

			fileinfo, err := file.Stat()
			if err != nil {
				return nil, err
			}
			n := fileinfo.Size()     // file size
			if n%RawBlockSize != 0 { // n should be integral multiple of block size
				return nil, errors.New("file size error")
			}
			count := int(n / RawBlockSize) // number of blocks in file
			// fmt.Println(count)
			for i := 0; i < count; i++ {
				var buffer bytes.Buffer

				_, err = io.CopyN(&buffer, file, RawBlockSize) // read a block
				if err != nil {
					// not EOF
					return nil, err
				}
				b := NewRawBlock(buffer.Bytes())

				blocks = append(blocks, b)
			}
			mask = (uint64(1) << 16) - 1
		} else if fileExists(makeDir3(startTime)) {
			mask = (uint64(1) << 16) - 1
		} else if fileExists(makeDir2(startTime)) {
			mask = (uint64(1) << 24) - 1
		} else if fileExists(makeDir1(startTime)) {
			mask = (uint64(1) << 32) - 1
		} else {
			mask = (uint64(1) << 40) - 1
		}
		startTime |= mask
		startTime++

	}
	return blocks, nil
}
