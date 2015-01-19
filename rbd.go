/*
	These are librbd bindings for the Go programming language.

	For Go librados bindings, please visit:
		https://github.com/clbh/go-rados

	To obtain the Ceph source from which librbd can be built,
	please visit:
		https://github.com/ceph/ceph

	Authors:
		Benoit Page-Guitard (benoit@anchor.net.au)

	License:
		GNU General Public License v3
		http://www.gnu.org/licenses/gpl.html
*/

package gorbd

// #cgo LDFLAGS: -lrbd -lrados
// #include <rbd/librbd.h>
import "C"

import (
	"errors"

	rados "github.com/clbh/go-rados"
)

// Our bindings version
const VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH = 1, 0, 1

// Exported types
type Image struct {
	handle C.rbd_image_t
	name   string
}

type ImageInfo struct {
	size     uint64
	obj_size uint64
	num_objs uint64
	order    int
}

type Snapshot struct {
	handle *C.rbd_snap_info_t
}

////
//   Library version querying
////

func LibraryVersion() (major, minor, extra int) {
	var c_major, c_minor, c_extra C.int

	C.rbd_version(&c_major, &c_minor, &c_extra)

	return int(c_major), int(c_minor), int(c_extra)
}

////
//   Pool operations
////

func ListImages(pool *rados.Pool) ([]string, error) {
	var buf [4096]C.char
	var size C.size_t = 4096

	result := C.rbd_list(C.rados_ioctx_t(pool.Handle()), &buf[0], &size)
	if result < 0 {
		return []string{}, errors.New("Failed to fetch image list from pool")
	}

	start := 0
	images := make([]string, 0)

	for x := 0; x < int(result); x++ {
		if buf[x] == 0x0 {
			// Detected end of string. Store image name
			images = append(images, C.GoStringN(&buf[start], C.int(x-start)))

			// Reset start marker for next image name
			start = x + 1
		}
	}

	return images, nil
}

////
//   Image methods
////
func OpenImage(pool *rados.Pool, name string) (*Image, error) {
	var handle C.rbd_image_t

	if result := C.rbd_open(C.rados_ioctx_t(pool.Handle()), C.CString(name), &handle, nil); result < 0 {
		return nil, errors.New("Failed to open RBD image")
	}

	return &Image{
		handle: handle,
		name:   name,
	}, nil
}

func (image *Image) Close() {
	C.rbd_close(image.handle)
}

func (image *Image) Name() string {
	return image.name
}
