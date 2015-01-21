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
	"fmt"

	rados "github.com/clbh/go-rados"
)

// Our bindings version
const VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH = 1, 0, 0

// Exported types
type Image struct {
	handle C.rbd_image_t
	name   string
}

type ImageInfo struct {
  Image    *Image
	Size     uint64
	Obj_size uint64
	Num_objs uint64
	Order    int
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

func DeleteImage(pool *rados.Pool, imageName string) error {
	// TODO: Release memory allocated by C.CString()
	if result := C.rbd_remove(C.rados_ioctx_t(pool.Handle()), C.CString(imageName)); result < 0 {
		return errors.New("Failed to remove image")
	}

	return nil
}

func RenameImage(pool *rados.Pool, srcName string, dstName string) error {
	// TODO: Release memory allocated by C.CString()
	if result := C.rbd_rename(C.rados_ioctx_t(pool.Handle()), C.CString(srcName), C.CString(dstName)); result < 0 {
		return errors.New("Failed to rename image")
	}

	return nil
}

func ListImages(pool *rados.Pool) ([]string, error) {
	var buf [65536]C.char
	var size C.size_t = 65536

	result := C.rbd_list(C.rados_ioctx_t(pool.Handle()), &buf[0], &size)
	if result < 0 {
		return []string{}, errors.New("Failed to fetch image list from pool")
	}

	// 'buf' now contains up to 4096 bytes worth of nul-separated image name
	// strings, which we need to split up to return a list of images

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

	// TODO: Release memory allocated by C.CString()
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

// Copy an image to a destination pool with the specified destination image name
func (image *Image) CopyToName(destPool *rados.Pool, destImage string) error {
	// rbd_copy() is a syncronous function. It will not return until the copy
	// operation has completed
	// TODO: Release memory allocated by C.CString()
	if result := C.rbd_copy(image.handle, C.rados_ioctx_t(destPool.Handle()), C.CString(destImage)); result < 0 {
		return errors.New("Failed to copy image")
	}

	return nil
}

// Copy an image to a destination image with an already-open handle
func (image *Image) CopyToImage(dest *Image) error {
	// rbd_copy() is a syncronous function. It will not return until the copy
	// operation has completed
	if result := C.rbd_copy2(image.handle, dest.Handle()); result < 0 {
		return errors.New("Failed to copy image")
	}

	return nil
}

func (image *Image) CreateSnapshot(name string) error {
	// TODO: Release unmanaged memory allocated by C.CString()
	if result := C.rbd_snap_create(image.handle, C.CString(name)); result < 0 {
		return fmt.Errorf("Unable to create snapshot '%s' on image '%s'", name, image.name)
	}

	return nil
}

func (image *Image) Format() int {
	var isOld C.uint8_t

	// rbd_get_old_format() will return true if image version 1
	// and will return false if image version 2 (sigh..)
	if result := C.rbd_get_old_format(image.handle, &isOld); result < 0 {
		return 0
	}

	switch isOld {
	case 1:
		return 1
	case 0:
		return 2
	default:
		return 0
	}
}

func (image *Image) Handle() C.rbd_image_t {
	return image.handle
}

func (image *Image) Info() (*ImageInfo, error) {
	var info C.rbd_image_info_t

	if result := C.rbd_stat(image.handle, &info, 0); result < 0 {
		return nil, errors.New("Failed to retrieve image info")
	}

	return &ImageInfo{
		Image:    image,
		Size:     uint64(info.size),
		Obj_size: uint64(info.obj_size),
		Num_objs: uint64(info.num_objs),
		Order:    int(info.order),
	}, nil
}


func (image *Image) Name() string {
	return image.name
}

func (image *Image) RemoveSnapshot(name string) error {
	// TODO: Release unmanaged memory allocated by C.CString()
	if result := C.rbd_snap_remove(image.handle, C.CString(name)); result < 0 {
		return fmt.Errorf("Unable to remove snapshot '%s' from image '%s'", name, image.name)
	}

	return nil
}

func (image *Image) Resize(size uint64) error {
	if result := C.rbd_resize(image.handle, C.uint64_t(size)); result < 0 {
		return fmt.Errorf("Unable to resize image '%s' to size %d", image.name, size)
	}

	return nil
}

func (image *Image) Size() uint64 {
	var size C.uint64_t

	if result := C.rbd_get_size(image.handle, &size); result < 0 {
		return 0
	}

	return uint64(size)
}
