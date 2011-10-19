// Copyright (c) 2011 Alexander Sychev. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package btree implements B-trees with fixed size keys, http://en.wikipedia.org/wiki/Btree.
package btree

import (
	"io"
	"os"
	"reflect"
	"encoding/binary"
	"bytes"
)

var (
	bo = binary.LittleEndian
)

// Errors in addition to IO errors.
var (
	NoReader        = os.NewError("reader is not specified")
	NoWriter        = os.NewError("writer is not specified")
	OddCapacity     = os.NewError("capacity must be even")
	MagicMismatch   = os.NewError("magic mismatch")
	KeyTypeMismatch = os.NewError("key type mismatch")
)

// An interface a key must support to be stored in the tree.
// A key is either a fixed-size arithmetic type (int8, uint8, int16, float32, complex64, ...) or an array or struct containing only fixed-size values.
// All fields of the key must be exportable.
// Compare has to return a value less that 0, equal of 0 or more that 0 if k is less, equal or more that an underlying key. 
type Key interface {
	Compare(k Key) int
}

// A basic interface for tree operations.
type Tree interface {
	Find(key Key) (Key, os.Error)
	Insert(key Key) (Key, os.Error)
	Update(key Key) (Key, os.Error)
	Delete(key Key) (Key, os.Error)
	Enum(key Key) func() (Key, os.Error)
}

// header of file with the tree
type fileHeader struct {
	Magic      [16]byte     // file magic
	KeyType    reflect.Kind // type of key
	KeySize    uint32       // size of key in bytes
	Capacity   uint32       // size of btree node in datas	
	Root       int64        // offset of the root node
	EmptyNodes int64        // offset of empty nodes offsets
}

// node struct.
type node struct {
	size    int          // size of one data element
	keytype reflect.Type // stored type of the key
	offset  int64        // offset of node
	count   uint32
	raw     []byte // raw node in bytes
	datas   []byte // datas
}

// an internal representation of B-tree.
type BTree struct {
	header  fileHeader     // header of index file
	reader  io.ReadSeeker  // stored reader
	writer  io.WriteSeeker // stored writer 
	keytype reflect.Type   // stored type of the key
	empty   []int64        // empty nodes offsets
}

// NewBTree creates a new B-tree with magic like a file magic, key like a key type and capacity like a number of elements per data.
// The capacity must be even.
// Both reader and writer have to be specified.
// It returns a pointer to the new tree and an error, if any
func NewBTree(writer io.WriteSeeker, reader io.ReadSeeker, magic [16]byte, key Key, capacity uint) (Tree, os.Error) {
	if writer == nil {
		return nil, NoWriter
	}
	if capacity%2 == 1 {
		return nil, OddCapacity
	}
	this := new(BTree)
	this.writer = writer
	this.reader = reader
	this.header.Magic = magic
	k := reflect.ValueOf(key)
	this.header.KeyType = k.Kind()
	this.header.KeySize = uint32(k.Type().Size())
	this.header.Capacity = uint32(capacity)
	this.header.Root = -1
	this.header.EmptyNodes = -1
	if _, err := writer.Seek(0, os.SEEK_SET); err != nil {
		return nil, err
	}
	if err := this.header.write(writer, nil); err != nil {
		return nil, err
	}
	this.keytype = k.Type()
	return this, nil
}

// OpenBTree opens an existing B-tree. 
// The file magic and magic must be the same, the type and the size of key and of the key in the tree must be the same too.
// Only reader is mandatory, writer may be nil if changing of the tree is not planned .
// It returns a pointer to the new tree and an error, if any.
func OpenBTree(reader io.ReadSeeker, writer io.WriteSeeker, magic [16]byte, key Key) (Tree, os.Error) {
	if reader == nil {
		return nil, NoReader
	}
	this := new(BTree)
	this.reader = reader
	this.writer = writer
	if en, err := this.header.read(reader); err != nil {
		return nil, err
	} else {
		this.empty = en
	}
	if !bytes.Equal(this.header.Magic[:], magic[:]) {
		return nil, MagicMismatch
	}
	if !this.checkKey(key) {
		return nil, KeyTypeMismatch
	}
	this.keytype = reflect.TypeOf(key)
	return this, nil
}

// Find searches a key in the tree.
// It returns the key if it is found or nil if the key is not found and an error, if any.
func (this *BTree) Find(key Key) (Key, os.Error) {
	if this.reader == nil {
		return nil, NoReader
	}
	if !this.checkKey(key) {
		return nil, KeyTypeMismatch
	}
	_, _, k, err := this.find(key)
	if err != nil {
		return nil, err
	}
	if k == nil {
		return nil, nil
	}
	return k, nil
}

// Find searches a key in the tree.
// It returns the key if it is already exists or nil if the key is inserted and an error, if any.
func (this *BTree) Insert(key Key) (Key, os.Error) {
	if this.reader == nil {
		return nil, NoReader
	}
	if !this.checkKey(key) {
		return nil, KeyTypeMismatch
	}
	k, err := this.insert(key)
	if err != nil {
		return nil, err
	}
	if k != nil {
		return k, nil
	}
	return nil, nil
}

// Update updates a key in the tree. 
// This is useful if the key is complex type with additional information.
// It returns an old value of the key if the key is updated or nil if the key is not found and an error, if any.
func (this *BTree) Update(key Key) (Key, os.Error) {
	if this.reader == nil {
		return nil, NoReader
	}
	if this.writer == nil {
		return nil, NoWriter
	}
	if !this.checkKey(key) {
		return nil, KeyTypeMismatch
	}
	n, i, k, err := this.find(key)
	if err != nil {
		return nil, err
	}
	if n == nil {
		return nil, nil
	}

	old := k
	n.setKey(i, key)
	return old, this.writeNode(n)
}

// BUG(santucco): The storage with B-tree can't be reduced even after erasing all of its keys. All empty nodes are stored and reused when necessary

// Delete deletes a key from the tree. 
// It returns the key if it is deleted or nil if the key is not found and an error, if any.
func (this *BTree) Delete(key Key) (Key, os.Error) {
	if this.reader == nil {
		return nil, NoReader
	}
	if this.writer == nil {
		return nil, NoWriter
	}
	if !this.checkKey(key) {
		return nil, KeyTypeMismatch
	}
	return this.delete(key)
}

// Enum returns a function-iterator to process enumeration entire the tree.
// Enumerating starts with key, if it is specified, or with lowest key otherwise.
// The iterator returns the key or nil if the end of the tree is reached and an error, if any.
func (this *BTree) Enum(key Key) func() (Key, os.Error) {
	nodes := make([]*node, 0, 10)
	offset := this.header.Root
	return func() (Key, os.Error) {
		for true {
			if offset == -1 {
				if len(nodes) == 0 {
					return nil, nil
				}
				l := len(nodes) - 1
				p := nodes[l]
				if p.count > 0 {
					offset = p.getOffset(0)
					key := p.getKey(0)
					p.datas = p.datas[p.size:]
					p.count--
					return key, nil
				}
				nodes = nodes[:l]
			} else {
				var p node
				p.init(this)
				if err := p.read(this.reader, offset); err != nil {
					return nil, err
				}
				if key != nil {
					if idx, _, less := p.find(key); idx != -1 {
						offset = p.getOffset(idx)
						p.datas = p.datas[p.size*(idx+1):]
						p.count -= uint32(idx + 1)
						key = nil
					} else if less == -1 {
						offset = p.getLeast()
					} else {
						offset = p.getOffset(less)
						p.datas = p.datas[p.size*(less+1):]
						p.count -= uint32(less + 1)
					}
				} else {
					offset = p.getLeast()
				}
				nodes = append(nodes, &p)
			}
		}
		return nil, nil
	}
}

// KeySize returns a size in bytes of the underlying key.
func (this BTree) KeySize() uint32 {
	return this.header.KeySize
}

// Capacity returns a number of keys per node.
func (this BTree) Capacity() uint32 {
	return this.header.Capacity
}

// KeyType returns a kind of the underlying key.
func (this BTree) KeyType() reflect.Kind {
	return this.header.KeyType
}

// Magic returns a magic of the B-tree storage.
func (this BTree) Magic() [16]byte {
	return this.header.Magic
}

// writeNode writes node to a buffer and writes the buffer to the storage.
// It returns nil on success or an error.
func (this *BTree) writeNode(p *node) os.Error {
	if p.offset != -1 {
		if _, err := this.writer.Seek(p.offset, os.SEEK_SET); err != nil {
			return err
		}
	} else {
		if len(this.empty) != 0 {
			p.offset = this.empty[len(this.empty)-1]
			this.empty = this.empty[:len(this.empty)-1]
			if len(this.empty) == 0 {
				if err := this.header.write(this.writer, nil); err != nil {
					return err
				}
			}
			if _, err := this.writer.Seek(p.offset, os.SEEK_SET); err != nil {
				return err
			}
		} else if off, err := this.writer.Seek(0, os.SEEK_END); err != nil {
			return err
		} else {
			p.offset = off
		}
	}

	return p.write(this.writer)
}

// checkKey verifies a key has the same size and the same time as stored in the tree.
// It returns true if the key is compatible with the keys of the tree, false otherwise.
func (this BTree) checkKey(key Key) bool {
	v := reflect.TypeOf(key)
	return v.Kind() == this.header.KeyType &&
		uint32(v.Size()) == this.header.KeySize
}

// find finds a key.
// It returns pointer to a node with the key and an index of the key in the node and an error, if any.
func (this *BTree) find(key Key) (*node, int, Key, os.Error) {
	curoff := this.header.Root
	for curoff > 0 {
		var p node
		p.init(this)
		if err := p.read(this.reader, curoff); err != nil {
			return nil, -1, nil, err
		}
		if idx, k, less := p.find(key); idx != -1 {
			return &p, idx, k, nil
		} else if less == -1 {
			curoff = p.getLeast()
		} else {
			curoff = p.getOffset(less)
		}
	}
	return nil, -1, nil, nil
}

// insert inserts a key in the tree.
// It returns the key if it is already exists or nil if the key is inserted and an error, if any.
func (this *BTree) insert(key Key) (Key, os.Error) {
	offset := this.header.Root
	offsets := make([]int64, 0, 10)
	c := 0
	var p node
	p.init(this)
	var less int = -1
	for true {
		var k Key
		if offset != -1 {
			if err := p.read(this.reader, offset); err != nil {
				return nil, err
			}
			_, k, less = p.find(key)
		}
		if p.count == this.header.Capacity {
			c++
		} else {
			c = 0
		}
		if k != nil {
			return k, nil
		}
		least := p.getLeast()
		if less == -1 && least != -1 {
			offset = least
			offsets = append(offsets, p.offset)
			continue
		} else if less != -1 {
			if off := p.getOffset(less); off != -1 {
				offset = off
				offsets = append(offsets, p.offset)
				continue
			}
		}
		break
	}
	// if current and previous nodes are full, trying to reserve a space in the storage
	// and put nodes in the list of empty nodes
	if c > len(this.empty) {
		c -= len(this.empty)
		var e node
		e.init(this)
		for i := 0; i < c; i++ {
			e.offset = -1
			if err := this.writeNode(&e); err != nil {
				return nil, err
			}
			this.empty = append(this.empty, e.offset)
		}
	}
	p.insert(less, key, -1)
	full := p.count > this.header.Capacity
	if !full {
		if err := this.writeNode(&p); err != nil {
			return nil, err
		}
	}
	if this.header.Root == -1 {
		this.header.Root = p.offset
		return nil, this.header.write(this.writer, this.empty)
	}
	if !full {
		return nil, nil
	}
	return nil, this.split(&p, offsets)
}

// split splits a node pointed by p on two nodes, inserts a middle data in a parent node and saves all changed nodes.
// It returns nil on success or an error.
func (this *BTree) split(p *node, offsets []int64) os.Error {
	var root int64 = -1
	for true {
		var np node
		np.init(this)
		np.count = p.count / 2
		np.datas = np.datas[:int(np.count)*p.size]
		copy(np.datas, p.datas[int(np.count+1)*p.size:])
		off := p.getOffset(int(np.count))
		np.setLeast(off)
		if err := this.writeNode(&np); err != nil {
			return err
		}
		offset := np.offset
		key := p.getKey(int(np.count))
		p.count /= 2
		if err := this.writeNode(p); err != nil {
			return err
		}
		if len(offsets) == 0 {
			var rp node
			rp.init(this)
			rp.insert(-1, key, offset)
			rp.setLeast(p.offset)
			if err := this.writeNode(&rp); err != nil {
				return err
			}
			root = rp.offset
		} else {
			off := offsets[len(offsets)-1]
			offsets = offsets[:len(offsets)-1]
			p.read(this.reader, off)
			_, _, less := p.find(key)
			p.insert(less, key, offset)
		}
		if p.count <= this.header.Capacity {
			if err := this.writeNode(p); err != nil {
				return err
			}
			break
		}
	}
	if root == -1 {
		return nil
	}
	this.header.Root = root
	return this.header.write(this.writer, this.empty)
}

// delete deletes a key from the tree.
// It returns nil on success or an error.
func (this *BTree) delete(key Key) (Key, os.Error) {
	offset := this.header.Root
	index := -1
	var pnode *node
	var poff []byte

	var p *node
	// loking for the key
	for true {
		tmp := new(node)
		tmp.init(this)
		var k Key
		less := -1
		if offset != -1 {
			if err := tmp.read(this.reader, offset); err != nil {
				return nil, err
			}
			index, k, less = tmp.find(key)
		}
		if k != nil {
			key = k
			p = tmp
			break
		}
		pnode = tmp
		least := tmp.getLeast()
		if less == -1 && least != -1 {
			offset = least
			poff = tmp.raw[0:4]
		} else if less != -1 {
			if off := tmp.getOffset(less); off != -1 {
				offset = off
				poff = tmp.datas[less*pnode.size : less*pnode.size+4]
				continue
			}
		} else {
			return nil, nil
		}
	}
	// removing data
	for true {
		offset = p.getOffset(index)
		if index < int(p.count) {
			copy(p.datas[index*p.size:], p.datas[(index+1)*p.size:])
		}
		p.count--
		index--
		if offset == -1 {
			if p.count >= this.header.Capacity/2 {
				return key, this.writeNode(p)
			}
			index = -1
			offset = p.getLeast()
			if offset != -1 {
				pnode = p
				poff = p.raw[0:4]
			}
			for i := 0; offset == -1 && i < int(p.count); i++ {
				off := p.getOffset(i)
				if off != -1 {
					offset = off
					poff = p.datas[i*p.size : i*p.size+4]
					index = i
				}
			}
		}
		if offset == -1 {
			// it is a leaf node
			if err := this.writeNode(p); err != nil {
				return key, err
			}
			if p.count > 0 {
				return key, nil
			}
			// it is an empty leaf node
			if poff != nil {
				copy(poff, []byte{0xff, 0xff, 0xff, 0xff})
				if err := this.writeNode(pnode); err != nil {
					return key, err
				}
			}
			this.empty = append(this.empty, p.offset)
			if p.offset == this.header.Root {
				this.header.Root = -1
				return key, this.header.write(this.writer, this.empty)
			}
			return key, nil
		}
		if poff != nil {
			copy(poff, []byte{0xff, 0xff, 0xff, 0xff})
		}
		var np *node
		for off := offset; ; {
			tmp := new(node)
			tmp.init(this)
			if err := tmp.read(this.reader, off); err != nil {
				return key, err
			}
			least := tmp.getLeast()
			if least == -1 {
				np = tmp
				break
			}
			off = least
			pnode = tmp
			poff = tmp.raw[0:4]
		}
		po := p.insert(index, np.getKey(0), offset)
		if offset == np.offset {
			poff = po
			pnode = p
		}
		index = 0
		if err := this.writeNode(p); err != nil {
			return key, err
		}
		p = np

	}
	return key, nil
}

// bufferSize returns a size of a buffer in bytes will be read from the reader at time.
func (this fileHeader) bufferSize() uint32 {
	return /*size of least*/ 4 +
		/*size of count*/ 4 +
		this.Capacity*(this.KeySize+ /*size of offset*/ 4)
}

// read reads the file header from reader.
// It retuns a slice of offsets of empty nodes and nil or nil and an error.
func (this *fileHeader) read(reader io.ReadSeeker) ([]int64, os.Error) {
	if _, err := reader.Seek(0, os.SEEK_SET); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, bo, this); err != nil {
		return nil, err
	}
	if this.EmptyNodes == -1 {
		return nil, nil
	}
	if _, err := reader.Seek(this.EmptyNodes, os.SEEK_SET); err != nil {
		return nil, err
	}
	var count int32
	if err := binary.Read(reader, bo, &count); err != nil {
		return nil, err
	}
	empty := make([]int64, count)
	for i := 0; i < int(count); i++ {
		var off int32
		if err := binary.Read(reader, bo, &off); err != nil {
			return nil, err
		}
		empty[i] = int64(off)
	}
	return empty, nil
}

// read writes the file header to writer
// It retuns nil on success or an error
func (this *fileHeader) write(writer io.WriteSeeker, empty []int64) os.Error {
	if len(empty) != 0 {
		if this.EmptyNodes == -1 {
			if off, err := writer.Seek(0, os.SEEK_END); err != nil {
				return err
			} else {
				this.EmptyNodes = off
			}
		} else {
			if _, err := writer.Seek(this.EmptyNodes, os.SEEK_SET); err != nil {
				return err
			}
		}
		var count int32 = int32(len(empty))
		if err := binary.Write(writer, bo, count); err != nil {
			return err
		}
		for i := 0; i < int(count); i++ {
			if err := binary.Write(writer, bo, int32(empty[i])); err != nil {
				return err
			}
		}
	} else {
		this.EmptyNodes = -1
	}

	if _, err := writer.Seek(0, os.SEEK_SET); err != nil {
		return err
	}
	return binary.Write(writer, bo, this)
}

// init inits the node with default values.
func (this *node) init(tree *BTree) {
	this.size = int(tree.header.KeySize) + /*size of offset*/ 4
	this.keytype = tree.keytype
	this.offset = -1
	this.count = 0
	this.raw = make([]byte, tree.header.bufferSize(), tree.header.bufferSize()+uint32(this.size))
	this.datas = this.raw[8:]
	copy(this.raw[0:4], []byte{0xff, 0xff, 0xff, 0xff})
}

// find fnds a key in the node.
// It returns an index of the key in the node and -1 or -1 and an index of a nearest key is less of the key.
func (this node) find(key Key) (int, Key, int) {
	min := 1
	max := int(this.count)
	for max >= min {
		i := (max + min) / 2
		k := this.getKey(i - 1)
		r := key.Compare(k)
		switch {
		case r < 0:
			max = i - 1
		case r == 0:
			return i - 1, k, -1
		case r > 0:
			min = i + 1
		}
	}
	return -1, nil, max - 1
}

// getOffset returns an offset is got from the datas by index i
func (this *node) getOffset(i int) int64 {
	var offset int32 = int32(bo.Uint32(this.datas[i*this.size : i*this.size+4]))
	return int64(offset)
}

// setKey writes an offset to the datas by index i
func (this *node) setOffset(i int, off int64) {
	var offset int32 = int32(off)
	bo.PutUint32(this.datas[i*this.size:i*this.size+4], uint32(offset))
}

// getKey returns a key data is got from the datas by index i
func (this *node) getKey(i int) Key {
	n := reflect.New(this.keytype)
	b := bytes.NewBuffer(this.datas[i*this.size+4 : (i+1)*this.size])
	if err := binary.Read(b, bo, n.Interface()); err != nil {
		return nil
	}
	return reflect.Indirect(n).Interface().(Key)
}

// setKey writes a key to the datas by index i
func (this *node) setKey(i int, k Key) {
	b := bytes.NewBuffer(nil)
	if err := binary.Write(b, bo, k); err != nil {
		return
	}
	copy(this.datas[i*this.size+4:(i+1)*this.size], b.Bytes())
}

// getLeast returns an offset of the least node
func (this *node) getLeast() int64 {
	var least int32 = int32(bo.Uint32(this.raw[0:4]))
	return int64(least)
}

// setLeast sets an offset of the least node
func (this *node) setLeast(l int64) {
	var least int32 = int32(l)
	bo.PutUint32(this.raw[0:4], uint32(least))
}

// insert inserts a key in the node after index position with offset.
// It returns a pointer to the filled offset.
func (this *node) insert(index int, key Key, offset int64) []byte {
	this.count++
	this.datas = this.raw[8 : 8+int(this.count)*this.size]
	if index+2 < int(this.count) {
		copy(this.datas[(index+2)*this.size:], this.datas[(index+1)*this.size:])
	}
	this.setOffset(index+1, offset)
	this.setKey(index+1, key)
	return this.datas[(index+1)*this.size : (index+1)*this.size+4]
}

// read reads the node from reader with keys of type t and sets a node offset to offset.
// It retuns nil on success or an error.
func (this *node) read(reader io.ReadSeeker, offset int64) os.Error {
	if _, err := reader.Seek(offset, os.SEEK_SET); err != nil {
		return err
	}
	if _, err := io.ReadFull(reader, this.raw); err != nil {
		return err
	}
	this.count = bo.Uint32(this.raw[4:])
	this.offset = offset
	this.datas = this.raw[8 : 8+int(this.count)*this.size]
	return nil
}

// write writes the node to writer with keys of type t.
// It retuns nil on success or an error.
func (this *node) write(writer io.Writer) os.Error {
	bo.PutUint32(this.raw[4:8], this.count)
	_, err := writer.Write(this.raw)
	return err
}
