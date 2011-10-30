# Copyright (c) 2011 Alexander Sychev. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

include $(GOROOT)/src/Make.inc

TARG=btree
GOFILES=\
	btree.go 

include $(GOROOT)/src/Make.pkg
