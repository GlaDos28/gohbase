// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"
	"github.com/glados28/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

// AddColumn represents a AddColumn HBase call
type AddColumn struct {
	base
	tableName *pb.TableName
	cfName []byte
}

// NewAddColumn creates a new AddColumn request that will
// add new column family to given table in HBase.
// For use by the admin client.
func NewAddColumn(ctx context.Context, tableName *pb.TableName, cfName []byte, options ...func(*AddColumn)) *AddColumn {
	ct := &AddColumn{
		base: base{
			table:    tableName.Qualifier,
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
		tableName: tableName,
		cfName: cfName,
	}

	for _, option := range options {
		option(ct)
	}

	return ct
}

// Name returns the name of this RPC call.
func (ct *AddColumn) Name() string {
	return "AddColumn"
}

// ToProto converts the RPC into a protobuf message
func (ct *AddColumn) ToProto() proto.Message {
	return &pb.AddColumnRequest{
		TableName:      ct.tableName,
		ColumnFamilies: &pb.ColumnFamilySchema{ Name: ct.cfName },
	}
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (ct *AddColumn) NewResponse() proto.Message {
	return &pb.AddColumnResponse{}
}
