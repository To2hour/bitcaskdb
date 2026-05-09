package util

import "github.com/bwmarrin/snowflake"

var node *snowflake.Node

func init() {
	node, _ = snowflake.NewNode(1)
}

func GenerateBatchId() snowflake.ID {
	return node.Generate()
}
func DecodeBatchId(key []byte) snowflake.ID {
	bytes, _ := snowflake.ParseBytes(key)
	return bytes
}
