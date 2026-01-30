package testx

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
)

// RunningUnderTest 返回当前进程是否由 `go test` 启动。
// 原理：
// - 测试二进制名通常以 `.test` 结尾；
// - testing 包在测试时会注册 `-test.*` 相关 flag。
// 这两种检测结合，能在绝大多数环境下可靠工作。
func RunningUnderTest() bool {
	// 通过 testing 注册的标志判断
	if flag.Lookup("test.v") != nil || flag.Lookup("test.run") != nil || flag.Lookup("test.short") != nil {
		return true
	}
	// 通过二进制名称判断
	name := filepath.Base(os.Args[0])
	return strings.HasSuffix(name, ".test") || strings.Contains(name, ".test")
}
