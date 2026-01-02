package entity

type JobPayload struct {
	Rpc    *RpcPayload   `json:"rpc,omitempty"`
	Http   *HttpPayload  `json:"http,omitempty"`
	Golang *GoPayload    `json:"golang,omitempty"`
	Python *PyPayload    `json:"python,omitempty"`
	Shell  *ShellPayload `json:"shell,omitempty"`
}

type RpcPayload struct {
	Endpoint string         `json:"endpoint"`
	Method   string         `json:"method"`
	ReqJson  string         `json:"req_json"`
	CtxKvs   map[string]any `json:"ctx_kvs,omitempty"`
}

type HttpPayload struct {
	Url     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers,omitempty"`
	Queries map[string]string `json:"queries,omitempty"`
	Body    string            `json:"body,omitempty"`
	CtxKvs  map[string]any    `json:"ctx_kvs,omitempty"`
}

type GoPayload struct {
	Script string `json:"script"`
}

type PyPayload struct {
	Script string `json:"script"`
}

type ShellPayload struct {
	Command string `json:"command"`
	Script  string `json:"script"`
}
