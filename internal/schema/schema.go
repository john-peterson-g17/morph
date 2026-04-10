package schema

import _ "embed"

//go:embed job.schema.json
var JobSchema []byte

//go:embed template.v1.yml
var JobTemplateV1 []byte
