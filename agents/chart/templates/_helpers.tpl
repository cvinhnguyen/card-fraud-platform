{{/* The release's agents plus one variant per A/B test: a copy of the tested agent (same prompt, tools, skills)
     with its own model config. Use as: $agents := include "card-support.agents" . | fromYaml */}}
{{- define "card-support.agents" -}}
{{- $all := deepCopy .Values.agents }}
{{- range $base, $t := .Values.abTests }}
{{- $_ := set $all $t.variant (merge (dict "modelConfig" $t.modelConfig "promptOf" $base) (deepCopy (index $.Values.agents $base))) }}
{{- end }}
{{- toYaml $all }}
{{- end }}

{{/* The declarative spec of one agent, shared by its Agent (Deployment) and its SandboxAgent (Substrate).
     Call with (dict "name" $name "a" $a "root" $). */}}
{{- define "card-support.declarative" -}}
runtime: go
modelConfig: {{ .a.modelConfig | default .root.Values.modelConfig }}
stream: true
systemMessageFrom: {type: ConfigMap, name: {{ .root.Release.Name }}-prompts, key: {{ .a.promptOf | default .name }}}
tools:
  {{- if .a.mcpServer }}
  - type: McpServer
    mcpServer: {apiGroup: kagent.dev, kind: RemoteMCPServer, name: {{ .a.mcpServer }}, toolNames: {{ toJson .a.tools }}}
  {{- end }}
  {{- range .a.subAgents }}
  - type: Agent
    agent: {name: {{ . }}}
    headersFrom:   # the gateway's A2A route accepts only this key for agent-to-agent calls
      - name: Authorization
        valueFrom: {type: Secret, name: a2a-key-{{ $.name }}, key: Authorization}
  {{- end }}
a2aConfig:
  skills: {{- toYaml .a.skills | nindent 4 }}
{{- end }}
