{{/* The declarative spec of one agent, shared by its Agent (Deployment) and its SandboxAgent (Substrate).
     Call with (dict "name" $name "a" $a "root" $). */}}
{{- define "card-support.declarative" -}}
runtime: go
modelConfig: {{ .root.Values.modelConfig }}
stream: true
systemMessageFrom: {type: ConfigMap, name: agent-prompts, key: {{ .name }}}
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
