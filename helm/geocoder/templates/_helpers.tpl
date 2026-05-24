{{/*
Expand the name of the chart.
*/}}
{{- define "geocoder.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "geocoder.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart name and version label.
*/}}
{{- define "geocoder.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "geocoder.labels" -}}
helm.sh/chart: {{ include "geocoder.chart" . }}
{{ include "geocoder.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "geocoder.selectorLabels" -}}
app.kubernetes.io/name: {{ include "geocoder.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Common environment variables for inserters and app services
*/}}
{{- define "geocoder.commonEnv" -}}
- name: NATS_URL
  value: "nats://{{ include "geocoder.fullname" . }}-nats:{{ .Values.nats.service.clientPort }}"
- name: NATS_STREAM
  value: {{ .Values.nats.stream | quote }}
- name: NATS_SUBJECT
  value: {{ .Values.nats.subject | quote }}
{{- end }}

{{/*
Elasticsearch environment variables
*/}}
{{- define "geocoder.esEnv" -}}
- name: ELASTICSEARCH_URL
  value: "http://{{ include "geocoder.fullname" . }}-elasticsearch:{{ .Values.elasticsearch.service.httpPort }}"
{{- end }}

{{/*
Redis environment variables
*/}}
{{- define "geocoder.redisEnv" -}}
- name: REDIS_HOST
  value: "{{ include "geocoder.fullname" . }}-redis"
- name: REDIS_PORT
  value: {{ .Values.redis.service.port | quote }}
{{- end }}

{{/*
PostGIS environment variables
*/}}
{{- define "geocoder.postgisEnv" -}}
- name: POSTGRES_HOST
  value: "{{ include "geocoder.fullname" . }}-postgis"
- name: POSTGRES_PORT
  value: {{ .Values.postgis.service.port | quote }}
- name: POSTGRES_DB
  value: {{ .Values.postgis.auth.database | quote }}
- name: POSTGRES_USER
  valueFrom:
    secretKeyRef:
      name: {{ include "geocoder.fullname" . }}-postgis
      key: username
- name: POSTGRES_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "geocoder.fullname" . }}-postgis
      key: password
{{- end }}

{{/*
Embedding model environment variables
*/}}
{{- define "geocoder.embeddingEnv" -}}
{{- if .Values.global.aiEnabled }}
- name: EMBEDDING_MODEL
  value: {{ .Values.embeddings.localModelPath | quote }}
- name: ENABLE_VECTORS
  value: "true"
- name: TRANSFORMERS_CACHE
  value: {{ .Values.embeddings.cacheDir | quote }}
- name: HF_HOME
  value: {{ .Values.embeddings.cacheDir | quote }}
{{- else }}
- name: EMBEDDING_MODEL
  value: {{ .Values.embeddings.model | quote }}
- name: ENABLE_VECTORS
  value: "false"
{{- end }}
{{- end }}

{{/*
Model volume mount for AI mode
*/}}
{{- define "geocoder.modelVolumeMount" -}}
{{- if .Values.global.aiEnabled }}
- name: models
  mountPath: {{ .Values.embeddings.cacheDir }}
{{- end }}
{{- end }}

{{/*
Model volume for AI mode
*/}}
{{- define "geocoder.modelVolume" -}}
{{- if .Values.global.aiEnabled }}
- name: models
  {{- if .Values.modelVolume.existingClaim }}
  persistentVolumeClaim:
    claimName: {{ .Values.modelVolume.existingClaim }}
  {{- else }}
  persistentVolumeClaim:
    claimName: {{ include "geocoder.fullname" . }}-models
  {{- end }}
{{- end }}
{{- end }}

{{/*
GPU resources for AI mode
*/}}
{{- define "geocoder.gpuResources" -}}
{{- if and .Values.global.aiEnabled .Values.gpu.enabled }}
nvidia.com/gpu: {{ .Values.gpu.count }}
{{- end }}
{{- end }}

{{/*
Image reference for application services
*/}}
{{- define "geocoder.appImage" -}}
{{- if .Values.global.imageRegistry }}
{{- printf "%s/%s:%s" .Values.global.imageRegistry .Values.app.image.repository .Values.app.image.tag }}
{{- else }}
{{- printf "%s:%s" .Values.app.image.repository .Values.app.image.tag }}
{{- end }}
{{- end }}
