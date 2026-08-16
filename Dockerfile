# syntax=docker/dockerfile:1.7
FROM golang:1.26.6-alpine AS build

WORKDIR /src

# Optional overrides for non-git build contexts (exported tarballs, etc.).
# When .git is present in the build context, the checkout SHA always wins for
# COMMIT — Coolify's frozen SOURCE_COMMIT env must not label a newer binary.
ARG VERSION=
ARG COMMIT=
ARG BUILD_TIME=
ARG GOPROXY=https://proxy.golang.org,direct
ARG GOSUMDB=sum.golang.org

ENV GOPROXY=${GOPROXY}
ENV GOSUMDB=${GOSUMDB}

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
	--mount=type=cache,target=/root/.cache/go-build \
	sh -ec 'for i in 1 2 3; do go mod download && exit 0; echo "go mod download failed, retry ${i}/3"; sleep $((i*2)); done; exit 1'

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
	--mount=type=cache,target=/root/.cache/go-build \
	sh -ec '\
		if [ -d .git ]; then \
			RESOLVED_COMMIT="$(git rev-parse HEAD)"; \
		elif [ -n "${COMMIT}" ] && [ "${COMMIT}" != "unknown" ]; then \
			RESOLVED_COMMIT="${COMMIT}"; \
		else \
			RESOLVED_COMMIT="unknown"; \
		fi; \
		# Prefer a human release tag (v1.2.3). Ignore leftover Coolify SHA envs. \
		if [ -n "${VERSION}" ] && [ "${VERSION}" != "coolify" ] && [ "${VERSION}" != "dev" ] && [ "${VERSION}" != "unknown" ] \
			&& ! printf '%s' "${VERSION}" | grep -Eq '^[0-9a-fA-F]{7,40}$'; then \
			RESOLVED_VERSION="${VERSION}"; \
		else \
			RESOLVED_VERSION="${RESOLVED_COMMIT}"; \
		fi; \
		if [ -n "${BUILD_TIME}" ] && [ "${BUILD_TIME}" != "unknown" ]; then \
			RESOLVED_TIME="${BUILD_TIME}"; \
		else \
			RESOLVED_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)"; \
		fi; \
		echo "build identity version=${RESOLVED_VERSION} commit=${RESOLVED_COMMIT} time=${RESOLVED_TIME}"; \
		LDFLAGS="-s -w -X main.buildVersion=${RESOLVED_VERSION} -X main.buildCommit=${RESOLVED_COMMIT} -X main.buildTime=${RESOLVED_TIME}"; \
		CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="$LDFLAGS" -o /out/api ./cmd/api; \
		CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="$LDFLAGS" -o /out/ingestor ./cmd/ingestor; \
		CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="$LDFLAGS" -o /out/worker ./cmd/worker; \
		CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="$LDFLAGS" -o /out/trust_worker ./cmd/trust_worker \
	'

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=build /out/api /app/api
COPY --from=build /out/ingestor /app/ingestor
COPY --from=build /out/worker /app/worker
COPY --from=build /out/trust_worker /app/trust_worker

USER nobody
EXPOSE 8080
CMD ["/app/api"]
