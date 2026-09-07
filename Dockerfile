# syntax=docker/dockerfile:1.7
# Coolify Compose builds inject --build-arg SOURCE_COMMIT automatically
# (predefined deploy SHA). Declare it here so the value reaches this
# stage. Do not put ${SOURCE_COMMIT} in docker-compose.coolify.yml —
# Coolify then locks it as a UI env var that cannot be deleted.
ARG SOURCE_COMMIT=
FROM golang:1.26.6-alpine AS build

WORKDIR /src

# Optional overrides for non-git build contexts (exported tarballs, etc.).
# Prefer git checkout SHA when .git is present. Otherwise use Coolify's
# SOURCE_COMMIT build-arg, then a manual COMMIT arg.
ARG SOURCE_COMMIT=
ARG VERSION=
ARG COMMIT=
ARG BUILD_TIME=
ARG GOPROXY=https://proxy.golang.org,direct
ARG GOSUMDB=sum.golang.org

ENV GOPROXY=${GOPROXY}
ENV GOSUMDB=${GOSUMDB}

# Alpine's CDN flakes under Coolify (DNS / brief 5xx). Retry, then swap
# to the geographic mirror pool before giving up. VERSION_ID is e.g. 3.22.1.
RUN sh -ec '\
	. /etc/os-release; \
	for i in 1 2 3 4 5; do \
		apk add --no-cache git && exit 0; \
		echo "apk add git failed, retry ${i}/5"; \
		if [ "$i" = 3 ]; then \
			echo "https://mirror.alpinelinux.org/alpine/v${VERSION_ID%.*}/main" > /etc/apk/repositories; \
			echo "https://mirror.alpinelinux.org/alpine/v${VERSION_ID%.*}/community" >> /etc/apk/repositories; \
		fi; \
		sleep $((i * 2)); \
	done; \
	exit 1'

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
		elif [ -n "${SOURCE_COMMIT}" ] && [ "${SOURCE_COMMIT}" != "unknown" ] && [ "${SOURCE_COMMIT}" != "HEAD" ]; then \
			RESOLVED_COMMIT="${SOURCE_COMMIT}"; \
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
		CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="$LDFLAGS" -o /out/trust_worker ./cmd/trust_worker; \
		CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="$LDFLAGS" -o /out/migrate ./cmd/migrate \
	'

FROM alpine:3.20
# No apk in the runtime stage: Coolify deploys have failed on Alpine CDN
# fetches, and the Go binaries are static. alpine ships
# ca-certificates-bundle plus busybox wget (used by Compose healthchecks).

WORKDIR /app
COPY --from=build /out/api /app/api
COPY --from=build /out/ingestor /app/ingestor
COPY --from=build /out/worker /app/worker
COPY --from=build /out/trust_worker /app/trust_worker
COPY --from=build /out/migrate /app/migrate

USER nobody
EXPOSE 8080
CMD ["/app/api"]
